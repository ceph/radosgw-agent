from collections import namedtuple
import datetime
import logging
import multiprocessing
import os
import socket
import threading

from radosgw_agent import client

log = logging.getLogger(__name__)

RESULT_SUCCESS = 0
RESULT_ERROR = 1

class Worker(multiprocessing.Process):
    """sync worker to run in its own process"""

    # sleep the prescribed amount of time and then set a bool to true.
    def flip_log_lock(self):
        self.relock_log = True

    def __init__(self, work_queue, result_queue, log_lock_time,
                 src, dest, **kwargs):
        super(Worker, self).__init__()
        self.source_zone = src.zone
        self.dest_zone = dest.zone
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.log_lock_time = log_lock_time

        self.local_lock_id = (socket.gethostname() + str(os.getpid()) +
                              str(threading.current_thread()))

        # construct the two connection objects
        self.source_conn = client.connection(src)
        self.dest_conn = client.connection(dest)

    # we explicitly specify the connection to use for the locking here
    # in case we need to lock a non-master log file
    def acquire_log_lock(self, lock_id, zone_id, shard_num):
        log.debug('acquiring lock on shard %d', shard_num)
        try:
            client.lock_shard(self.source_conn, self._type, shard_num, zone_id,
                              self.log_lock_time, self.local_lock_id)
        except client.HttpError as e:
            log.error('locking shard %d in zone %s failed: %s',
                     shard_num, zone_id, e)
            # clear this flag for the next pass
            self.relock_log = False
            raise

        # twiddle the boolean flag to false
        self.relock_log = False

        # then start the timer to twiddle it back to true
        self.relock_timer = threading.Timer(0.5 * self.log_lock_time, \
                                            self.flip_log_lock)
        self.relock_timer.start()


    def release_log_lock(self, lock_id, zone_id, shard_num):
        log.debug('releasing lock on shard %d', shard_num)
        client.unlock_shard(self.source_conn, self._type, shard_num,
                            zone_id, self.local_lock_id)


MetadataEntry = namedtuple('MetadataEntry', ['section', 'name', 'marker', 'timestamp'])

def _meta_entry_from_json(entry):
    return MetadataEntry(
        entry['section'],
        entry['name'],
        entry['id'],
        entry['timestamp'],
        )

class MetadataWorker(Worker):

    def __init__(self, *args, **kwargs):
        super(MetadataWorker, self).__init__(*args, **kwargs)
        self._type = 'metadata'

    def sync_meta(self, section, name):
        log.debug('syncing metadata type %s key "%s"', section, name)
        try:
            metadata = client.get_metadata(self.source_conn, section, name)
        except client.NotFound:
            try:
                client.delete_metadata(self.dest_conn, section, name)
            except client.NotFound:
                pass
        except client.HttpError as e:
            log.error('error getting metadata for %s "%s": %s',
                      section, name, e)
            raise
        else:
            client.update_metadata(self.dest_conn, section, name, metadata)

class MetadataWorkerPartial(MetadataWorker):

    def __init__(self, *args, **kwargs):
        self.daemon_id = kwargs['daemon_id']
        self.max_entries = kwargs['max_entries']
        super(MetadataWorkerPartial, self).__init__(*args, **kwargs)

    def get_and_process_entries(self, marker, shard_num):
        num_entries = self.max_entries
        while num_entries >= self.max_entries:
            num_entries, marker = self._get_and_process_entries(marker,
                                                                shard_num)

    def _get_and_process_entries(self, marker, shard_num):
        """
        sync up to self.max_entries entries, returning number of entries
        processed and the last marker of the entries processed.
        """
        log_entries = client.get_meta_log(self.source_conn, shard_num,
                                          marker, self.max_entries)

        log.info('shard %d has %d entries after %r', shard_num, len(log_entries),
                 marker)
        try:
            entries = [_meta_entry_from_json(entry) for entry in log_entries]
        except KeyError:
            log.error('log conting bad key is: %s', log_entries)
            raise

        mentioned = set([(entry.section, entry.name) for entry in entries])
        for section, name in mentioned:
            self.sync_meta(entry.section, entry.name)

        if entries:
            try:
                client.set_worker_bound(self.source_conn, 'metadata',
                                        shard_num, entries[-1].marker,
                                        entries[-1].timestamp,
                                        self.daemon_id)
                return len(entries), entries[-1].marker
            except:
                log.exception('error setting worker bound, may duplicate some work later')

        return 0, ''

    def run(self):
        while True:
            shard_num = self.work_queue.get()
            if shard_num is None:
                log.info('process %s is done. Exiting', self.ident)
                break

            log.info('%s is processing shard number %d',
                     self.ident, shard_num)

            # first, lock the log
            try:
                self.acquire_log_lock(self.local_lock_id, self.source_zone,
                                      shard_num)
            except client.NotFound:
                # no log means nothing changed in this time period
                self.result_queue.put((RESULT_SUCCESS, shard_num))
                continue
            except client.HttpError as e:
                log.info('error locking shard %d log, assuming'
                         ' it was processed by someone else and skipping: %s',
                         shard_num, e)
                self.result_queue.put((RESULT_ERROR, shard_num))
                continue

            try:
                marker, time = client.get_min_worker_bound(self.source_conn,
                                                           'metadata',
                                                           shard_num)
                log.debug('oldest marker and time for shard %d are: %r %r',
                          shard_num, marker, time)
            except client.NotFound:
                marker, time = '', '1970-01-01 00:00:00'
            except Exception as e:
                log.exception('error getting worker bound for shard %d',
                              shard_num)
                self.result.queue.put((RESULT_ERROR, shard_num))
                continue

            result = RESULT_SUCCESS
            try:
                self.get_and_process_entries(marker, shard_num)
            except:
                log.exception('syncing entries from %s for shard %d failed',
                              marker, shard_num)
                result = RESULT_ERROR

            # finally, unlock the log
            try:
                self.release_log_lock(self.local_lock_id,
                                      self.source_zone, shard_num)
            except:
                log.exception('error unlocking log, continuing anyway '
                              'since lock will timeout')

            self.result_queue.put((result, shard_num))
            log.info('finished processing shard %d', shard_num)

class MetadataWorkerFull(MetadataWorker):

    def run(self):
        while True:
            meta = self.work_queue.get()
            if meta is None:
                log.info('No more entries in queue, exiting')
                break

            try:
                section, name = meta
                self.sync_meta(section, name)
                result = RESULT_SUCCESS
            except Exception as e:
                log.exception('could not sync entry %s "%s": %s',
                              section, name, e)
                result = RESULT_ERROR
            self.result_queue.put((result, section, name))
