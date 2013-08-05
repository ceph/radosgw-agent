from collections import namedtuple
import logging
import multiprocessing
import os
import socket

from radosgw_agent import client
from radosgw_agent import lock

log = logging.getLogger(__name__)

RESULT_SUCCESS = 0
RESULT_ERROR = 1

class Worker(multiprocessing.Process):
    """sync worker to run in its own process"""

    def __init__(self, work_queue, result_queue, log_lock_time,
                 src, dest, **kwargs):
        super(Worker, self).__init__()
        self.source_zone = src.zone
        self.dest_zone = dest.zone
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.log_lock_time = log_lock_time
        self.lock = None

        self.local_lock_id = socket.gethostname() + str(os.getpid())

        # construct the two connection objects
        self.source_conn = client.connection(src)
        self.dest_conn = client.connection(dest)

    def prepare_lock(self):
        assert self.lock is None
        self.lock = lock.Lock(self.source_conn, self.type, self.local_lock_id,
                              self.log_lock_time, self.source_zone)
        self.lock.daemon = True
        self.lock.start()

MetadataEntry = namedtuple('MetadataEntry',
                           ['section', 'name', 'marker', 'timestamp'])

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
        self.type = 'metadata'

    def sync_meta(self, section, name):
        log.debug('syncing metadata type %s key %r', section, name)
        try:
            metadata = client.get_metadata(self.source_conn, section, name)
        except client.NotFound:
            log.debug('%s %r not found on master, deleting from secondary',
                      section, name)
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

class MetadataWorkerIncremental(MetadataWorker):

    def __init__(self, *args, **kwargs):
        self.daemon_id = kwargs['daemon_id']
        self.max_entries = kwargs['max_entries']
        super(MetadataWorkerIncremental, self).__init__(*args, **kwargs)

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
            self.sync_meta(section, name)

        if entries:
            try:
                client.set_worker_bound(self.dest_conn, 'metadata',
                                        shard_num, entries[-1].marker,
                                        entries[-1].timestamp,
                                        self.daemon_id)
                return len(entries), entries[-1].marker
            except:
                log.exception('error setting worker bound, may duplicate some work later')

        return 0, ''

    def run(self):
        self.prepare_lock()
        while True:
            shard_num = self.work_queue.get()
            if shard_num is None:
                log.info('process %s is done. Exiting', self.ident)
                break

            log.info('%s is processing shard number %d',
                     self.ident, shard_num)

            # first, lock the log
            try:
                self.lock.set_shard(shard_num)
                self.lock.acquire()
            except client.NotFound:
                # no log means nothing changed in this time period
                self.lock.unset_shard()
                self.result_queue.put((RESULT_SUCCESS, shard_num))
                continue
            except client.HttpError as e:
                log.info('error locking shard %d log, assuming'
                         ' it was processed by someone else and skipping: %s',
                         shard_num, e)
                self.lock.unset_shard()
                self.result_queue.put((RESULT_ERROR, shard_num))
                continue

            result = RESULT_SUCCESS
            try:
                marker, time = client.get_min_worker_bound(self.dest_conn,
                                                           'metadata',
                                                           shard_num)
                log.debug('oldest marker and time for shard %d are: %r %r',
                          shard_num, marker, time)
            except client.NotFound:
                # if no worker bounds have been set, start from the beginning
                marker, time = '', '1970-01-01 00:00:00'
            except Exception as e:
                log.exception('error getting worker bound for shard %d',
                              shard_num)
                result = RESULT_ERROR

            try:
                if result == RESULT_SUCCESS:
                    self.get_and_process_entries(marker, shard_num)
            except:
                log.exception('syncing entries from %s for shard %d failed',
                              marker, shard_num)
                result = RESULT_ERROR

            # finally, unlock the log
            try:
                self.lock.release_and_clear()
            except lock.LockBroken as e:
                log.warn('work may be duplicated: %s', e)
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
