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


class DataWorker(Worker):

    def __init__(self, *args, **kwargs):
        super(DataWorker, self).__init__(*args, **kwargs)
        self._type = 'data'

    # TODO actually use the markers
    def set_datalog_work_bound(self, shard_num, time_to_use):
        (ret, out) = client.request(self.source_conn,
                               ['replica_log', 'set', 'work_bound'],
                      {"id": shard_num, 'type': self._type, 'marker': 'FIIK', }) 

        if 200 != ret:
            print 'data list failed, returned http code: ', ret

    # get the updates for this bucket and sync the data across
    def sync_bucket(self, shard_num, bucket_name):
        ret = 200
        # There is not an explicit bucket-index log lock. This is coverred
        # by the lock on the datalog for this shard

        just_the_bucket = bucket_name.split(':')[0]
        print 'just the bucket: ', just_the_bucket
        # get the bilog for this bucket
        (ret, out) = client.request(self.source_conn,
                           ['log', 'list', 'type=bucket-index'],
                          #{"bucket":bucket_name, 'marker':dummy_marker })
                          {"bucket":bucket_name, 'bucket-instance':bucket_name}) 
                          #{"rgwx-bucket-instance":bucket_name})
                          #{"bucket":just_the_bucket})

        if 200 != ret:
            print 'get bucket-index for bucket ', bucket_name, \
                  ' failed, returned http code: ', ret
            return ret

        bucket_events = out()
        print 'bilog for bucket ', bucket_name, ' has ', \
              len(bucket_events), ' entries'

        # first, make sure the events are sorted in index_ver order
        sorted_events = sorted(bucket_events, key=lambda entry: entry['index_ver']) 
                          #reverse=True)

        for event in sorted_events:
            #make sure we still have the lock
            if self.relock_log:
                ret = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, \
                                           self.source_zone, shard_num)
                if 200 != ret:
                    print 'error acquiring lock for shard ', shard_num, \
                          ' lock_id: ', self.local_lock_id, \
                          ' in zone ', self.source_zone, \
                          ' in process_entries_for_data_log_shard(). ' \
                          ' Returned http code ', ret
                    # log unlocking and adding the return value to the result queue
                    # will be handled by the calling function
                    return ret

            if event['state'] == 'complete':
                print '   applying: ', event

                if event['op'] == 'write':
                    print 'copying object ', bucket_name + '/' + event['object']
                    # sync this operation from source to destination
                    # issue this against the destination rgw, since the
                    # operation is implemented as a 'pull' of the object
                    #
                    # TODO put real values in for rgwx-client-id and rgwx-op-od
                    (ret, out) = client.request(self.dest_conn,
                               ['object', 'add', bucket_name + '/' + event['object']], 
                      #{"bucket":bucket_name, 'marker':dummy_marker })
                              {"rgwx-source-zone":self.source_zone,
                               "rgwx-client-id":'joe bucks awesome client',
                               "rgwx-op-od":"42"})
                elif event['op'] == 'del':
                    print 'deleting object ', bucket_name + '/' + event['object']
                    # delete this object from the destination
                    (ret, out) = client.request(self.dest_conn,
                               ['object', 'rm', bucket_name + '/' + event['object']], )
                      #{"bucket":bucket_name, 'marker':dummy_marker })
                              #{"rgwx-source-zone":source_zone})

                else:
                    print 'no idea what op this is: ', event['op']
                    ret = 500

            if ret < 200 or ret > 299: # all 200 - 299 codes are success
                print 'sync of object ', event['object'], \
                      ' failed, returned http code: ', ret, \
                        '. Bailing'
                return ret

        return ret

    # sort by timestamp and then by name
    def sort_and_filter_entries(self, to_sort):

        data_entries = {}

        #debug
        print 'pre-filter count ', len(to_sort)

        # iterate over the sorted entries and keep only the first instance of
        # each entry
        for entry in to_sort:
            # just a bit of future-proofing as only bucket entri

            if entry['entity_type'] != 'bucket':
                print 'unknown entity type ', entry['entity_type']
                continue

            name = entry['key']

            # only add entries that are not already in the dict
            if not data_entries.has_key(name):
                data_entries[name] = ""

        #debug
        print 'post-filter count ', len(data_entries)

        return data_entries

    def process_entries_for_data_log_shard(self, shard_num, entries):
        ret = 200

        # we need this due to a bug in rgw that isn't auto-filling in sensible
        # defaults when start-time is omitted
        really_old_time = "2010-10-10 12:12:00"

        # NOTE rgw deals in UTC time. Make sure you adjust
        # your calls accordingly
        sync_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # sync each entry / tag pair
        # bail on any user where a non-200 status is returned
        for bucket_name in entries:
            if self.relock_log:
                ret = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, \
                                           self.source_zone, shard_num)
                if 200 != ret:
                    print 'error acquiring lock for shard ', shard_num, \
                          ' lock_id: ', self.local_lock_id, \
                          ' in zone ', self.source_zone, \
                          ' in process_entries_for_data_log_shard(). ' \
                          ' Returned http code ', ret
                    # log unlocking and adding the return value to the
                    # result queue will be handled by the calling
                    # function
                    return ret


                ret = self.sync_bucket(shard_num, bucket_name)

                if 200 != ret:
                    print 'sync_bucket() failed for bucket ', bucket_name, \
                          ', returned http code: ', ret

                    # if there is an error, release the log lock and bail
                    ret = self.release_log_lock(self.source_conn, \
                                              self.local_lock_id, \
                                              self.source_zone, shard_num)
                    return ret

        # TODO trim the log and then unlock it
        # trim the log for this bucket now that its objects are synced
        (ret, out) = client.request(self.source_conn,
                  ['log', 'trim', 'id=' + str(shard_num)],
                  {'id': shard_num, 'type': 'data',
                   'start-time': really_old_time, 'end-time': sync_start_time})

        if 200 != ret:
            print 'data log trim for shard ', shard_num, ' returned http code ', ret
            # normally we would unlock and return a avlue here,
            # but since that's going to happen next, we effectively just fall through
            # into it

        ret = self.release_log_lock(self.source_conn, \
                                       self.local_lock_id, \
                                       self.source_zone, shard_num)
        return ret


    def run(self):
        while True:   # keep looping until we break
            shard_num = self.work_queue.get()
            if shard_num is None:
                log.debug('process {0} is done with all available shards',
                          self.processName)
                break

            log.debug('{0} is processing shard {1}',
                      self.processName, shard_num)

            # first, lock the data log
            ret = self.acquire_log_lock(self.source_conn,
                                           self.local_lock_id,
                                           self.source_zone,
                                           shard_num)

            if 200 != ret:
                print 'acquire_log_lock() failed, returned http code: ', ret
                self.result_queue.put((self.processID, shard_num, ret))
                continue

            # get the log for this data log shard
            (ret, out) = client.request(self.source_conn,
                                   ['log', 'list', 'id=' + str(shard_num)],
                                   {'type': 'data', 'id': shard_num})

            if 200 != ret:
                print 'data list for shard ', shard_num, \
                      ' failed, returned http code: ', ret
                # we hit an error getting the data to sync.
                # Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, ret))
                continue
            log.debug('data list for shard {0} returned {1}', shard_num, ret)

            log_entry_list = out()

            log.debug('shard {0} has {1} entries',
                      shard_num, len(log_entry_list))

            # filter the entries so that a given entry only shows up once
            # to be synced
            buckets_to_sync = self.sort_and_filter_entries(log_entry_list)

            ret = self.process_entries_for_data_log_shard(shard_num, \
                                                             buckets_to_sync)

            self.result_queue.put((self.processID, shard_num, '200'))


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
        except client.HttpError as e:
            log.error('error getting metadata for %s "%s": %s',
                      section, name, e)
            raise
        except client.NotFound:
            client.delete_metadata(self.dest_conn, section, name)
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
                log.debug('oldest marker and time for shard %d are: "%s" "%s"',
                          shard_num, marker, time)
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
