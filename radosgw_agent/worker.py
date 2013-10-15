from collections import namedtuple
import logging
import multiprocessing
import os
import socket
import time

from radosgw_agent import client
from radosgw_agent import lock

log = logging.getLogger(__name__)

RESULT_SUCCESS = 0
RESULT_ERROR = 1

class SkipShard(Exception):
    pass

class SyncError(Exception):
    pass
class SyncTimedOut(SyncError):
    pass
class SyncFailed(SyncError):
    pass

DEFAULT_TIME = '1970-01-01 00:00:00'

class Worker(multiprocessing.Process):
    """sync worker to run in its own process"""

    def __init__(self, work_queue, result_queue, log_lock_time,
                 src, dest, **kwargs):
        super(Worker, self).__init__()
        self.src = src
        self.dest = dest
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.log_lock_time = log_lock_time
        self.lock = None

        self.local_lock_id = socket.gethostname() + ':' + str(os.getpid())

        # construct the two connection objects
        self.src_conn = client.connection(src)
        self.dest_conn = client.connection(dest)

    def prepare_lock(self):
        assert self.lock is None
        self.lock = lock.Lock(self.dest_conn, self.type, self.local_lock_id,
                              self.log_lock_time, self.dest.zone.name)
        self.lock.daemon = True
        self.lock.start()

    def lock_shard(self, shard_num):
        result = shard_num, []
        try:
            self.lock.set_shard(shard_num)
            self.lock.acquire()
        except client.NotFound:
            # no log means nothing changed this shard yet
            self.lock.unset_shard()
            self.result_queue.put((RESULT_SUCCESS, result))
            raise SkipShard('no log for shard')
        except Exception as e:
            log.warn('error locking shard %d log, '
                     ' skipping for now: %s',
                     shard_num, e)
            self.lock.unset_shard()
            self.result_queue.put((RESULT_ERROR, result))
            raise SkipShard()

    def unlock_shard(self):
        try:
            self.lock.release_and_clear()
        except lock.LockBroken as e:
            log.warn('work may be duplicated: %s', e)
        except Exception as e:
            log.warn('error unlocking log, continuing anyway '
                     'since lock will timeout')

    def set_bound(self, key, marker, retries, type_=None):
        # api doesn't allow setting a bound with a blank marker
        if marker:
            if type_ is None:
                type_ = self.type
            try:
                data = [dict(name=item, time=DEFAULT_TIME) for item in retries]
                client.set_worker_bound(self.dest_conn,
                                        type_,
                                        marker,
                                        DEFAULT_TIME,
                                        self.daemon_id,
                                        key,
                                        data=data)
                return RESULT_SUCCESS
            except Exception as e:
                log.warn('error setting worker bound for key "%s",'
                         ' may duplicate some work later: %s', key, e)
                return RESULT_ERROR

MetadataEntry = namedtuple('MetadataEntry',
                           ['section', 'name', 'marker', 'timestamp'])

def _meta_entry_from_json(entry):
    return MetadataEntry(
        entry['section'],
        entry['name'],
        entry['id'],
        entry['timestamp'],
        )

BucketIndexEntry = namedtuple('BucketIndexEntry',
                              ['object', 'marker', 'timestamp'])

def _bi_entry_from_json(entry):
    return BucketIndexEntry(
        entry['object'],
        entry['op_id'],
        entry['timestamp'],
        )

class IncrementalMixin(object):
    """This defines run() and get_and_process_entries() for incremental sync.

    These are the same for data and metadata sync, so share their
    implementation here.
    """

    def run(self):
        self.prepare_lock()
        while True:
            item = self.work_queue.get()
            if item is None:
                log.info('process %s is done. Exiting', self.ident)
                break

            shard_num, (log_entries, retries) = item

            log.info('%s is processing shard number %d',
                     self.ident, shard_num)

            # first, lock the log
            try:
                self.lock_shard(shard_num)
            except SkipShard:
                continue

            result = RESULT_SUCCESS
            try:
                new_retries = self.sync_entries(log_entries, retries)
            except Exception:
                log.exception('syncing entries for shard %d failed',
                              shard_num)
                result = RESULT_ERROR
                new_retries = []

            # finally, unlock the log
            self.unlock_shard()
            self.result_queue.put((result, (shard_num, new_retries)))
            log.info('finished processing shard %d', shard_num)


class DataWorker(Worker):

    def __init__(self, *args, **kwargs):
        super(DataWorker, self).__init__(*args, **kwargs)
        self.type = 'data'
        self.op_id = 0
        self.object_sync_timeout = kwargs.get('object_sync_timeout', 60 * 60 * 60)
        self.daemon_id = kwargs['daemon_id']

    def sync_object(self, bucket, obj):
        self.op_id += 1
        local_op_id = self.local_lock_id + ':' +  str(self.op_id)
        try:
            until = time.time() + self.object_sync_timeout
            client.sync_object_intra_region(self.dest_conn, bucket, obj,
                                            self.src.zone.name,
                                            self.daemon_id,
                                            local_op_id)
        except client.NotFound:
            log.debug('"%s/%s" not found on master, deleting from secondary',
                      bucket, obj)
            try:
                client.delete_object(self.dest_conn, bucket, obj)
            except client.NotFound:
                # Since we were trying to delete the object, just return
                return
            except Exception:
                msg = 'could not delete "%s/%s" from secondary' % (bucket, obj)
                log.exception(msg)
                raise SyncFailed(msg)
        except SyncFailed:
            raise
        except Exception as e:
            log.debug('exception during sync: %s', e)
            self.wait_for_object(bucket, obj, until)
        # TODO: clean up old op states
        try:
            client.remove_op_state(self.dest_conn, self.daemon_id, local_op_id,
                                   bucket, obj)
        except Exception:
            log.exception('could not remove op state for daemon "%s" op_id %s',
                          self.daemon_id, local_op_id)

    def wait_for_object(self, bucket, obj, until):
        while time.time() < until:
            try:
                state = client.get_op_state(self.dest_conn,
                                            self.daemon_id,
                                            self.op_id,
                                            bucket, obj)
                log.debug('op state is %s', state)
                state = state[0]['state']
                if state == 'complete':
                    return
                elif state != 'in-progress':
                    raise SyncFailed('state is {0}'.format(state))
                time.sleep(1)
            except SyncFailed:
                raise
            except Exception as e:
                log.debug('error geting op state: %s', e)
                time.sleep(1)
        # timeout expired
        raise SyncTimedOut()

    def get_bucket_instance(self, bucket):
        metadata = client.get_metadata(self.src_conn, 'bucket', bucket)
        return bucket + ':' + metadata['data']['bucket']['bucket_id']

    def get_bucket(self, bucket_instance):
        return bucket_instance.split(':', 1)[0]

    def sync_bucket(self, bucket, objects):
        log.info('syncing bucket "%s"', bucket)
        retry_objs = []
        count = 0
        for obj in objects:
            count += 1
            # sync each object
            log.debug('syncing object "%s/%s"', bucket, obj),
            try:
                self.sync_object(bucket, obj)
            except SyncError as err:
                log.error('failed to sync object %s/%s: %s',
                          bucket, obj, err)
                retry_objs.append(obj)

        log.debug('bucket {bucket} has {num_objects} object'.format(
                  bucket=bucket, num_objects=count))
        if retry_objs:
            log.debug('these objects failed to be synced and will be during '
                      'the next incremental sync: %s', retry_objs)

        return retry_objs


class DataWorkerIncremental(IncrementalMixin, DataWorker):

    def __init__(self, *args, **kwargs):
        super(DataWorkerIncremental, self).__init__(*args, **kwargs)
        self.max_entries = kwargs['max_entries']

    def inc_sync_bucket_instance(self, instance, marker, timestamp, retries):
        try:
            log_entries = client.get_log(self.src_conn, 'bucket-index',
                                         marker, self.max_entries, instance)
        except client.NotFound:
            log_entries = []

        log.info('bucket instance "%s" has %d entries after "%s"', instance,
                 len(log_entries), marker)

        try:
            entries = [_bi_entry_from_json(entry) for entry in log_entries]
        except KeyError:
            log.error('log missing key is: %s', log_entries)
            raise

        if entries:
            max_marker, timestamp = entries[-1].marker, entries[-1].timestamp
        else:
            max_marker, timestamp = '', DEFAULT_TIME
        objects = set([entry.object for entry in entries])
        bucket = self.get_bucket(instance)
        new_retries = self.sync_bucket(bucket, objects.union(retries))

        result = self.set_bound(instance, max_marker, new_retries,
                                'bucket-index')
        if new_retries:
            result = RESULT_ERROR
        return result

    def sync_entries(self, log_entries, retries):
        try:
            bucket_instances = set([entry['key'] for entry in log_entries])
        except KeyError:
            log.error('log containing bad key is: %s', log_entries)
            raise

        new_retries = []
        for bucket_instance in bucket_instances.union(retries):
            try:
                marker, timestamp, retries = client.get_worker_bound(
                    self.dest_conn,
                    'bucket-index',
                    bucket_instance)
            except client.NotFound:
                log.debug('no worker bound found for bucket instance "%s"',
                          bucket_instance)
                marker, timestamp, retries = '', DEFAULT_TIME, []
            try:
                sync_result = self.inc_sync_bucket_instance(bucket_instance,
                                                            marker,
                                                            timestamp,
                                                            retries)
            except Exception as e:
                log.warn('error syncing bucket instance "%s": %s',
                         bucket_instance, e)
                sync_result = RESULT_ERROR
            if sync_result == RESULT_ERROR:
                new_retries.append(bucket_instance)

        return new_retries

class DataWorkerFull(DataWorker):

    def full_sync_bucket(self, bucket):
        try:
            instance = self.get_bucket_instance(bucket)
            marker = client.get_log_info(self.src_conn, 'bucket-index',
                                         instance)['max_marker']
            log.debug('bucket instance is "%s" with marker %s', instance, marker)
            # nothing to do for this bucket
            if not marker:
                return True

            objects = client.list_objects_in_bucket(self.src_conn, bucket)
            if not objects:
                return True
        except Exception as e:
            log.error('error preparing for full sync of bucket "%s": %s',
                      bucket, e)
            return False

        retries = self.sync_bucket(bucket, objects)

        result = self.set_bound(instance, marker, retries, 'bucket-index')
        return not retries and result == RESULT_SUCCESS

    def run(self):
        self.prepare_lock()
        while True:
            item = self.work_queue.get()
            if item is None:
                log.info('No more entries in queue, exiting')
                break

            shard_num, buckets = item

            # first, lock the log
            try:
                self.lock_shard(shard_num)
            except SkipShard:
                continue

            # attempt to sync each bucket, add to a list to retry
            # during incremental sync if sync fails
            retry_buckets = []
            for bucket in buckets:
                if not self.full_sync_bucket(bucket):
                    retry_buckets.append(bucket)

            # unlock shard and report buckets to retry during incremental sync
            self.unlock_shard()
            self.result_queue.put((RESULT_SUCCESS, (shard_num, retry_buckets)))
            log.info('finished syncing shard %d', shard_num)
            log.info('incremental sync will need to retry buckets: %s',
                     retry_buckets)

class MetadataWorker(Worker):

    def __init__(self, *args, **kwargs):
        super(MetadataWorker, self).__init__(*args, **kwargs)
        self.type = 'metadata'

    def sync_meta(self, section, name):
        log.debug('syncing metadata type %s key "%s"', section, name)
        try:
            metadata = client.get_metadata(self.src_conn, section, name)
        except client.NotFound:
            log.debug('%s "%s" not found on master, deleting from secondary',
                      section, name)
            try:
                client.delete_metadata(self.dest_conn, section, name)
            except client.NotFound:
                # Since this error is handled appropriately, return success
                return RESULT_SUCCESS
        except Exception as e:
            log.error('error getting metadata for %s "%s": %s',
                      section, name, e)
            return RESULT_ERROR
        else:
            try:
                client.update_metadata(self.dest_conn, section, name, metadata)
                return RESULT_SUCCESS
            except Exception as e:
                log.error('error updating metadata for %s "%s": %s',
                          section, name, e)
                return RESULT_ERROR

class MetadataWorkerIncremental(IncrementalMixin, MetadataWorker):

    def __init__(self, *args, **kwargs):
        super(MetadataWorkerIncremental, self).__init__(*args, **kwargs)

    def sync_entries(self, log_entries, retries):
        try:
            entries = [_meta_entry_from_json(entry) for entry in log_entries]
        except KeyError:
            log.error('log containing bad key is: %s', log_entries)
            raise

        new_retries = []
        mentioned = set([(entry.section, entry.name) for entry in entries])
        split_retries = [entry.split('/', 1) for entry in retries]
        for section, name in mentioned.union(split_retries):
            sync_result = self.sync_meta(section, name)
            if sync_result == RESULT_ERROR:
                new_retries.append(section + '/' + name)

        return new_retries

class MetadataWorkerFull(MetadataWorker):

    def empty_result(self, shard):
        return shard, []

    def run(self):
        self.prepare_lock()
        while True:
            item = self.work_queue.get()
            if item is None:
                log.info('No more entries in queue, exiting')
                break

            log.debug('syncing item "%s"', item)

            shard_num, metadata = item

            # first, lock the log
            try:
                self.lock_shard(shard_num)
            except SkipShard:
                continue

            # attempt to sync each bucket, add to a list to retry
            # during incremental sync if sync fails
            retries = []
            for section, name in metadata:
                try:
                    self.sync_meta(section, name)
                except Exception as e:
                    log.warn('could not sync %s "%s", saving for retry: %s',
                             section, name, e)
                    retries.append(section + '/' + name)

            # unlock shard and report buckets to retry during incremental sync
            self.unlock_shard()
            self.result_queue.put((RESULT_SUCCESS, (shard_num, retries)))
            log.info('finished syncing shard %d', shard_num)
            log.info('incremental sync will need to retry items: %s',
                     retries)
