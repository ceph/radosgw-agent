from collections import namedtuple
from itertools import ifilter
import logging
import multiprocessing
import os
import socket
import time

from radosgw_agent import client
from radosgw_agent import lock
from radosgw_agent.util import obj as obj_, get_dev_logger
from radosgw_agent.exceptions import SkipShard, SyncError, SyncTimedOut, SyncFailed, NotFound, BucketEmpty
from radosgw_agent.constants import DEFAULT_TIME, RESULT_SUCCESS, RESULT_ERROR

log = logging.getLogger(__name__)
dev_log = get_dev_logger(__name__)


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
        except NotFound:
            # no log means nothing changed this shard yet
            self.lock.unset_shard()
            self.result_queue.put((RESULT_SUCCESS, result))
            raise SkipShard('no log for shard')
        except Exception:
            log.warn('error locking shard %d log, '
                     ' skipping for now. Traceback: ',
                     shard_num, exc_info=True)
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
                     'since lock will timeout. Traceback:', exc_info=True)

    def set_bound(self, key, marker, retries, type_=None):
        # api doesn't allow setting a bound with a blank marker
        if marker:
            if type_ is None:
                type_ = self.type
            try:
                data = [
                    obj_.to_dict(item, time=DEFAULT_TIME) for item in retries
                ]
                client.set_worker_bound(self.dest_conn,
                                        type_,
                                        marker,
                                        DEFAULT_TIME,
                                        self.daemon_id,
                                        key,
                                        data=data)
                return RESULT_SUCCESS
            except Exception:
                log.warn('error setting worker bound for key "%s",'
                         ' may duplicate some work later. Traceback:', key,
                         exc_info=True)
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
                              [
                                  'object',
                                  'marker',
                                  'timestamp',
                                  'op',
                                  'versioned',
                                  'ver',
                                  'name',
                                  # compatibility with boto objects:
                                  'VersionedEpoch',
                                  'version_id',
                              ])

BucketVer = namedtuple('BucketVer',
        [
            'epoch',
            'pool',
        ])


def _bi_entry_from_json(entry):
    ver = entry.get('ver', {})
    entry_ver = BucketVer(
        ver.get('epoch'),
        ver.get('pool')
    )

    # compatibility with boto objects:
    VersionedEpoch = ver.get('epoch')
    version_id = entry.get('instance', 'null')

    return BucketIndexEntry(
        entry['object'],
        entry['op_id'],
        entry['timestamp'],
        entry.get('op', ''),
        entry.get('versioned', False),
        entry_ver,
        entry['object'],
        VersionedEpoch,
        version_id,
        )


def filter_versioned_objects(entry):
    """
    On incremental sync operations, the log may indicate that 'olh' entries,
    which should be ignored. So this filter function will check for the
    different attributes present in an ``entry`` and return only valid ones.

    This should be backwards compatible with older gateways that return log
    entries that don't support versioning.
    """
    # do not attempt filtering on non-versioned entries
    if not entry.versioned:
        return entry

    # writes or delete 'op' values should be ignored
    if entry.op not in ['write', 'delete']:
        # allowed op states are `link_olh` and `link_olh_del`
        return entry


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
                dev_log.info('process %s is done. Exiting', self.ident)
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
        log.debug('syncing object %s/%s', bucket, obj.name)
        self.op_id += 1
        local_op_id = self.local_lock_id + ':' +  str(self.op_id)
        found = False

        try:
            until = time.time() + self.object_sync_timeout
            client.sync_object_intra_region(self.dest_conn, bucket, obj,
                                            self.src.zone.name,
                                            self.daemon_id,
                                            local_op_id)
            found = True
        except NotFound:
            log.debug('object "%s/%s" not found on master, deleting from secondary',
                      bucket, obj.name)
            try:
                client.delete_object(self.dest_conn, bucket, obj)
            except NotFound:
                # Since we were trying to delete the object, just return
                return False
            except Exception:
                msg = 'could not delete "%s/%s" from secondary' % (bucket, obj.name)
                log.exception(msg)
                raise SyncFailed(msg)
        except SyncFailed:
            raise
        except Exception as error:
            msg = 'encountered an error during sync'
            dev_log.warn(msg, exc_info=True)
            log.warning('%s: %s' % (msg, error))
            # wait for it if the op state is in-progress
            self.wait_for_object(bucket, obj, until, local_op_id)
        # TODO: clean up old op states
        try:
            if found:
                client.remove_op_state(self.dest_conn, self.daemon_id,
                                       local_op_id, bucket, obj)
        except NotFound:
            log.debug('op state already gone')
        except Exception:
            log.exception('could not remove op state for daemon "%s" op_id %s',
                          self.daemon_id, local_op_id)

        return True

    def wait_for_object(self, bucket, obj, until, local_op_id):
        while time.time() < until:
            try:
                state = client.get_op_state(self.dest_conn,
                                            self.daemon_id,
                                            local_op_id,
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
            except NotFound:
                raise SyncFailed('object copy state not found')
            except Exception as e:
                log.debug('error geting op state: %s', e, exc_info=True)
                time.sleep(1)
        # timeout expired
        raise SyncTimedOut()

    def get_bucket_instance(self, bucket):
        metadata = client.get_metadata(self.src_conn, 'bucket', bucket)
        return bucket + ':' + metadata['data']['bucket']['bucket_id']

    def get_bucket(self, bucket_instance):
        return bucket_instance.split(':', 1)[0]

    def sync_bucket(self, bucket, objects):
        log.info('*'*80)
        log.info('syncing bucket "%s"', bucket)
        retry_objs = []
        count = 0
        for obj in objects:
            try:
                self.sync_object(bucket, obj)
                count += 1
            except SyncError as err:
                log.error('failed to sync object %s/%s: %s',
                          bucket, obj.name, err)
                log.warning(
                    'will retry sync of failed object at next incremental sync'
                )
                retry_objs.append(obj)
        log.info('synced %s objects' % count)
        log.info('completed syncing bucket "%s"', bucket)
        log.info('*'*80)

        return retry_objs


class DataWorkerIncremental(IncrementalMixin, DataWorker):

    def __init__(self, *args, **kwargs):
        super(DataWorkerIncremental, self).__init__(*args, **kwargs)
        self.max_entries = kwargs['max_entries']

    def get_bucket_instance_entries(self, marker, instance):
        entries = []
        while True:
            try:
                log_entries = client.get_log(self.src_conn, 'bucket-index',
                                             marker, self.max_entries, instance)
            except NotFound:
                log_entries = []

            log.debug('bucket instance "%s" has %d entries after "%s"', instance,
                      len(log_entries), marker)

            try:
                entries += [_bi_entry_from_json(entry) for entry in log_entries]
            except KeyError:
                log.error('log missing key is: %s', log_entries)
                raise

            if entries:
                marker = entries[-1].marker
            else:
                marker = ' '

            if len(log_entries) < self.max_entries:
                break
        return marker, entries

    def inc_sync_bucket_instance(self, instance, marker, timestamp, retries):
        max_marker, entries = self.get_bucket_instance_entries(marker, instance)

        # regardless if entries are versioned, make sure we filter them
        entries = [i for i in ifilter(filter_versioned_objects, entries)]

        objects = set([entry for entry in entries])
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
            if ':' not in bucket_instance:
                # it's just a plain bucket from an old version of the agent
                bucket_instance = self.get_bucket_instance(bucket_instance)

            bound = client.get_worker_bound(
                self.dest_conn,
                'bucket-index',
                bucket_instance)

            marker = bound['marker']
            # remap dictionaries to object-like
            retries = [obj_.to_obj(i) for i in bound['retries']]
            timestamp = bound['oldest_time']

            try:
                sync_result = self.inc_sync_bucket_instance(bucket_instance,
                                                            marker,
                                                            timestamp,
                                                            retries)
            except Exception as e:
                log.warn('error syncing bucket instance "%s": %s',
                         bucket_instance, e, exc_info=True)
                sync_result = RESULT_ERROR
            if sync_result == RESULT_ERROR:
                new_retries.append(bucket_instance)

        return new_retries


class DataWorkerFull(DataWorker):

    def full_sync_bucket(self, bucket):
        try:
            instance = self.get_bucket_instance(bucket)
            try:
                marker = client.get_log_info(self.src_conn, 'bucket-index',
                                             instance)['max_marker']
            except NotFound:
                marker = ' '
            log.debug('bucket instance is "%s" with marker %s', instance, marker)

            objects = client.list_objects_in_bucket(self.src_conn, bucket)
            retries = self.sync_bucket(bucket, objects)

            result = self.set_bound(instance, marker, retries, 'bucket-index')
            return not retries and result == RESULT_SUCCESS
        except BucketEmpty:
            log.debug('no objects in bucket %s', bucket)
            return True
        except Exception:
            log.exception('error preparing for full sync of bucket "%s"',
                          bucket)
            return False

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
            if retry_buckets:
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
        except NotFound:
            log.debug('%s "%s" not found on master, deleting from secondary',
                      section, name)
            try:
                client.delete_metadata(self.dest_conn, section, name)
            except NotFound:
                # Since this error is handled appropriately, return success
                return RESULT_SUCCESS
        except Exception as e:
            log.warn('error getting metadata for %s "%s": %s',
                     section, name, e, exc_info=True)
            return RESULT_ERROR
        else:
            try:
                client.update_metadata(self.dest_conn, section, name, metadata)
                return RESULT_SUCCESS
            except Exception as e:
                log.warn('error updating metadata for %s "%s": %s',
                          section, name, e, exc_info=True)
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
        split_retries = [tuple(entry.split('/', 1)) for entry in retries]
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
                             section, name, e, exc_info=True)
                    retries.append(section + '/' + name)

            # unlock shard and report buckets to retry during incremental sync
            self.unlock_shard()
            self.result_queue.put((RESULT_SUCCESS, (shard_num, retries)))
            log.info('finished syncing shard %d', shard_num)
            log.info('incremental sync will need to retry items: %s',
                     retries)
