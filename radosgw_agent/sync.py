import datetime
import logging
import multiprocessing

from radosgw_agent import worker
from radosgw_agent import client

log = logging.getLogger(__name__)

NEEDS_SYNC = 'NEEDSSYNC'
SYNC_IN_PROGRESS = 'INPROGRESS'
SYNC_COMPLETED = 'DONESYNC'

class Syncer(object):
    def __init__(self, type_, src, dest, daemon_id):
        self.type = type_
        self.src = src
        self.dest = dest
        self.src_conn = client.connection(src)
        self.dest_conn = client.connection(dest)
        self.daemon_id = daemon_id
        self.worker_cls = None # filled in by subclass constructor

    def get_num_shards(self):
        try:
            num_shards = client.num_log_shards(self.src_conn, self.type)
            log.debug('%d shards to check', num_shards)
            return num_shards
        except Exception:
            log.exception('finding number of shards failed')
            raise

    def prepare(self):
        """Setup any state required before syncing starts"""
        pass

    def generate_work(self):
        """Generate items to be place in a queue or processing"""
        pass

    def complete(self):
        """Called when syncing completes successfully"""
        pass

    def get_worker_cls(self):
        """Return the subclass of Worker to run"""
        pass

    def sync(self, num_workers, log_lock_time, max_entries=None):
        self.prepare()

        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        processes = [self.worker_cls(workQueue,
                                     resultQueue,
                                     log_lock_time,
                                     self.src,
                                     self.dest,
                                     daemon_id=self.daemon_id,
                                     max_entries=max_entries)
                     for i in xrange(num_workers)]
        for process in processes:
            process.daemon = True
            process.start()

        log.info('Starting sync')
        # enqueue the shards to be synced
        num_items = 0
        for item in self.generate_work():
            num_items += 1
            workQueue.put(item)

        # add a poison pill for each worker
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        errors = []
        for i in xrange(num_items):
            result, item = resultQueue.get()
            if result == worker.RESULT_SUCCESS:
                log.debug('synced item %r', item)
            else:
                log.error('error syncing item %r', item)
                errors.append(item)

            log.info('%d/%d items processed', i + 1, num_items)
        if errors:
            log.error('Encountered errors syncing these %d items: %r',
                      len(errors), errors)
        else:
            self.complete()


class MetaSyncerInc(Syncer):

    def __init__(self, *args, **kwargs):
        super(MetaSyncerInc, self).__init__(*args, **kwargs)
        self.worker_cls = worker.MetadataWorkerIncremental

    def generate_work(self):
        return xrange(self.get_num_shards())


class DataSyncerFull(Syncer):

    def __init__(self, *args, **kwargs):
        super(DataSyncerFull, self).__init__(*args, **kwargs)
        self.worker_cls = worker.DataWorkerFull

    def shard_num_for_bucket(self, bucket_name, num_shards):
        bucket_name = bucket_name.encode('utf8')
        hash_val = 0
        for char in bucket_name:
            c = ord(char)
            hash_val = (hash_val + (c << 4) + (c >> 4)) * 11;
        return hash_val % num_shards;

    def get_bucket_instance(self, bucket_name):
        metadata = client.get_metadata(self.src_conn, 'bucket', bucket_name)
        return bucket_name + ':' + metadata['data']['bucket']['bucket_id']

    def prepare(self):
        # TODO we need some sort of a lock here to make sure that only
        # one client is getting a list of buckets to sync so that it's 
        # consistent.

        self.num_shards = self.get_num_shards()

        # get list of buckets before getting any markers to avoid inconsistency
        buckets = client.get_bucket_list(self.src_conn)

        # save data log markers for each shard
        self.shard_info = []
        for shard in xrange(self.num_shards):
            info = client.get_log_info(self.src_conn, 'data', shard)
            # setting an empty marker returns an error
            if info['marker']:
                self.shard_info.append((shard, info['marker'],
                                        info['last_update']))

        # bucket index info doesn't include a timestamp, so just use
        # local time since it isn't important for correctness
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        self.bucket_info = []
        for bucket_name in buckets:
            instance = self.get_bucket_instance(bucket_name)
            marker = client.get_bucket_index_marker('bucket-instance', instance)
            self.bucket_info.append((instance, marker, now))

    def generate_work(self):
        return self.bucket_info

    def complete(self):
        for shard_num, marker, time in self.shard_info:
            out = client.set_worker_bound(self.dest_conn, 'data', marker,
                                          time, self.daemon_id,
                                          shard_num=shard_num)
            log.debug('jbuck, set replica log output:\n%s', out)

        for bucket_instance, marker, time in self.bucket_info:
            out = client.set_worker_bound(self.dest_conn, 'bucket-index', marker,
                                          time, self.daemon_id,
                                          bucket_instance=bucket_instance)
            log.debug('set_worker_bound on bucket index replica log returned:\n%s', out)


class MetaSyncerFull(Syncer):
    def __init__(self, *args, **kwargs):
        super(MetaSyncerInc, self).__init__(*args, **kwargs)
        self.worker_cls = worker.MetadataWorkerFull

    def prepare(self):
        try:
            self.sections = client.get_metadata_sections(self.src_conn)
        except client.HttpError as e:
            log.error('Error listing metadata sections: %s', e)
            raise

        # grab the lastest shard markers and timestamps before we sync
        self.shard_info = []
        num_shards = self.get_num_shards()
        for shard_num in xrange(num_shards):
            info = client.get_log_info(self.src_conn, 'metadata', shard_num)
            # setting an empty marker returns an error
            if info['marker']:
                self.shard_info.append((shard_num, info['marker'],
                                        info['last_update']))

    def generate_work(self):
        for section in self.sections:
            try:
                yield [(section, key) for key in
                       client.list_metadata_keys(self.src_conn, section)]
            except client.NotFound:
                # no keys of this type exist
                continue
            except client.HttpError as e:
                log.error('Error listing metadata for section %s: %s',
                          section, e)
                raise

    def complete(self):
        for shard_num, marker, timestamp in self.shard_info:
            client.set_worker_bound(self.dest_conn, 'metadata', shard_num,
                                    marker, timestamp, self.daemon_id)
            client.del_worker_bound(self.dest_conn, 'metadata', shard_num,
                                    self.daemon_id)
