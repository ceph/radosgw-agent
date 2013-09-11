import datetime
import hashlib
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

    def prepare(self):
        # TODO we need some sort of a lock here to make sure that only
        # one client is getting a list of buckets to sync so that it's 
        # consistent.

        num_data_shards = self.get_num_shards()

        # get the set of all buckets and then add an entry to the data replica 
        # log for each
        buckets = client.get_bucket_list(self.src_conn) 
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        shard_to_bucket_dict = dict()
        for bucket_name in buckets:
            bucket_hash = int(hashlib.md5(bucket_name).hexdigest(),16) % num_data_shards
            if bucket_hash not in shard_to_bucket_dict:
                shard_to_bucket_dict[bucket_hash] = list()
            # TODO: the docs for set_worker_bound suggest that the data payload should be a list of dicts, with each dict
            # haveing a 'bucket' and 'time' entry. I'm trying to stash the state of the bucket (NEEDSSYNC) and the 
            # bucket name as the value. May need a different approach / delimiter
            shard_to_bucket_dict[bucket_hash].append({
                                                      'bucket': '{needssync}:{bucket_name}'.format(needssync=NEEDS_SYNC,bucket_name=bucket_name),
                                                      'time':now
                                                      })  
        for shard_num, entries in shard_to_bucket_dict.items():
            # this call returns a list of buckets that are "in progress" from before the time in the "now" variable
            # TODO: sort out if this is the proper usage of set_worker_bound
            replica_log_output = client.set_worker_bound(self.dest_conn, 'data', shard_num, 
                'buckets_in_shard_{n}'.format(n=shard_num), 
                now, self.daemon_id,in_data=entries
                )
            log.debug('jbuck, set replica log output:\n{data}'.format(data=replica_log_output))

    def generate_work(self):
        return xrange(self.get_num_shards())

    def complete(self):
        # TODO: set replica log
        pass


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
