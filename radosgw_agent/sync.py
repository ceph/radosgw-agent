import logging
import multiprocessing
import time

from radosgw_agent import worker
from radosgw_agent import client
from radosgw_agent.util import get_dev_logger
from radosgw_agent.exceptions import NotFound, HttpError


log = logging.getLogger(__name__)
dev_log = get_dev_logger(__name__)

# the replica log api only supports one entry, and updating it
# requires sending a daemon id that matches the existing one. This
# doesn't make a whole lot of sense with the current structure of
# radosgw-agent, so just use a constant value for the daemon id.
DAEMON_ID = 'radosgw-agent'

def prepare_sync(syncer, error_delay):
    """Attempt to prepare a syncer for running a sync.

    :param error_delay: seconds to wait before retrying

    This will retry forever so the sync agent continues if radosgws
    are unavailable temporarily.
    """
    while True:
        try:
            syncer.prepare()
            break
        except Exception:
            log.warn('error preparing for sync, will retry. Traceback:',
                     exc_info=True)
            time.sleep(error_delay)

def incremental_sync(meta_syncer, data_syncer, num_workers, lock_timeout,
                     incremental_sync_delay, metadata_only, error_delay):
    """Run a continuous incremental sync.

    This will run forever, pausing between syncs by a
    incremental_sync_delay seconds.
    """
    while True:
        try:
            meta_syncer.sync(num_workers, lock_timeout)
            if not metadata_only:
                data_syncer.sync(num_workers, lock_timeout)
        except Exception:
            log.warn('error doing incremental sync, will try again. Traceback:',
                     exc_info=True)

        # prepare data before sleeping due to rgw_log_bucket_window
        if not metadata_only:
            prepare_sync(data_syncer, error_delay)
        log.info('waiting %d seconds until next sync',
                 incremental_sync_delay)
        time.sleep(incremental_sync_delay)
        prepare_sync(meta_syncer, error_delay)

class Syncer(object):
    def __init__(self, src, dest, max_entries, *args, **kwargs):
        self.src = src
        self.dest = dest
        self.src_conn = client.connection(src)
        self.dest_conn = client.connection(dest)
        self.daemon_id = DAEMON_ID
        self.worker_cls = None # filled in by subclass constructor
        self.num_shards = None
        self.max_entries = max_entries
        self.object_sync_timeout = kwargs.get('object_sync_timeout')

    def init_num_shards(self):
        if self.num_shards is not None:
            return
        try:
            self.num_shards = client.num_log_shards(self.src_conn, self.type)
            log.debug('%d shards to check', self.num_shards)
        except Exception:
            log.error('finding number of shards failed')
            raise

    def shard_num_for_key(self, key):
        key = key.encode('utf8')
        hash_val = 0
        for char in key:
            c = ord(char)
            hash_val = (hash_val + (c << 4) + (c >> 4)) * 11
        return hash_val % self.num_shards

    def prepare(self):
        """Setup any state required before syncing starts.

        This must be called before sync().
        """
        pass

    def generate_work(self):
        """Generate items to be place in a queue or processing"""
        pass

    def wait_until_ready(self):
        pass

    def complete_item(self, shard_num, retries):
        """Called when syncing a single item completes successfully"""
        marker = self.shard_info.get(shard_num)
        if not marker:
            return
        try:
            data = [dict(name=retry, time=worker.DEFAULT_TIME)
                    for retry in retries]
            client.set_worker_bound(self.dest_conn,
                                    self.type,
                                    marker,
                                    worker.DEFAULT_TIME,
                                    self.daemon_id,
                                    shard_num,
                                    data)
        except Exception:
            log.warn('could not set worker bounds, may repeat some work.'
                     'Traceback:', exc_info=True)

    def sync(self, num_workers, log_lock_time):
        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        processes = [self.worker_cls(workQueue,
                                     resultQueue,
                                     log_lock_time,
                                     self.src,
                                     self.dest,
                                     daemon_id=self.daemon_id,
                                     max_entries=self.max_entries,
                                     object_sync_timeout=self.object_sync_timeout,
                                     )
                     for i in xrange(num_workers)]
        for process in processes:
            process.daemon = True
            process.start()

        self.wait_until_ready()

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
        retries = {}
        for i in xrange(num_items):
            result, item = resultQueue.get()
            shard_num, retries = item
            if result == worker.RESULT_SUCCESS:
                log.debug('synced item %r successfully', item)
                self.complete_item(shard_num, retries)
            else:
                log.error('error syncing shard %d', shard_num)
                retries.append(shard_num)

            log.info('%d/%d items processed', i + 1, num_items)
        if retries:
            log.error('Encountered errors syncing these %d shards: %r',
                      len(retries), retries)


class IncrementalSyncer(Syncer):

    def get_worker_bound(self, shard_num):
        bound = client.get_worker_bound(
            self.dest_conn,
            self.type,
            shard_num)

        marker = bound['marker']
        retries = bound['retries']

        dev_log.debug('oldest marker and time for shard %d are: %r %r',
                      shard_num, marker, bound['oldest_time'])
        dev_log.debug('%d items to retry are: %r', len(retries), retries)

        return marker, retries


    def get_log_entries(self, shard_num, marker):
        try:
            result = client.get_log(self.src_conn, self.type,
                                    marker, self.max_entries,
                                    shard_num)
            last_marker = result['marker']
            log_entries = result['entries']
            if len(log_entries) == self.max_entries:
                log.warn('shard %d log has fallen behind - log length >= %d',
                         shard_num, self.max_entries)
        except NotFound:
            # no entries past this marker yet, but we my have retries
            last_marker = ' '
            log_entries = []
        return last_marker, log_entries

    def prepare(self):
        self.init_num_shards()

        self.shard_info = {}
        self.shard_work = {}
        for shard_num in xrange(self.num_shards):
            marker, retries = self.get_worker_bound(shard_num)
            last_marker, log_entries = self.get_log_entries(shard_num, marker)
            self.shard_work[shard_num] = log_entries, retries
            self.shard_info[shard_num] = last_marker

        self.prepared_at = time.time()

    def generate_work(self):
        return self.shard_work.iteritems()


class MetaSyncerInc(IncrementalSyncer):

    def __init__(self, *args, **kwargs):
        super(MetaSyncerInc, self).__init__(*args, **kwargs)
        self.worker_cls = worker.MetadataWorkerIncremental
        self.type = 'metadata'


class DataSyncerInc(IncrementalSyncer):

    def __init__(self, *args, **kwargs):
        super(DataSyncerInc, self).__init__(*args, **kwargs)
        self.worker_cls = worker.DataWorkerIncremental
        self.type = 'data'
        self.rgw_data_log_window = kwargs.get('rgw_data_log_window', 30)

    def wait_until_ready(self):
        log.info('waiting to make sure bucket log is consistent')
        while time.time() < self.prepared_at + self.rgw_data_log_window:
            time.sleep(1)


class DataSyncerFull(Syncer):

    def __init__(self, *args, **kwargs):
        super(DataSyncerFull, self).__init__(*args, **kwargs)
        self.worker_cls = worker.DataWorkerFull
        self.type = 'data'
        self.rgw_data_log_window = kwargs.get('rgw_data_log_window', 30)

    def prepare(self):
        log.info('preparing to do a full data sync')
        self.init_num_shards()

        # save data log markers for each shard
        self.shard_info = {}
        for shard in xrange(self.num_shards):
            info = client.get_log_info(self.src_conn, 'data', shard)
            # setting an empty marker returns an error
            if info['marker']:
                self.shard_info[shard] = info['marker']
            else:
                self.shard_info[shard] = ' '

        # get list of buckets after getting any markers to avoid skipping
        # entries added before we got the marker info
        log.debug('getting bucket list')
        buckets = client.get_bucket_list(self.src_conn)

        self.prepared_at = time.time()

        self.buckets_by_shard = {}
        for bucket in buckets:
            shard = self.shard_num_for_key(bucket)
            self.buckets_by_shard.setdefault(shard, [])
            self.buckets_by_shard[shard].append(bucket)

    def generate_work(self):
        return self.buckets_by_shard.iteritems()

    def wait_until_ready(self):
        log.info('waiting to make sure bucket log is consistent')
        while time.time() < self.prepared_at + self.rgw_data_log_window:
            time.sleep(1)


class MetaSyncerFull(Syncer):
    def __init__(self, *args, **kwargs):
        super(MetaSyncerFull, self).__init__(*args, **kwargs)
        self.worker_cls = worker.MetadataWorkerFull
        self.type = 'metadata'

    def prepare(self):
        try:
            self.sections = client.get_metadata_sections(self.src_conn)
        except HttpError as e:
            log.error('Error listing metadata sections: %s', e)
            raise

        # grab the lastest shard markers and timestamps before we sync
        self.shard_info = {}
        self.init_num_shards()
        for shard_num in xrange(self.num_shards):
            info = client.get_log_info(self.src_conn, 'metadata', shard_num)
            # setting an empty marker returns an error
            if info['marker']:
                self.shard_info[shard_num] = info['marker']
            else:
                self.shard_info[shard_num] = ' '

        self.metadata_by_shard = {}
        for section in self.sections:
            try:
                for key in client.list_metadata_keys(self.src_conn, section):
                    shard = self.shard_num_for_key(section + ':' + key)
                    self.metadata_by_shard.setdefault(shard, [])
                    self.metadata_by_shard[shard].append((section, key))
            except NotFound:
                # no keys of this type exist
                continue
            except HttpError as e:
                log.error('Error listing metadata for section %s: %s',
                          section, e)
                raise

    def generate_work(self):
        return self.metadata_by_shard.iteritems()
