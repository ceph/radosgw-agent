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

class Syncer:
    def __init__(self, type_, src, dest, daemon_id):
        self._type = type_
        self.src = src
        self.dest = dest
        self.src_conn = client.connection(src)
        self.dest_conn = client.connection(dest)
        self.daemon_id = daemon_id

    def data_sync_incremental(self, num_workers, log_lock_time, max_entries):
        pass

    def metadata_sync_incremental(self, num_workers, log_lock_time, max_entries):
        try:
            num_shards = client.num_log_shards(self.src_conn, self._type)
        except:
            log.exception('finding number of shards failed')
            raise
        log.debug('We have %d shards to check', num_shards)

        # create the work and results Queue
        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        # create the worker processes
        if self._type == 'data':
            worker_cls = worker.DataWorkerIncremental
        else:
            worker_cls = worker.MetadataWorkerIncremental
        processes = [worker_cls(workQueue,
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

        log.info('Starting incremental sync')
        # enqueue the shards to be synced
        for i in xrange(num_shards):
            workQueue.put(i)

        # add a poison pill for each worker
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        errors = []
        connection_errors = []
        for i in xrange(num_shards):
            # if all processes error out, stop trying to process data
            if len(connection_errors) == len(processes):
                log.error('All {num_workers} incremental sync workers have failed.'
                          ' Ceasing to process shards'.format(num_workers=len(processes)))
                break
            result, shard_num = resultQueue.get()
            if result == worker.RESULT_SUCCESS:
                log.debug('synced shard %d', shard_num)
            else:
                log.error('error on incremental sync of shard %d', shard_num)
                errors.append(shard_num)
            if result == worker.RESULT_CONNECTION_ERROR:
                connection_errors.append(shard_num)

            log.info('%d/%d shards processed', i + 1, num_shards)
        if errors:
            log.error('Encountered  errors syncing these %d shards: %s',
                      len(errors), errors)


    def sync_incremental(self, num_workers, log_lock_time, max_entries):
        if self._type == 'metadata':
            self.metadata_sync_incremental(num_workers, log_lock_time, max_entries)
        elif self._type == 'data':
            self.data_sync_incremental(num_workers, log_lock_time, max_entries)
        else:
            raise Exception('Unknown _type in sync.py: {_type}'.format(_type=self._type))

    def data_sync_full(self, num_workers, log_lock_time):
        # TODO we need some sort of a lock here to make sure that only
        # one client is getting a list of buckets to sync so that it's 
        # consistent.

        num_data_shards = client.num_log_shards(self.src_conn, 'data')
        log.debug('There are {ns} data log shards'.format(ns=num_data_shards))

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

        # create the work and results Queue
        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        # create the worker processes
        if self._type == 'data':
            worker_cls = worker.DataWorkerFull
        else:
            worker_cls = worker.MetadataWorkerFull

        processes = [worker_cls(workQueue, resultQueue, log_lock_time, self.src,
                                self.dest, daemon_id=self.daemon_id) for i in xrange(num_workers)]
        for process in processes:
            process.daemon = True
            process.start()

        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        log.info('Starting full data sync at %s', start_time)

        # enqueue the shards to be synced. 
        # the shards simply tell the worker tasks which data replica log shard to check
        for shard_num in xrange(num_data_shards):
            workQueue.put(shard_num)

        # add a poison pill for each worker
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        errors = []
        connection_errors = []
        for i in xrange(num_data_shards):
            if len(connection_errors) == len(processes):
                log.error('All {num_workers} full sync workers have failed.'
                          ' Ceasing to process shards'.format(num_workers=len(processes)))
                break
            log.info('%d/%d shards synced, ', i, num_data_shards)
            result, shard_num = resultQueue.get()
            if result != worker.RESULT_SUCCESS:
                log.error('error syncing shard %d', shard_num)
                errors.append((shard_num))
            else:
                log.debug('synced shard %s', shard_num)
            if result == worker.RESULT_CONNECTION_ERROR:
                connection_errors.append(shard_num)

        for process in processes:
            process.join()
        if errors:
            log.error('Encountered  errors syncing these %d entries: %s',
                      len(errors), errors)

    def metadata_sync_full(self, num_workers, log_lock_time):
        try:
            sections = client.get_metadata_sections(self.src_conn)
        except client.HttpError as e:
            log.error('Error listing metadata sections: %s', e)
            raise

        # grab the lastest shard markers and timestamps before we sync
        shard_info = []
        num_shards = client.num_log_shards(self.src_conn, 'metadata')
        for shard_num in xrange(num_shards):
            info = client.get_log_info(self.src_conn, 'metadata', shard_num)
            # setting an empty marker returns an error
            if info['marker']:
                shard_info.append((shard_num, info['marker'],
                                   info['last_update']))

        meta_keys = []
        for section in sections:
            try:
                meta_keys += [(section, key) for key in
                              client.list_metadata_keys(self.src_conn, section)]
            except client.NotFound:
                # no keys of this type exist
                continue
            except client.HttpError as e:
                log.error('Error listing metadata for section %s: %s',
                          section, e)
                raise

        # create the work and results Queue
        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        # create the worker processes
        if self._type == 'data':
            worker_cls = worker.DataWorkerFull
        else:
            worker_cls = worker.MetadataWorkerFull
        processes = [worker_cls(workQueue, resultQueue, log_lock_time, self.src,
                                self.dest) for i in xrange(num_workers)]
        for process in processes:
            process.daemon = True
            process.start()

        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        log.info('Starting full sync at %s', start_time)

        # enqueue the shards to be synced
        for meta in meta_keys:
            workQueue.put(meta)

        # add a poison pill for each worker
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        errors = []
        connection_errors = []
        for i in xrange(len(meta_keys)):
            # if all processes error out, stop trying to process data
            if len(connection_errors) == len(processes):
                log.error('All {num_workers} full sync workers have failed.'
                          ' Ceasing to process shards'.format(num_workers=len(processes)))
                break

            log.info('%d/%d items synced', i, len(meta_keys))
            result, section, name = resultQueue.get()
            if result != worker.RESULT_SUCCESS:
                log.error('error on full sync of %s %r', section, name)
                errors.append((section, name))
            else:
                log.debug('synced %s %r', section, name)
            if result == worker.RESULT_CONNECTION_ERROR:
                connection_errors.append(shard_num)
        for process in processes:
            process.join()
        if errors:
            log.error('Encountered  errors syncing these %d entries: %s',
                      len(errors), errors)
        else:
            for shard_num, marker, timestamp in shard_info:
                client.set_worker_bound(self.dest_conn, 'metadata', shard_num,
                                        marker, timestamp, self.daemon_id)
                client.del_worker_bound(self.dest_conn, 'metadata', shard_num,
                                        self.daemon_id)

    def sync_full(self, num_workers, log_lock_time):
        if self._type == 'metadata':
            self.metadata_sync_full(num_workers, log_lock_time)
        elif self._type == 'data':
            self.data_sync_full(num_workers, log_lock_time)
        else:
            raise Exception('Unknown _type in sync.py: {_type}'.format(_type=self._type))

