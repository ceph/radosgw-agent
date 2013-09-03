import datetime
import logging
import multiprocessing

from radosgw_agent import worker
from radosgw_agent import client

log = logging.getLogger(__name__)

class Syncer:

    def __init__(self, type_, src, dest, daemon_id):
        self._type = type_
        self.src = src
        self.dest = dest
        self.src_conn = client.connection(src)
        self.dest_conn = client.connection(dest)
        self.daemon_id = daemon_id

    def sync_incremental(self, num_workers, log_lock_time, max_entries):
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

    def sync_full(self, num_workers, log_lock_time):
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
