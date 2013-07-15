import boto
import boto.s3.connection
import datetime
import logging
import multiprocessing

from radosgw_agent import worker
from radosgw_agent import client

log = logging.getLogger(__name__)

class Syncer:

    def __init__(self, type_, src, dest, store):
        self._type = type_
        self.src = src
        self.dest = dest
        self.src_conn = boto.s3.connection.S3Connection(
            aws_access_key_id=self.src.access_key,
            aws_secret_access_key=self.src.secret_key,
            is_secure=False,
            host=self.src.host,
            port=self.src.port,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            debug=2,
        )
        self.store = store

    def sync_partial(self, num_workers, log_lock_time):
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
            worker_cls = worker.DataWorkerPartial
        else:
            worker_cls = worker.MetadataWorkerPartial
        processes = [worker_cls(workQueue, resultQueue, log_lock_time, self.src,
                                self.dest) for i in xrange(num_workers)]
        for process in processes:
            process.daemon = True
            process.start()

        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        log.info('Starting partial sync at %s', start_time)

        # enqueue the shards to be synced
        for i in xrange(num_shards):
            workQueue.put((i, self.store.last_shard_sync(i), start_time))

        # add a poison pill for each worker
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        errors = []
        for i in xrange(num_shards):
            log.info('%d/%d shards synced', i, num_shards)
            result, shard_num = resultQueue.get()
            if result == worker.RESULT_SUCCESS:
                self.store.update_shard_timestamp(shard_num, start_time)
                if resultQueue.empty():
                    self.store.save_to_disk()
                log.debug('synced shard %d', shard_num)
            else:
                log.error('error syncing shard %d', shard_num)
                errors.append(shard_num)
        if errors:
            log.error('Encountered  errors syncing these %d shards: %s',
                      len(errors), errors)

    def sync_full(self, num_workers, log_lock_time):
        try:
            sections = client.get_metadata_sections(self.src_conn)
        except client.HttpError as e:
            log.error('Error listing metadata sections: %s', e)
            raise

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
        for i in xrange(len(meta_keys)):
            log.info('%d/%d items synced', i, len(meta_keys))
            result, section, name = resultQueue.get()
            if result != worker.RESULT_SUCCESS:
                log.error('error syncing %s "%s"', section, name)
                errors.append((section, name))
            else:
                log.debug('synced %s "%s"', section, name)
        for process in processes:
            process.join()
        if errors:
            log.error('Encountered  errors syncing these %d entries: %s',
                      len(errors), errors)
        else:
            self.store.update_full_sync_timestamp(start_time)
            self.store.save_to_disk()
