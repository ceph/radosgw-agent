import boto
import datetime
import json
import multiprocessing
import random
import string
import threading

from . import worker
from . import client

log_lock_time = 60 #seconds

debug_commands = False
deep_compare = False

class Syncer:

    def __init__(self, type_):
        self._type = type_

    def sync_all(self, source_access_key, source_secret_key, source_host,
                 source_port, source_zone, dest_access_key, dest_secret_key,
                 dest_host, dest_port, dest_zone, num_workers, log_lock_time):

        self.source_conn = boto.s3.connection.S3Connection(
            aws_access_key_id = source_access_key,
            aws_secret_access_key = source_secret_key,
            is_secure=False,
            host = source_host,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            debug=2
        )

        # get the list of changes
        (retVal, out) = client.request(self.source_conn,
                                       ['log', 'type=%s' % self._type])

        if 200 != retVal:
            print 'log list in sync_all() failed, code: ', retVal
        elif debug_commands:
            print 'log list in sync_all() returned code: ', retVal

        num_shards = out()['num_objects']
        print 'We have ', num_shards, ' shards to check'

        # create the work and results Queue
        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        # list of processes that will sync the shards
        processes = []
        workerID = 0

        # create the worker processes
        if self._type == 'data':
            worker_cls = worker.DataWorker
        else:
            worker_cls = worker.MetadataWorker
        for i in xrange(num_workers):
            process = \
                worker_cls(workerID, workQueue, resultQueue, self._type,
                           log_lock_time, source_access_key, source_secret_key,
                           source_host, source_port, source_zone,
                           dest_access_key, dest_secret_key, dest_host,
                           dest_port, dest_zone)
            process.start()
            processes.append(process)
            workerID += 1

        # enqueue the shards to be synced
        for i in xrange(num_shards):
            workQueue.put(i)

        # add a poison pill for each worker
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        for i in xrange(num_shards):
            processID, shard_num, retVal = resultQueue.get()
            if 200 != retVal:
                print 'shard ', shard_num, ' process ', processID, \
                      ' exited with status ', retVal
            elif debug_commands:
                print 'shard ', shard_num, ' process ', processID, \
                      ' exited with status ', retVal

            print 'shard ', shard_num, ' process ', processID, \
            ' exited with status ', retVal
