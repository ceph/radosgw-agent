
import boto
import datetime
import json
import logging
import multiprocessing
import random
#import requests
import string
import sys
import threading
import time

import boto.s3.acl
import boto.s3.connection

from boto.connection import AWSAuthConnection
from operator import attrgetter
from RGW_REST_factory import RGW_REST_factory

log_lock_time = 60 #seconds

#debug_commands = True
debug_commands = False
#deep_compare = True
deep_compare = False
source_zone = 'rgw-n1-z1'

class REST_data_syncer:
    logging.basicConfig(filename='boto_data_syncer.log', level=logging.DEBUG)
    log = logging.getLogger(__name__)

    # empty constructor
    def __init__(self):
        pass

    def sync_all_buckets(self, source_access_key, source_secret_key, source_host, 
                         source_zone,
                         dest_access_key, dest_secret_key, dest_host, dest_zone,
                         num_workers, log_lock_time):

    
        # construct the two connection objects
        self.source_conn = boto.s3.connection.S3Connection(
            aws_access_key_id = source_access_key,
            aws_secret_access_key = source_secret_key,
            is_secure=False,
            host = source_host,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            debug=2
        )

        rest_factory = RGW_REST_factory()

        # get the list of  changed buckets
        (retVal, out) = rest_factory.rest_call(self.source_conn, 
                            ['log', 'list', 'type=data'], {"type":"data"})

        if 200 != retVal:
            print 'log list in sync_all_buckets() failed, code: ', retVal
        elif debug_commands:
            print 'log list in sync_all_buckets() returned code: ', retVal

        numObjects = out()['num_objects']
        print 'We have ', numObjects, ' buckets to check'

        # create the work and results Queue
        workQueue = multiprocessing.Queue()
        resultQueue = multiprocessing.Queue()

        # list of processes that will sync the buckets
        processes = []
        workerID = 0

        # create the worker processes
        for i in xrange(num_workers):
            process = \
                REST_data_syncer_worker(workerID, workQueue, resultQueue, \
                    log_lock_time, source_access_key, source_secret_key, \
                    source_host, source_zone, dest_access_key, \
                    dest_secret_key, dest_host, dest_zone)
            process.start()
            processes.append(process)
            workerID += 1

        # enqueue the buckets to be synced
        for i in xrange(numObjects):
            workQueue.put(i)

        # add a poison pill for each worker 
        for i in xrange(num_workers):
            workQueue.put(None)

        # pull the results out as they are produced
        for i in xrange(numObjects):
            processID, shard_num, retVal = resultQueue.get()
            if 200 != retVal:
                print 'shard ', shard_num, ' process ', processID, \
                      ' exited with status ', retVal
            elif debug_commands:
                print 'shard ', shard_num, ' process ', processID, \
                      ' exited with status ', retVal

            print 'shard ', shard_num, ' process ', processID, \
            ' exited with status ', retVal

class REST_data_syncer_worker(multiprocessing.Process):
    """data sync worker to run in its own process"""

    source_conn = None
    dest_conn = None
    source_zone = None
    dest_zone = None
    local_lock_id = None
    rest_factory = None
    processID = None
    processName = None
    work_queue = None
    result_queue = None
    relock_log = True
    relock_timer = None
    log_lock_time = None

    # sleep the prescribed amount of time and then set a bool to true.
    def flip_log_lock(self):
        self.relock_log = True

    def __init__(self, processID, work_queue, result_queue, log_lock_time,
                 source_access_key, source_secret_key, source_host, source_zone,
                 dest_access_key, dest_secret_key, dest_host, dest_zone):

        multiprocessing.Process.__init__(self)
        self.source_zone = source_zone
        self.dest_zone = dest_zone
        self.processID = processID
        self.processName= 'process-' + str(self.processID)
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.log_lock_time = log_lock_time

        # generates an N character random string from letters and digits 16 digits for now
        self.local_lock_id = \
          ''.join(random.choice(string.ascii_letters + string.digits) for x in range(16))
    
        # construct the two connection objects
        self.source_conn = boto.s3.connection.S3Connection(
            aws_access_key_id = source_access_key,
            aws_secret_access_key = source_secret_key,
            is_secure=False,
            host = source_host,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            debug=2
        )

        self.dest_conn = boto.s3.connection.S3Connection(
          aws_access_key_id = dest_access_key,
          aws_secret_access_key = dest_secret_key,
          is_secure=False,
          host = dest_host,
          calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

        self.rest_factory = RGW_REST_factory()

    # we explicitly specify the connection to use for the locking here  
    # in case we need to lock a non-master log file
    def acquire_log_lock(self, conn, lock_id, zone_id, bucket_num):

        if debug_commands:
            print bucket_num, ' acquiring lock'

        (retVal, out) = self.rest_factory.rest_call(conn, ['log', 'lock'], 
            {"type":"data", "id":bucket_num, "length":log_lock_time, \
             'zone-id':zone_id, 'locker-id':lock_id})

        if 200 != retVal:
            print 'acquire_log_lock for bucket ', bucket_num, \
                  ' in zone: ', zone_id, \
                  ' failed, returned http code: ', retVal
            # clear this flag for the next pass
            self.relock_log = False
            return retVal

        elif debug_commands:
            print 'acquire_log_lock returned: ', retVal


        # twiddle the boolean flag to false
        self.relock_log = False

        # then start the timer to twiddle it back to true
        self.relock_timer = threading.Timer(0.85 * self.log_lock_time, \
                                            self.flip_log_lock) 
        self.relock_timer.start()

        return retVal


    # we explicitly specify the connection to use for the locking here  
    # in case we need to lock a non-master log file
    def release_log_lock(self, conn, lock_id, zone_id, bucket_num):
        (retVal, out) = self.rest_factory.rest_call(self.source_conn, 
                                ['log', 'unlock'], {
                                'type':'data', 'id':bucket_num, \
                                'locker-id':lock_id, 'zone-id':zone_id})

        if 200 != retVal:
            print 'data log unlock for zone: ', zone_id, \
                  ' failed, returned http code: ', retVal
        elif debug_commands:
            print 'data log unlock for zone: ', zone_id, ' returned: ', retVal

        return retVal


    # TODO actually use the markers
    def set_datalog_work_bound(self, bucket_num, time_to_use):
        (ret, out) = self.rest_factory.rest_call(self.source_conn, 
                               ['replica_log', 'set', 'work_bound'], 
                      {"id":bucket_num, "type":"data", "marker":"FIIK", }) 

        if 200 != ret:
            print 'data list failed, returned http code: ', ret
        elif debug_commands:
            print 'data list returned: ', ret

    # get the updates for this bucket and sync the data across
    def sync_bucket(self, shard, bucket_name):
        retVal = 200
        # There is not an explicit bucket-index log lock. This is coverred
        # by the lock on the datalog for this shard

        just_the_bucket = bucket_name.split(':')[0]
        print 'just the bucket: ', just_the_bucket
        # get the bilog for this bucket
        (retVal, out) = self.rest_factory.rest_call(self.source_conn, 
                           ['log', 'list', 'type=bucket-index'], 
                          #{"bucket":bucket_name, 'marker':dummy_marker }) 
                          {"bucket":bucket_name, 'bucket-instance':bucket_name}) 
                          #{"rgwx-bucket-instance":bucket_name}) 
                          #{"bucket":just_the_bucket}) 

        if 200 != retVal:
            print 'get bucket-index for bucket ', bucket_name, \
                  ' failed, returned http code: ', retVal
            return retVal
        elif debug_commands:
            print 'get bucket-index for bucket ', bucket_name, \
                  ' returned http code: ', retVal

        bucket_events = out()
  
        print 'bilog for bucket ', bucket_name, ' has ', \
              len(bucket_events), ' entries'

        # first, make sure the events are sorted in index_ver order 
        sorted_events = sorted(bucket_events, key=lambda entry: entry['index_ver']) 
                          #reverse=True)

        for event in sorted_events:
            #make sure we still have the lock
            if self.relock_log:
                retVal = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, \
                                           self.source_zone, data_log_shard)
                if 200 != retVal:
                    print 'error acquiring lock for bucket ', bucket, \
                          ' lock_id: ', self.local_lock_id, \
                          ' in zone ', self.source_zone, \
                          ' in process_entries_for_data_log_shard(). ' \
                          ' Returned http code ', retVal
                    # log unlocking and adding the return value to the result queue
                    # will be handled by the calling function
                    return retVal

            if event['state'] == 'complete':
                print '   applying: ', event

                if event['op'] == 'write':
                    print 'copying object ', bucket_name + '/' + event['object']
                    # sync this operation from source to destination
                    # issue this against the destination rgw, since the 
                    # operation is implemented as a 'pull' of the object 
                    #
                    # TODO put real values in for rgwx-client-id and rgwx-op-od
                    (retVal, out) = self.rest_factory.rest_call(self.dest_conn, 
                               ['object', 'add', bucket_name + '/' + event['object']], 
                      #{"bucket":bucket_name, 'marker':dummy_marker }) 
                              {"rgwx-source-zone":self.source_zone, 
                               "rgwx-client-id":'joe bucks awesome client',
                               "rgwx-op-od":"42"}) 
                elif event['op'] == 'del':
                    print 'deleting object ', bucket_name + '/' + event['object']
                    # delete this object from the destination
                    (retVal, out) = self.rest_factory.rest_call(self.dest_conn, 
                               ['object', 'rm', bucket_name + '/' + event['object']], )
                      #{"bucket":bucket_name, 'marker':dummy_marker }) 
                              #{"rgwx-source-zone":source_zone}) 

                else:
                    print 'no idea what op this is: ', event['op']
                    retVal = 500

            if retVal < 200 or retVal > 299: # all 200 - 299 codes are success
                print 'sync of object ', event['object'], \
                      ' failed, returned http code: ', retVal, \
                        '. Bailing'
                return retVal
            elif debug_commands:
                print 'copy of object ', event['object'], \
                        ' returned http code: ', retVal

        return retVal

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

    def process_entries_for_data_log_shard(self, data_log_shard, entries):
                     
        retVal = 200

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
                retVal = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, \
                                           self.source_zone, data_log_shard)
                if 200 != retVal:
                    print 'error acquiring lock for bucket ', bucket, \
                          ' lock_id: ', self.local_lock_id, \
                          ' in zone ', self.source_zone, \
                          ' in process_entries_for_data_log_shard(). ' \
                          ' Returned http code ', retVal
                    # log unlocking and adding the return value to the result queue
                    # will be handled by the calling function
                    return retVal
  

                retVal = self.sync_bucket(data_log_shard, bucket_name)

                if 200 != retVal:
                    print 'sync_bucket() failed for bucket ', bucket_name, \
                          ', returned http code: ', retVal

                    # if there is an error, release the log lock and bail
                    retVal = self.release_log_lock(self.source_conn, \
                                              self.local_lock_id, \
                                              self.source_zone, data_log_shard)
                    return retVal
                elif debug_commands:
                    print 'sync_bucket() for bucket ', bucket_name, \
                          ' returned http code: ', retVal

        # TODO trim the log and then unlock it
        # trim the log for this bucket now that its objects are synced
        (retVal, out) = self.rest_factory.rest_call(self.source_conn, 
                  ['log', 'trim', 'id=' + str(data_log_shard)], 
                  {"id":data_log_shard, "type":"data", "start-time":really_old_time, 
                   "end-time":sync_start_time})

        if 200 != retVal:
            print 'data log trim for shard ', shard, ' returned http code ', retVal
            # normally we would unlock and return a avlue here, 
            # but since that's going to happen next, we effectively just fall through
            # into it

        elif debug_commands:
            print 'data log trim shard ',shard, ' returned http code ', retVal

        retVal = self.release_log_lock(self.source_conn, \
                                       self.local_lock_id, \
                                       self.source_zone, data_log_shard)
        return retVal


    def run(self):
        while True:   # keep looping until we break
            data_log_shard = self.work_queue.get()
            if data_log_shard is None:
                if debug_commands:
                    print 'process ', self.processName, ' is done with ',\
                          ' changed data log shard ', data_log_shard, \
                          '. Exiting'
                # debug
                print 'process ', self.processName, ' is done with ',\
                      ' changed data log shard ', data_log_shard, \
                      '. Exiting'
                break

            if debug_commands:
                print data_log_shard, ' is being processed by process ', \
                      self.processName


            # first, lock the data log
            retVal = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, self.source_zone, \
                                           data_log_shard)

            if 200 != retVal:
                print 'acquire_log_lock() failed, returned http code: ', retVal
                self.result_queue.put((self.processID, data_log_shard, retVal))
                continue
            elif debug_commands:
                print 'acquire_log_lock() returned http code: ', retVal

            # get the log for this data log shard
            (retVal, out) = self.rest_factory.rest_call(self.source_conn, 
                                   ['log', 'list', 'id=' + str(data_log_shard)], 
                                   {"type":"data", "id":data_log_shard})
    
            if 200 != retVal:
                print 'data list for shard ', data_log_shard, \
                      ' failed, returned http code: ', retVal
                # we hit an error getting the data to sync. 
                # Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, data_log_shard, retVal))
                continue
            elif debug_commands:
                print 'data list for shard ', data_log_shard, \
                      ' returned: ', retVal
    
            log_entry_list = out()

            print 'data log shard ', data_log_shard, ' has ', \
                  len(log_entry_list), ' entries'

            # filter the entries so that a given entry only shows up once
            # to be synced
            buckets_to_sync = self.sort_and_filter_entries(log_entry_list)

            retVal = self.process_entries_for_data_log_shard(data_log_shard, \
                                                             buckets_to_sync)

            self.result_queue.put((self.processID, data_log_shard, '200'))

        # this should only be encountered via the break at the top of the loop 
        return
