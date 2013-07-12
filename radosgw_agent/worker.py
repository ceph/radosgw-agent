import boto
import datetime
import json
import logging
import multiprocessing
import os
import threading
import time

import radosgw_agent.client

log = logging.getLogger(__name__)

class Worker(multiprocessing.Process):
    """sync worker to run in its own process"""

    # sleep the prescribed amount of time and then set a bool to true.
    def flip_log_lock(self):
        self.relock_log = True

    def __init__(self, processID, work_queue, result_queue, log_lock_time,
                 source_access_key, source_secret_key, source_host, source_port,
                 source_zone, dest_access_key, dest_secret_key,
                 dest_host, dest_port, dest_zone, pending_timeout=10):

        super(Worker, self).__init__()
        self.source_zone = source_zone
        self.dest_zone = dest_zone
        self.processID = processID
        self.processName = 'process-' + str(self.processID)
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.log_lock_time = log_lock_time

        self.local_lock_id = os.getpid() # plus a few random chars

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

    # we explicitly specify the connection to use for the locking here
    # in case we need to lock a non-master log file
    def acquire_log_lock(self, conn, lock_id, zone_id, shard_num):

        log.debug('acquiring lock on shard %d', shard_num)

        try:
            client.request(conn, ['log', 'lock'],
                           {'type': self._type, 'id': shard_num,
                            'length': self.log_lock_time,
                            'zone-id': zone_id, 'locker-id': lock_id})
        except client.HttpError as e:
            log.warn('acquire_log_lock for shard {0} in '
                     'zone {1} failed with status {2}',
                     shard_num, zone_id, e.code)
            # clear this flag for the next pass
            self.relock_log = False
            return e.code

        log.debug('acquire_log_lock returned: ', ret)

        # twiddle the boolean flag to false
        self.relock_log = False

        # then start the timer to twiddle it back to true
        self.relock_timer = threading.Timer(0.85 * self.log_lock_time, \
                                            self.flip_log_lock)
        self.relock_timer.start()

        return ret


    # we explicitly specify the connection to use for the locking here
    # in case we need to lock a non-master log file
    def release_log_lock(self, conn, lock_id, zone_id, shard_num):
        try:
            client.request(self.source_conn,
                           ['log', 'unlock'], {
                               'type': self._type, 'id': shard_num,
                               'locker-id': lock_id,
                               'zone-id': zone_id})

        except client.HttpError as e:
            log.warn('data log unlock for zone %s failed, '
                     'returned http code %d', zone_id, ret)
        log.debug('data log unlock for zone %s returned %d',
                  zone_id, ret)

        return ret


class DataWorker(Worker):

    def __init__(self, *args, **kwargs):
        super(DataWorker, self).__init__(*args, **kwargs)
        self._type = 'data'

    # TODO actually use the markers
    def set_datalog_work_bound(self, shard_num, time_to_use):
        (ret, out) = client.request(self.source_conn,
                               ['replica_log', 'set', 'work_bound'],
                      {"id": shard_num, 'type': self._type, 'marker': 'FIIK', }) 

        if 200 != ret:
            print 'data list failed, returned http code: ', ret

    # get the updates for this bucket and sync the data across
    def sync_bucket(self, shard_num, bucket_name):
        ret = 200
        # There is not an explicit bucket-index log lock. This is coverred
        # by the lock on the datalog for this shard

        just_the_bucket = bucket_name.split(':')[0]
        print 'just the bucket: ', just_the_bucket
        # get the bilog for this bucket
        (ret, out) = client.request(self.source_conn,
                           ['log', 'list', 'type=bucket-index'],
                          #{"bucket":bucket_name, 'marker':dummy_marker })
                          {"bucket":bucket_name, 'bucket-instance':bucket_name}) 
                          #{"rgwx-bucket-instance":bucket_name})
                          #{"bucket":just_the_bucket})

        if 200 != ret:
            print 'get bucket-index for bucket ', bucket_name, \
                  ' failed, returned http code: ', ret
            return ret

        bucket_events = out()
        print 'bilog for bucket ', bucket_name, ' has ', \
              len(bucket_events), ' entries'

        # first, make sure the events are sorted in index_ver order
        sorted_events = sorted(bucket_events, key=lambda entry: entry['index_ver']) 
                          #reverse=True)

        for event in sorted_events:
            #make sure we still have the lock
            if self.relock_log:
                ret = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, \
                                           self.source_zone, shard_num)
                if 200 != ret:
                    print 'error acquiring lock for shard ', shard_num, \
                          ' lock_id: ', self.local_lock_id, \
                          ' in zone ', self.source_zone, \
                          ' in process_entries_for_data_log_shard(). ' \
                          ' Returned http code ', ret
                    # log unlocking and adding the return value to the result queue
                    # will be handled by the calling function
                    return ret

            if event['state'] == 'complete':
                print '   applying: ', event

                if event['op'] == 'write':
                    print 'copying object ', bucket_name + '/' + event['object']
                    # sync this operation from source to destination
                    # issue this against the destination rgw, since the
                    # operation is implemented as a 'pull' of the object
                    #
                    # TODO put real values in for rgwx-client-id and rgwx-op-od
                    (ret, out) = client.request(self.dest_conn,
                               ['object', 'add', bucket_name + '/' + event['object']], 
                      #{"bucket":bucket_name, 'marker':dummy_marker })
                              {"rgwx-source-zone":self.source_zone,
                               "rgwx-client-id":'joe bucks awesome client',
                               "rgwx-op-od":"42"})
                elif event['op'] == 'del':
                    print 'deleting object ', bucket_name + '/' + event['object']
                    # delete this object from the destination
                    (ret, out) = client.request(self.dest_conn,
                               ['object', 'rm', bucket_name + '/' + event['object']], )
                      #{"bucket":bucket_name, 'marker':dummy_marker })
                              #{"rgwx-source-zone":source_zone})

                else:
                    print 'no idea what op this is: ', event['op']
                    ret = 500

            if ret < 200 or ret > 299: # all 200 - 299 codes are success
                print 'sync of object ', event['object'], \
                      ' failed, returned http code: ', ret, \
                        '. Bailing'
                return ret

        return ret

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

    def process_entries_for_data_log_shard(self, shard_num, entries):
        ret = 200

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
                ret = self.acquire_log_lock(self.source_conn, \
                                           self.local_lock_id, \
                                           self.source_zone, shard_num)
                if 200 != ret:
                    print 'error acquiring lock for shard ', shard_num, \
                          ' lock_id: ', self.local_lock_id, \
                          ' in zone ', self.source_zone, \
                          ' in process_entries_for_data_log_shard(). ' \
                          ' Returned http code ', ret
                    # log unlocking and adding the return value to the
                    # result queue will be handled by the calling
                    # function
                    return ret


                ret = self.sync_bucket(shard_num, bucket_name)

                if 200 != ret:
                    print 'sync_bucket() failed for bucket ', bucket_name, \
                          ', returned http code: ', ret

                    # if there is an error, release the log lock and bail
                    ret = self.release_log_lock(self.source_conn, \
                                              self.local_lock_id, \
                                              self.source_zone, shard_num)
                    return ret

        # TODO trim the log and then unlock it
        # trim the log for this bucket now that its objects are synced
        (ret, out) = client.request(self.source_conn,
                  ['log', 'trim', 'id=' + str(shard_num)],
                  {'id': shard_num, 'type': 'data',
                   'start-time': really_old_time, 'end-time': sync_start_time})

        if 200 != ret:
            print 'data log trim for shard ', shard_num, ' returned http code ', ret
            # normally we would unlock and return a avlue here,
            # but since that's going to happen next, we effectively just fall through
            # into it

        ret = self.release_log_lock(self.source_conn, \
                                       self.local_lock_id, \
                                       self.source_zone, shard_num)
        return ret


    def run(self):
        while True:   # keep looping until we break
            shard_num = self.work_queue.get()
            if shard_num is None:
                log.debug('process {0} is done with all available shards',
                          self.processName)
                break

            log.debug('{0} is processing shard {1}',
                      self.processName, shard_num)

            # first, lock the data log
            ret = self.acquire_log_lock(self.source_conn,
                                           self.local_lock_id,
                                           self.source_zone,
                                           shard_num)

            if 200 != ret:
                print 'acquire_log_lock() failed, returned http code: ', ret
                self.result_queue.put((self.processID, shard_num, ret))
                continue

            # get the log for this data log shard
            (ret, out) = client.request(self.source_conn,
                                   ['log', 'list', 'id=' + str(shard_num)],
                                   {'type': 'data', 'id': shard_num})

            if 200 != ret:
                print 'data list for shard ', shard_num, \
                      ' failed, returned http code: ', ret
                # we hit an error getting the data to sync.
                # Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, ret))
                continue
            log.debug('data list for shard {0} returned {1}', shard_num, ret)

            log_entry_list = out()

            log.debug('shard {0} has {1} entries',
                      shard_num, len(log_entry_list))

            # filter the entries so that a given entry only shows up once
            # to be synced
            buckets_to_sync = self.sort_and_filter_entries(log_entry_list)

            ret = self.process_entries_for_data_log_shard(shard_num, \
                                                             buckets_to_sync)

            self.result_queue.put((self.processID, shard_num, '200'))


class MetadataEntry(object):
    def __init__(self, entry):
        self.data = entry
        self.name = entry['name']
        self.tag = entry['data']['write_version']['tag']
        self.version = entry['data']['write_version']['version']
        self.status = entry['data']['status']['status']
        assert self.status in ['write', 'remove', 'complete']
        self.section = entry['section']
        assert self.section in ['user', 'bucket', 'bucket.instance']
        self.waiting = []

    def is_user(self):
        return self.section == 'user'

    def __str__(self):
        return str(self.data)

class MetadataWorker(Worker):

    def __init__(self, *args, **kwargs):
        super(MetadataWorker, self).__init__(*args, **kwargs)
        self._type = 'metadata'

    # this is used to check either a user or a bucket creation / update, since
    # both use the same APIs
    def sync_complete_entry(self, entry_name, md_type, tag=None):
        ret = 200

        # If the tag in the metadata log on the source_data does not match the
        # current tag for the entry, skip this entry. We assume this is caused
        # by this entry being for a entry that has been deleted.
        if tag != None and tag != source_data['ver']['tag']:
            print 'log tag ', tag, ' != current entry tag ', \
                  source_data['ver']['tag'], \
                  '. Skipping this entry / tag pair (', entry_name, \
                  ' / ', tag, ')'
            return 200

        # if the user does not exist on the non-master side, then a 404
        # return value is appropriate
        ret, dest_data = self.pull_metadata_for_entry(self.dest_conn, \
                                                         entry_name, md_type)
        if 200 != ret and 404 != ret:
            print 'pull from dest failed for entry: ', entry_name
            return ret

        # if this user does not exist on the destination side, add it
        if ret == 404 and dest_data['Code'] == 'NoSuchKey':
            print 'entry: ', entry_name, ' missing from the remote side. ', \
                  'Adding it'
            ret = self.add_entry_to_remote(entry_name, md_type)

        else: # if the user exists on the remote side,
              # ensure they're the same version
            dest_ver = dest_data['ver']['ver']
            source_ver = source_data['ver']['ver']

            if dest_ver != source_ver:
                print 'entry: ', entry_name, ' local_ver: ', source_ver, \
                      ' != dest_ver: ', dest_ver, ' UPDATING'
                ret = self.update_remote_entry(entry_name, md_type)
            elif debug_commands:
                print 'entry: ', entry_name, ' local_ver: ', source_ver, \
                      ' == dest_ver: ', dest_ver

        return ret

    def check_pending(self, pending):
        for key, entry in pending:
            section, name = key
            log.debug('Checking pending entry %s', entry)
                self.client.request(self.dest_conn,
                                    ['metadata', 'metaput', type_])

    def sync_meta(self, section, name):
            ret, metadata = client.get_metadata(self.source_conn, section,
                                                name)
            if ret == 404:
                ret, _ = client.delete_metadata(self.source_conn, section,
                                                name)
            elif ret == 200:
                ret, _ = client.update_metadata(self.source_conn, section,
                                                name, metadata)
            else:
                log.error('error getting metadata for %s: %d %s',
                          entry, ret, metadata)

    def get_complete_entries(self, entries):
        """
        Go through all the entries, looking for the start and end of
        operations on a given piece of metadata.

        Returns a tuple of completed entries and pending entries,
        each of which is a list of MetadataEntry objects
        """
        completed = {}
        pending = {}
        while entries:
            entry = entries.pop(0)
            key = (entry.section, entry.name)

            # this is a newer operation, so ignore the old one
            if key in completed:
                del completed[key]

            if entry.status == 'complete':
                assert key in pending
                if pending[key].version == entry.version and \
                       pending[key].tag == entry.tag:
                    completed[key] = pending[key]
                    # put the subsequent entries at the beginning of
                    # the list to be processed so they're ordered
                    # correctly wrt any entries for the same metadata
                    # we haven't looked at yet
                    entries = pending[key].waiting + entries
                    del pending[key]
                    completed[key].waiting = []
                    continue
                # fall through to the next case if versions differ - this
                # completion isn't the one we're looking for yet

            # if we haven't found the 'complete' entry for the first
            # pending operation on this (section, name) pair, hold off on
            # processing the next entry
            if key in pending:
                pending[key].waiting.append(entry)
                continue

            # wait for the 'complete' entry corresponding to this entry
            pending[key] = entry

        return completed.values(), pending.values()

    def process_entries(self, entries):
        complete, pending = self.get_complete_entries(entries)

        if pending:
            time.sleep(self.pending_timeout)

        for _, entry in complete + pending:
            self.sync_metadata(entry.section, entry.name)

        return ret

    def run(self):
        while True:
            shard_num = self.work_queue.get()
            if shard_num is None:
                log.info('process %s is done. Exiting', self.processName)
                break

            log.info('%s is processing shard number %d',
                     self.processName, shard_num)

            # we need this due to a bug in rgw that isn't auto-filling in
            # sensible defaults when start-time is omitted
            really_old_time = "2010-10-10 12:12:00"

            # NOTE rgw deals in UTC time. Make sure you adjust your
            # calls accordingly
            sync_start_time = \
              datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # first, lock the log
            ret = self.acquire_log_lock(self.source_conn,
                                           self.local_lock_id, self.source_zone,
                                           shard_num)

            if 200 != ret:
                print 'acquire_log_lock() failed, returned http code: ', ret
                self.result_queue.put((self.processID, shard_num, ret))
                continue

            # get the log for this shard of the metadata log
            (ret, out) = self.client.request(self.source_conn,
                                   ['log', 'list', 'id=' + str(shard_num)],
                                   {"type":"metadata", "id":shard_num})

            if 200 != ret:
                print 'metadata list failed, returned http code: ', ret
                # we hit an error getting the data to sync.
                # Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, ret))
                continue

            log_entry_list = out()

            print 'shard ', shard_num, ' has ', len(log_entry_list), ' entries'

            try:
                entries = [MetadataEntry(x) for entry in log_entry_list]
            except KeyError:
                log.exception('error reading metadata entry, skipping shard')
                continue

            ret = self.process_entries(entries)

            if 200 != ret:
                print 'process_entries() returned http code ', ret
                # we hit an error processing a user. Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, ret))
                continue

            # trim the log for this shard now that all the users are synched
            # this should only occur if no users threw errors
            (ret, out) = self.client.request(self.source_conn,
                          ['log', 'trim', 'id=' + str(shard_num)],
                          {'id':shard_num, 'type':'metadata',
                          'start-time':really_old_time,
                          'end-time':sync_start_time})

            if 200 != ret:
                print 'log trim returned http code ', ret
                # we hit an error processing a user. Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, ret))
                continue

            # finally, unlock the log
            self.release_log_lock(self.source_conn, self.local_lock_id, \
                                  self.source_zone, shard_num)

            self.result_queue.put((self.processID, shard_num, ret))

            log.info('%s finished processing shard %d',
                     self.processName, shard_num)
