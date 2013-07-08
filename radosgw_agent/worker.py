import boto
import multiprocessing
import logging

from . import client

log = logging.getLogger(__name__)

class Worker(multiprocessing.Process):
    """sync worker to run in its own process"""

    # sleep the prescribed amount of time and then set a bool to true.
    def flip_log_lock(self):
        self.relock_log = True

    def __init__(self, processID, work_queue, result_queue, log_lock_time,
                 source_access_key, source_secret_key, source_host, source_port,
                 source_zone, dest_access_key, dest_secret_key,
                 dest_host, dest_port, dest_zone):

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

    # we explicitly specify the connection to use for the locking here
    # in case we need to lock a non-master log file
    def acquire_log_lock(self, conn, lock_id, zone_id, shard_num):

        log.debug('acquiring lock on shard %d', shard_num)

        retVal, _ = client.request(conn, ['log', 'lock'],
            {'type': self._type, 'id': object_num, 'length': log_lock_time,
             'zone-id': zone_id, 'locker-id': lock_id})

        if 200 != retVal:
            log.warn('acquire_log_lock for shard {0} in '
                     'zone {1} failed with status {2}',
                     bucket_num, zode_id, retVal)
            # clear this flag for the next pass
            self.relock_log = False
            return retVal

        log.debug('acquire_log_lock returned: ', retVal)

        # twiddle the boolean flag to false
        self.relock_log = False

        # then start the timer to twiddle it back to true
        self.relock_timer = threading.Timer(0.85 * self.log_lock_time, \
                                            self.flip_log_lock)
        self.relock_timer.start()

        return retVal


    # we explicitly specify the connection to use for the locking here
    # in case we need to lock a non-master log file
    def release_log_lock(self, conn, lock_id, zone_id, shard_num):
        retVal, _ = client.request(self.source_conn,
                                   ['log', 'unlock'], {
                                       'type': self._type, 'id': shard_num,
                                       'locker-id': lock_id,
                                       'zone-id': zone_id})

        if 200 != retVal:
            log.warn('data log unlock for zone %s failed, '
                     'returned http code %d', zone_id, retVal)
        log.debug('data log unlock for zone %s returned %d',
                  zode_id, retVal)

        return retVal


class DataWorker(Worker):

    def __init__(*args, **kwargs):
        super(self, DataWorker).__init__(*args, **kwargs)
        self._type = 'data'

    # TODO actually use the markers
    def set_datalog_work_bound(self, shard_num, time_to_use):
        (ret, out) = client.request(self.source_conn,
                               ['replica_log', 'set', 'work_bound'],
                      {"id": shard_num, 'type': self._type, 'marker': 'FIIK', }) 

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
        (retVal, out) = client.request(self.source_conn,
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
                    (retVal, out) = client.request(self.dest_conn,
                               ['object', 'add', bucket_name + '/' + event['object']], 
                      #{"bucket":bucket_name, 'marker':dummy_marker })
                              {"rgwx-source-zone":self.source_zone,
                               "rgwx-client-id":'joe bucks awesome client',
                               "rgwx-op-od":"42"})
                elif event['op'] == 'del':
                    print 'deleting object ', bucket_name + '/' + event['object']
                    # delete this object from the destination
                    (retVal, out) = client.request(self.dest_conn,
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
                    # log unlocking and adding the return value to the
                    # result queue will be handled by the calling
                    # function
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
        (retVal, out) = client.request(self.source_conn,
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
            shard_num = self.work_queue.get()
            if shard_num is None:
                log.debug('process {0} is done with all available shards',
                          self.processName)
                break

            log.debug('{0} is processing shard {1}',
                      self.processName, shard_num)

            # first, lock the data log
            retVal = self.acquire_log_lock(self.source_conn,
                                           self.local_lock_id,
                                           self.source_zone,
                                           shard_num)

            if 200 != retVal:
                print 'acquire_log_lock() failed, returned http code: ', retVal
                self.result_queue.put((self.processID, data_log_shard, retVal))
                continue
            elif debug_commands:
                print 'acquire_log_lock() returned http code: ', retVal

            # get the log for this data log shard
            (retVal, out) = client.request(self.source_conn,
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
            log.debug('data list for shard {0} returned {1}', shard_num, retVal)

            log_entry_list = out()

            log.debug('shard {0} has {1} entries',
                      shard_num, len(log_entry_list))

            # filter the entries so that a given entry only shows up once
            # to be synced
            buckets_to_sync = self.sort_and_filter_entries(log_entry_list)

            retVal = self.process_entries_for_data_log_shard(data_log_shard, \
                                                             buckets_to_sync)

            self.result_queue.put((self.processID, data_log_shard, '200'))


class MetadataWorker(Worker):

    def __init__(*args, **kwargs):
        super(self, MetadataWorker).__init__(*args, **kwargs)
        self._type = 'metadata'

    # we've already done all the logical checks, just delete it at this point
    def delete_remote_entry(self, conn, entry_name, md_type):
        # get current info for this entry
        if md_type == metadata_type.USER:
            retVal, _ = self.client.request(conn,
                                ['user', 'delete', entry_name], {}, admin=True)
        else:
            retVal, _ = self.client.request(conn,
                                ['bucket', 'delete', entry_name], {}, admin=False)

        if 200 != retVal:
            print 'delete entry failed for ', entry_name, '; returned http code: ', retVal

        return retVal


    # copies the curret metadata for a user from the master side to the
    # non-master side
    def add_entry_to_remote(self, entry_name, md_type):

        # get current info for this entry
        if md_type == metadata_type.USER:
            (retVal, src_acct) = self.client.request(self.source_conn,
                                ['metadata', 'metaget', 'user'],
                                {'key':entry_name})
        else:
            (retVal, src_acct) = self.client.request(self.source_conn,
                                ['metadata', 'metaget', 'bucket'],
                                {'key':entry_name})

        if 200 != retVal:
            print 'add_entry_to_remote source side metadata (GET) failed, ',\
                  'code: ', retVal
            return retVal

        # create an empty dict and pull out the name to use as an argument for next call
        args = {}
        if md_type == metadata_type.USER:
            args['key'] = src_acct()['data']['user_id']
        else:
            #args['key'] = src_acct()['data']['bucket_info']['bucket']['name']
            args['key'] = src_acct()['data']['bucket']['name']

        # json encode the data
        outData = json.dumps(src_acct())

        if md_type == metadata_type.USER:
            (retVal, dest_acct) = self.client.request(self.dest_conn,
                                ['metadata', 'metaput', 'user'], args, data=outData)
        elif md_type == metadata_type.BUCKET or md_type == metadata_type.BUCKET_INSTANCE:
            (retVal, dest_acct) = self.client.request(self.dest_conn,
                                ['metadata', 'metaput', 'bucket'], args, data=outData)
        else:
            # invalid metadata type, return an http error code
            print 'invalid metadata_type found in add_entry_to_remote(). value: ', md_type
            return 404

        if 200 != retVal:
            print 'metadata user (PUT) failed, return http code: ', retVal
            print 'body: ', dest_acct()
            return retVal
        elif debug_commands:
            print 'add_entry_to_remote metadata userput() returned: ', retVal


        return retVal


     # for now, just reuse the add_entry code. May need to differentiate in the future
    def update_remote_entry(self, entry, md_type):
        return self.add_entry_to_remote(entry, md_type)


    # use the specified connection as it may be pulling metadata from any rgw
    def pull_metadata_for_entry(self, conn, entry_name, md_type):

        if md_type == metadata_type.USER:
            (retVal, out) = self.client.request(conn,
                                ['metadata', 'metaget', 'user'], {"key":entry_name})
        else:
            (retVal, out) = self.client.request(conn,
                                ['metadata', 'metaget', 'bucket'], {"key":entry_name})

        if 200 != retVal and 404 != retVal:
            print 'pull_metadata_for_entry() metadata user(GET) failed for ', \
                  '{entry} returned {val}'.format(entry=entry_name,val=retVal)
            return retVal, None
        else:
            if debug_commands:
                print 'pull_metadata_for_entry for {entry} returned: ', \
                      '{val}'.format(entry=entry_name,val=retVal)

        return retVal, out()

    # only used for debugging at present
    def manually_diff(self, source_data, dest_data):
        diffkeys1 = [k for k in source_data if source_data[k] != dest_data[k]]
        diffkeys2 = [k for k in dest_data if dest_data[k] != \
                      source_data[k] and not (k in source_data) ]

        return diffkeys1 + diffkeys2

    def manually_diff_individual_user(self, uid):
        retVal = 200 # default to success

        # get user metadata from the source side
        (retVal, source_data) = self.pull_metadata_for_uid(self.source_conn, uid)
        if 200 != retVal:
            return retVal

        # get the metadata for the same uid from the dest side
        (retVal, dest_data) = self.pull_metadata_for_uid(self.dest_conn, uid)
        if 200 != retVal:
            return retVal

        diff_set = self.manually_diff(source_data, dest_data)

        if 0 == len(diff_set):
            print 'deep comparison of uid ', uid, ' passed'
        else:
            for k in diff_set:
                print k, ':', source_data[k], ' != ', dest_data[k]
                retVal = 400 # throw an http-ish error code

        return retVal

    # this is used to delete either a user or a bucket create/destroy, since
    # both use the same APIs
    def delete_meta_entry(self, conn, entry_name, md_type, tag=None):
        retVal = 200

        # this should be a 404, as the entry was deleted from the
        # source (which is why it's in the log as a delete) or a 200
        # (if it was deleted and then the same entry was added in the
        # same logging window)
        retVal, source_data = self.pull_metadata_for_entry(self.source_conn, entry_name, md_type)
        if 200 != retVal and 404 != retVal:
            print 'pull from source failed for entry ', entry_name, '; ', \
                  retVal, ' was returned'
            return retVal

        retVal, dest_data = self.pull_metadata_for_entry(self.dest_conn, \
                                                      entry_name, md_type)
        if 200 != retVal:
            # if the entry does not exist on the non-master zone for
            # some reason, then our work is done here, so return a
            # success on a 404
            if 404 == retVal:
                return 200
            else:
                print 'pull from dest failed for entry ', entry_name, '; ', \
                      retVal, ' was returned'
                return retVal

        # If the tag in the metadata log on the dest_data does not
        # match the current tag for the entry, skip this entry. We
        # assume this is caused by this entry being for a entry that
        # has been deleted.
        if tag != None and tag != dest_data['ver']['tag']:
            print 'log tag ', tag, ' != current entry tag ', \
                  dest_data['ver']['tag'], \
                  '. Skipping this entry / tag pair (', entry_name, " / ", \
                  tag, ")"
            return 200

        retVal = self.delete_remote_entry(conn, entry_name, md_type)
        if 200 != retVal:
            print 'delete_remote_entry() failed for entry ', entry_name, '; ', \
                      retVal, ' was returned'
            return retVal
        elif debug_commands:
            print 'delete_remote_entry() for entry ', entry_name, '; ', \
                      ' returned ', retVal

        return retVal

    # this is used to check either a user or a bucket creation / update, since
    # both use the same APIs
    def check_meta_entry(self, entry_name, md_type, tag=None):
        retVal = 200

        retVal, source_data = \
          self.pull_metadata_for_entry(self.source_conn, entry_name, md_type)

        # user must have been deleted; return success as there's nothing to sync
        if 404 == retVal:
            print 'entry ', entry_name, ' no longer exists on the master. ', \
                  'Skipping it'
            return 200
        if 200 != retVal:
            print 'pull from source failed for entry ', entry_name, '; ', \
                  retVal, ' was returned'
            return retVal
        elif debug_commands:
            print 'in check_meta_entry(), pull_metadata_for_entry() returned ',\
                  retVal, ' for entry: ', entry_name

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
        retVal, dest_data = self.pull_metadata_for_entry(self.dest_conn, \
                                                         entry_name, md_type)
        if 200 != retVal and 404 != retVal:
            print 'pull from dest failed for entry: ', entry_name
            return retVal

        # if this user does not exist on the destination side, add it
        if (retVal == 404 and dest_data['Code'] == 'NoSuchKey'):
            print 'entry: ', entry_name, ' missing from the remote side. ', \
                  'Adding it'
            retVal = self.add_entry_to_remote(entry_name, md_type)

        else: # if the user exists on the remote side,
              # ensure they're the same version
            dest_ver = dest_data['ver']['ver']
            source_ver = source_data['ver']['ver']

            if dest_ver != source_ver:
                print 'entry: ', entry_name, ' local_ver: ', source_ver, \
                      ' != dest_ver: ', dest_ver, ' UPDATING'
                retVal = self.update_remote_entry(entry_name, md_type)
            elif debug_commands:
                print 'entry: ', entry_name, ' local_ver: ', source_ver, \
                      ' == dest_ver: ', dest_ver

        return retVal

    def sort_entries(self, to_sort):
        # sort the data by reverse status (so write comes prior to completed)
        def entry_keys(entry):
            version = entry['data']['write_version']['ver']
            # reverse so that complete comes after write
            reverse_status = 'z' - entry['data']['status']['status'][0]
            return entry['name'], version, reverse_status
        to_sort.sort(key=entry_keys)

    def process_entries(self, entries):
        retVal = 200
        metadata_entries = {}
        # iterate over the sorted keys to find the highest logged
        # version for each uid / tag combo (unique instance of a given
        # uid
        for entry in entries:
            # use both the uid and tag as the key since a uid may have
            # been deleted and re-created
            section = entry['section'] # should be just 'user' or 'bucket'
            name = entry['name']
            tag = entry['data']['write_version']['tag']
            ver = entry['data']['write_version']['ver']
            status = entry['data']['status']['status']
            key = (name, tag)

            # test if there is already an entry in the dictionary for the user
            if key in metadata_entries:
                # if there is, then only add this one if the ver is higher
                if metadata_entries[key] < ver:
                    metadata_entries[key] = section, ver, status
            else: # if not, just add this entry
                metadata_entries[key] = section, ver, status

        # sync each entry / tag pair
        # bail on any user where a non-200 status is returned
        for key, value in metadata_entries.iteritems():
            # TODO: use separate thread for lock renewal, and check
            # its status here instead of sending another request
            if self.relock_log:
                retVal = self.acquire_log_lock(self.source_conn,
                                               self.local_lock_id,
                                               self.source_zone, shard_num)
            if 200 != retVal:
                print 'error acquiring lock for shard ', shard_num,
                      ' lock_id: ', self.local_lock_id,
                      ' in zone ', self.source_zone,
                      ' in process_entries(). Returned http code ', retVal

                # log unlocking and adding the return value to the
                # result queue will be handled by the calling function
                return retVal

            name, tag = key
            section, ver, status = value
            try:
                md_type = {
                    'user': metadata_type.USER,
                    'bucket': metadata_type.BUCKET,
                    'bucket.instance': metadata_type.BUCKET_INSTANCE
                    }[section]
            except KeyError:
                LOG.error('found unknown metadata type "%s", bailing', section)
                return 500

            if status == 'remove':
                retVal = self.delete_meta_entry(self.dest_conn, name, md_type,
                                                tag)
            elif status == 'write':
                    retVal = self.check_meta_entry(name, md_type, tag)
            else:
                print 'doing something???? to ', name, ' section: ', status
                retVal = 500

            if 200 != retVal:
                print 'error in process_entries() returned http code ', retVal,\
                      ' for', name
                # log unlocking and adding the return value to the result queue
                # will be handled by the calling function
                return retVal
        return retVal


    def run(self):
        while True:
            shard_num = self.work_queue.get()
            if shard_num is None:
                if debug_commands:
                    print 'process ', self.processName, ' is done. Exiting'
                break

            if debug_commands:
                print shard_num, ' is being processed by process ', \
                      self.processName

            # we need this due to a bug in rgw that isn't auto-filling in
            # sensible defaults when start-time is omitted
            really_old_time = "2010-10-10 12:12:00"

            # NOTE rgw deals in UTC time. Make sure you adjust your
            # calls accordingly
            sync_start_time = \
              datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # first, lock the log
            retVal = self.acquire_log_lock(self.source_conn,
                                           self.local_lock_id, self.source_zone,
                                           shard_num)

            if 200 != retVal:
                print 'acquire_log_lock() failed, returned http code: ', retVal
                self.result_queue.put((self.processID, shard_num, retVal))
                continue
            elif debug_commands:
                print 'acquire_log_lock() returned: ', retVal

            # get the log for this shard of the metadata log
            (retVal, out) = self.client.request(self.source_conn,
                                   ['log', 'list', 'id=' + str(shard_num)],
                                   {"type":"metadata", "id":shard_num})

            if 200 != retVal:
                print 'metadata list failed, returned http code: ', retVal
                # we hit an error getting the data to sync.
                # Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, retVal))
                continue
            elif debug_commands:
                print 'metadata list returned: ', retVal

            log_entry_list = out()

            print 'shard ', shard_num, ' has ', len(log_entry_list), ' entries'

            # sort the entries so that a given entry only shows up once
            # to be synced and entries with old tags are removed
            sorted_entries = self.sort_entries(log_entry_list)

            retVal = self.process_entries(sorted_entries)

            if 200 != retVal:
                print 'process_entries() returned http code ', retVal
                # we hit an error processing a user. Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, retVal))
                continue
            elif debug_commands:
                print 'process_entries() returned http code ', retVal

            # trim the log for this shard now that all the users are synched
            # this should only occur if no users threw errors
            (retVal, out) = self.client.request(self.source_conn,
                          ['log', 'trim', 'id=' + str(shard_num)],
                          {'id':shard_num, 'type':'metadata',
                          'start-time':really_old_time,
                          'end-time':sync_start_time})

            if 200 != retVal:
                print 'log trim returned http code ', retVal
                # we hit an error processing a user. Bail and unlock the log
                self.release_log_lock(self.source_conn, self.local_lock_id, \
                                      self.source_zone, shard_num)
                self.result_queue.put((self.processID, shard_num, retVal))
                continue
            elif debug_commands:
                print 'log trim for shard ', shard_num, \
                      ' returned http code ', retVal

            # finally, unlock the log
            self.release_log_lock(self.source_conn, self.local_lock_id, \
                                  self.source_zone, shard_num)

            self.result_queue.put((self.processID, shard_num, retVal))

            if debug_commands:
                print shard_num, ' is done being processed by process ',
                self.processName
