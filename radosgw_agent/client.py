import boto
import functools
import json
import logging
import random
import socket
import urllib
from urlparse import urlparse

from boto.exception import BotoServerError
from boto.s3.connection import S3Connection

from radosgw_agent import request as aws_request
from radosgw_agent import config
from radosgw_agent import exceptions as exc
from radosgw_agent.util import get_dev_logger
from radosgw_agent.constants import DEFAULT_TIME
from radosgw_agent.exceptions import NetworkError

log = logging.getLogger(__name__)
dev_log = get_dev_logger(__name__)


class Endpoint(object):
    def __init__(self, host, port, secure,
                 access_key=None, secret_key=None, region=None, zone=None):
        self.host = host
        default_port = 443 if secure else 80
        self.port = port or default_port
        self.secure = secure
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.zone = zone

    def __eq__(self, other):
        if self.host != other.host:
            return False
        if self.port == other.port:
            return True
        # if self and other are mixed http/https with default ports,
        # i.e. http://example.com and https://example.com, consider
        # them the same

        def diff_only_default_ports(a, b):
            return a.secure and a.port == 443 and not b.secure and b.port == 80
        return (diff_only_default_ports(self, other) or
                diff_only_default_ports(other, self))

    def __repr__(self):
        return 'Endpoint(host={host}, port={port}, secure={secure})'.format(
            host=self.host,
            port=self.port,
            secure=self.secure)

    def __str__(self):
        scheme = 'https' if self.secure else 'http'
        return '{scheme}://{host}:{port}'.format(scheme=scheme,
                                                 host=self.host,
                                                 port=self.port)


def parse_endpoint(endpoint):
    url = urlparse(endpoint)
    if url.scheme not in ['http', 'https']:
        raise exc.InvalidProtocol('invalid protocol %r' % url.scheme)
    if not url.hostname:
        raise exc.InvalidHost('no hostname in %r' % endpoint)
    return Endpoint(url.hostname, url.port, url.scheme == 'https')

code_to_exc = {
    404: exc.NotFound,
    }


def boto_call(func):
    @functools.wraps(func)
    def translate_exception(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except boto.exception.S3ResponseError as e:
            raise code_to_exc.get(e.status, exc.HttpError)(e.status, e.body)
    return translate_exception


def check_result_status(result):
    if result.status / 100 != 2:
        raise code_to_exc.get(result.status,
                              exc.HttpError)(result.status, result.reason)


def url_safe(component):
    if isinstance(component, basestring):
        string = component.encode('utf8')
    else:
        string = str(component)
    return urllib.quote(string)


def request(connection, type_, resource, params=None, headers=None,
            data=None, expect_json=True, special_first_param=None, _retries=3):
    if headers is None:
        headers = {}

    if type_ in ['put', 'post']:
        headers['Content-Type'] = 'application/json; charset=UTF-8'

    request_data = data if data else ''
    if params is None:
        params = {}
    safe_params = dict([(k, url_safe(v)) for k, v in params.iteritems()])
    connection.count_request()
    request = aws_request.base_http_request(connection.s3_connection,
                             type_.upper(),
                             resource=resource,
                             special_first_param=special_first_param,
                             headers=headers,
                             data=request_data,
                             params=safe_params)

    url = '{protocol}://{host}{path}'.format(protocol=request.protocol,
                                             host=request.host,
                                             path=request.path)

    request.authorize(connection=connection)

    boto.log.debug('url = %r\nparams=%r\nheaders=%r\ndata=%r',
                   url, params, request.headers, data)

    try:
        result = aws_request.make_request(
            connection.s3_connection,
            type_.upper(),
            resource=resource,
            special_first_param=special_first_param,
            headers=headers,
            data=request_data,
            params=safe_params,
            _retries=_retries)
    except socket.error as error:
        msg = 'unable to connect to %s %s' % (request.host, error)
        raise NetworkError(msg)

    except BotoServerError as error:
        check_result_status(error)

    check_result_status(result)

    if data or not expect_json:
        return result

    return json.loads(result.read())


def get_metadata(connection, section, name):
    return request(connection, 'get', 'admin/metadata/' + section,
                   params=dict(key=name))


def update_metadata(connection, section, name, metadata):
    if not isinstance(metadata, basestring):
        metadata = json.dumps(metadata)
    return request(connection, 'put', 'admin/metadata/' + section,
                   params=dict(key=name), data=metadata)


def delete_metadata(connection, section, name):
    return request(connection, 'delete', 'admin/metadata/' + section,
                   params=dict(key=name), expect_json=False)


def get_metadata_sections(connection):
    return request(connection, 'get', 'admin/metadata')


def list_metadata_keys(connection, section):
    return request(connection, 'get', 'admin/metadata/' + section)


def get_op_state(connection, client_id, op_id, bucket, obj):
    return request(connection, 'get', 'admin/opstate',
                   params={
                       'op-id': op_id,
                       'object': u'{0}/{1}'.format(bucket, obj.name),
                       'client-id': client_id,
                      }
                   )


def remove_op_state(connection, client_id, op_id, bucket, obj):
    return request(connection, 'delete', 'admin/opstate',
                   params={
                       'op-id': op_id,
                       'object': u'{0}/{1}'.format(bucket, obj.name),
                       'client-id': client_id,
                      },
                   expect_json=False,
                   )


def get_bucket_list(connection):
    return list_metadata_keys(connection, 'bucket')


@boto_call
def list_objects_in_bucket(connection, bucket_name):
    versioned = config['use_versioning']

    # use the boto library to do this
    bucket = connection.get_bucket(bucket_name)
    list_call = bucket.list_versions if versioned else bucket.list
    try:
        for key in list_call():
            yield key
    except boto.exception.S3ResponseError as e:
        # since this is a generator, the exception will be raised when
        # it's read, rather than when this call returns, so raise a
        # unique exception to distinguish this from client errors from
        # other calls
        if e.status == 404:
            raise exc.BucketEmpty()
        else:
            raise


@boto_call
def mark_delete_object(connection, bucket_name, obj, params=None):
    """
    Marking an object for deletion is only necessary for versioned objects, we
    should not try these calls for non-versioned ones.

    Usually, only full-sync operations will use this call, incremental should
    perform actual delete operations with ``delete_versioned_object``
    """
    params = params or {}

    params['rgwx-version-id'] = obj.version_id
    params['rgwx-versioned-epoch'] = obj.VersionedEpoch

    path = u'{bucket}/{object}'.format(
        bucket=bucket_name,
        object=obj.name,
        )

    return request(connection, 'delete', path,
                   params=params,
                   expect_json=False)


@boto_call
def delete_versioned_object(connection, bucket_name, obj):
    """
    Perform a delete on a versioned object, the requirements for these types
    of requests is to be able to pass the ``versionID`` as a query argument
    """
    # if obj.delete_marker is False we should not delete this and we shouldn't
    # have been called, so return without doing anything
    if getattr(obj, 'delete_marker', False) is False:
        log.info('obj: %s has `delete_marker=False`, will skip' % obj.name)
        return

    params = {}

    params['rgwx-version-id'] = obj.version_id
    params['rgwx-versioned-epoch'] = obj.VersionedEpoch
    params['versionID'] = obj.version_id

    path = u'{bucket}/{object}'.format(
        bucket=bucket_name,
        object=obj.name,
        )

    return request(connection, 'delete', path,
                   params=params,
                   expect_json=False)


@boto_call
def delete_object(connection, bucket_name, obj):
    if is_versioned(obj):
        log.debug('performing a delete for versioned obj: %s' % obj.name)
        delete_versioned_object(connection, bucket_name, obj)
    else:
        bucket = connection.get_bucket(bucket_name)
        bucket.delete_key(obj.name)


def is_versioned(obj):
    """
    Check if a given object is versioned by inspecting some of its attributes.
    """
    # before any heuristic, newer versions of RGW will tell if an obj is
    # versioned so try that first
    if hasattr(obj, 'versioned'):
        return obj.versioned

    if not hasattr(obj, 'VersionedEpoch'):
        # overly paranoid here, an object that is not versioned should *never*
        # have a `VersionedEpoch` attribute
        if getattr(obj, 'version_id', None):
            if obj.version_id is None:
                return False
            return True  # probably will never get here
        return False
    return True


def sync_object_intra_region(connection, bucket_name, obj, src_zone,
                             client_id, op_id):

    params = {
        'rgwx-source-zone': src_zone,
        'rgwx-client-id': client_id,
        'rgwx-op-id': op_id,
    }

    if is_versioned(obj):
        log.debug('detected obj as versioned: %s' % obj.name)
        log.debug('obj attributes are:')
        for k in dir(obj):
            if not k.startswith('_'):
                v = getattr(obj, k, None)
                log.debug('%s.%s = %s' % (obj.name, k, v))

        # set the extra params to support versioned operations
        params['rgwx-version-id'] = obj.version_id
        params['rgwx-versioned-epoch'] = obj.VersionedEpoch

        # delete_marker may not exist in the obj
        if getattr(obj, 'delete_marker', None) is True:
            log.debug('obj %s has a delete_marker, marking for deletion' % obj.name)
            # when the object has a delete marker we need to create it with
            # a delete marker on the destination rather than copying
            return mark_delete_object(connection, bucket_name, obj, params=params)

    path = u'{bucket}/{object}'.format(
        bucket=bucket_name,
        object=obj.name,
        )

    return request(connection, 'put', path,
                   params=params,
                   headers={
                       'x-amz-copy-source': url_safe('%s/%s' % (bucket_name, obj.name)),
                       },
                   expect_json=False)


def lock_shard(connection, lock_type, shard_num, zone_id, timeout, locker_id):
    return request(connection, 'post', 'admin/log',
                   params={
                       'type': lock_type,
                       'id': shard_num,
                       'length': timeout,
                       'zone-id': zone_id,
                       'locker-id': locker_id,
                       },
                   special_first_param='lock',
                   expect_json=False)


def unlock_shard(connection, lock_type, shard_num, zone_id, locker_id):
    return request(connection, 'post', 'admin/log',
                   params={
                       'type': lock_type,
                       'id': shard_num,
                       'locker-id': locker_id,
                       'zone-id': zone_id,
                       },
                   special_first_param='unlock',
                   expect_json=False)


def _id_name(type_):
    return 'bucket-instance' if type_ == 'bucket-index' else 'id'


def get_log(connection, log_type, marker, max_entries, id_):
    key = _id_name(log_type)
    return request(connection, 'get', 'admin/log',
                   params={
                       'type': log_type,
                       key: id_,
                       'marker': marker,
                       'max-entries': max_entries,
                       },
                   )


def get_log_info(connection, log_type, id_):
    key = _id_name(log_type)
    return request(
        connection, 'get', 'admin/log',
        params={
            'type': log_type,
            key: id_,
            },
        special_first_param='info',
        )


def num_log_shards(connection, shard_type):
    out = request(connection, 'get', 'admin/log', dict(type=shard_type))
    return out['num_objects']


def set_worker_bound(connection, type_, marker, timestamp,
                     daemon_id, id_, data=None, sync_type='incremental'):
    """

    :param sync_type: The type of synchronization that should be attempted by
    the agent, defaulting to "incremental" but can also be "full".
    """
    if data is None:
        data = []
    key = _id_name(type_)
    boto.log.debug('set_worker_bound: data = %r', data)
    return request(
        connection, 'post', 'admin/replica_log',
        params={
            'type': type_,
            key: id_,
            'marker': marker,
            'time': timestamp,
            'daemon_id': daemon_id,
            'sync-type': sync_type,
            },
        data=json.dumps(data),
        special_first_param='work_bound',
        )


def del_worker_bound(connection, type_, daemon_id, id_):
    key = _id_name(type_)
    return request(
        connection, 'delete', 'admin/replica_log',
        params={
            'type': type_,
            key: id_,
            'daemon_id': daemon_id,
            },
        special_first_param='work_bound',
        expect_json=False,
        )


def get_worker_bound(connection, type_, id_):
    key = _id_name(type_)
    try:
        out = request(
            connection, 'get', 'admin/replica_log',
            params={
                'type': type_,
                key: id_,
                },
            special_first_param='bounds',
            )
        dev_log.debug('get_worker_bound returned: %r', out)
    except exc.NotFound:
        dev_log.debug('no worker bound found for bucket instance "%s"',
                      id_)
        # if no worker bounds have been set, start from the beginning
        # returning fallback, default values
        return dict(
            marker=' ',
            oldest_time=DEFAULT_TIME,
            retries=[]
        )

    retries = set()
    for item in out['markers']:
        names = [retry['name'] for retry in item['items_in_progress']]
        retries = retries.union(names)
    out['retries'] = retries
    return out


class Zone(object):
    def __init__(self, zone_info):
        self.name = zone_info['name']
        self.is_master = False
        self.endpoints = [parse_endpoint(e) for e in zone_info['endpoints']]
        self.log_meta = zone_info['log_meta'] == 'true'
        self.log_data = zone_info['log_data'] == 'true'

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.name


class Region(object):
    def __init__(self, region_info):
        self.name = region_info['key']
        self.is_master = region_info['val']['is_master'] == 'true'
        self.zones = {}
        for zone_info in region_info['val']['zones']:
            zone = Zone(zone_info)
            self.zones[zone.name] = zone
            if zone.name == region_info['val']['master_zone']:
                zone.is_master = True
                self.master_zone = zone
        assert hasattr(self, 'master_zone'), \
               'No master zone found for region ' + self.name

    def __repr__(self):
        return str(self)

    def __str__(self):
        return str(self.zones.keys())


class RegionMap(object):
    def __init__(self, region_map):
        self.regions = {}
        for region_info in region_map['regions']:
            region = Region(region_info)
            self.regions[region.name] = region
            if region.is_master:
                self.master_region = region
        assert hasattr(self, 'master_region'), \
               'No master region found in region map'

    def __repr__(self):
        return str(self)

    def __str__(self):
        return str(self.regions)

    def find_endpoint(self, endpoint):
        for region in self.regions.itervalues():
            for zone in region.zones.itervalues():
                if endpoint in zone.endpoints or endpoint.zone == zone.name:
                    return region, zone
        raise exc.ZoneNotFound('%s not found in region map' % endpoint)


def get_region_map(connection):
    region_map = request(connection, 'get', 'admin/config')
    return RegionMap(region_map)


def _validate_sync_dest(dest_region, dest_zone):
    if dest_region.is_master and dest_zone.is_master:
        raise exc.InvalidZone('destination cannot be master zone of master region')


def _validate_sync_source(src_region, src_zone, dest_region, dest_zone,
                          meta_only):
    if not src_zone.is_master:
        raise exc.InvalidZone('source zone %s must be a master zone' % src_zone.name)
    if (src_region.name == dest_region.name and
        src_zone.name == dest_zone.name):
        raise exc.InvalidZone('source and destination must be different zones')
    if not src_zone.log_meta:
        raise exc.InvalidZone('source zone %s must have metadata logging enabled' % src_zone.name)
    if not meta_only and not src_zone.log_data:
        raise exc.InvalidZone('source zone %s must have data logging enabled' % src_zone.name)
    if not meta_only and src_region.name != dest_region.name:
        raise exc.InvalidZone('data sync can only occur between zones in the same region')
    if not src_zone.endpoints:
        raise exc.InvalidZone('region map contains no endpoints for default source zone %s' % src_zone.name)


def configure_endpoints(region_map, dest_endpoint, src_endpoint, meta_only):
    print('region map is: %r' % region_map)

    dest_region, dest_zone = region_map.find_endpoint(dest_endpoint)
    _validate_sync_dest(dest_region, dest_zone)

    # source may be specified by http endpoint or zone name
    if src_endpoint.host or src_endpoint.zone:
        src_region, src_zone = region_map.find_endpoint(src_endpoint)
    else:
        # try the master zone in the same region, then the master zone
        # in the master region
        try:
            _validate_sync_source(dest_region, dest_region.master_zone,
                                  dest_region, dest_zone, meta_only)
            src_region, src_zone = dest_region, dest_region.master_zone
        except exc.InvalidZone as e:
            log.debug('source region %s zone %s unaccetpable: %s',
                      dest_region.name, dest_region.master_zone.name, e)
            master_region = region_map.master_region
            src_region, src_zone = master_region, master_region.master_zone

    _validate_sync_source(src_region, src_zone, dest_region, dest_zone,
                          meta_only)

    # choose a random source endpoint if one wasn't specified
    if not src_endpoint.host:
        endpoint = random.choice(src_zone.endpoints)
        src_endpoint.host = endpoint.host
        src_endpoint.port = endpoint.port
        src_endpoint.secure = endpoint.secure

    # fill in region and zone names
    dest_endpoint.region = dest_region
    dest_endpoint.zone = dest_zone
    src_endpoint.region = src_region
    src_endpoint.zone = src_zone


class S3ConnectionWrapper(object):
    def __init__(self, endpoint, debug):
        self.endpoint = endpoint
        self.debug = debug
        self.s3_connection = None
        self.reqs_before_reset = 512
        self._recreate_s3_connection()

    def count_request(self):
        self.num_requests += 1
        if self.num_requests > self.reqs_before_reset:
            self._recreate_s3_connection()

    def _recreate_s3_connection(self):
        self.num_requests = 0
        self.s3_connection = S3Connection(
            aws_access_key_id=self.endpoint.access_key,
            aws_secret_access_key=self.endpoint.secret_key,
            is_secure=self.endpoint.secure,
            host=self.endpoint.host,
            port=self.endpoint.port,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            debug=self.debug,
        )

    def __getattr__(self, attrib):
        return getattr(self.s3_connection, attrib)


def connection(endpoint, debug=None):
    log.info('creating connection to endpoint: %s' % endpoint)
    return S3ConnectionWrapper(endpoint, debug)
