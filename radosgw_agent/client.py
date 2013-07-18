import boto
import collections
import json
import requests

from boto.connection import AWSAuthConnection
from boto.s3.connection import S3Connection

Endpoint = collections.namedtuple('Endpoint', ['host', 'port', 'access_key',
                                               'secret_key', 'zone'])

class HttpError(Exception):
    def __init__(self, code, body):
        self.code = code
        self.body = body
        self.message = 'Http error code %d content %s' % (code, body)
    def __str__(self):
        return self.message
class NotFound(HttpError):
    pass
code_to_exc = {
    404: NotFound,
    }

"""
Adapted from the build_request() method of boto.connection
"""

def _build_request(conn, method, basepath='', resource = '', headers=None,
                   data=None, special_first_param=None, params=None):
    path = conn.calling_format.build_path_base(basepath, resource)
    auth_path = conn.calling_format.build_auth_path(basepath, resource)
    host = conn.calling_format.build_host(conn.server_name(), '')

    if special_first_param:
        path += '?' + special_first_param
        boto.log.debug('path=%s' % path)
        auth_path += '?' + special_first_param
        boto.log.debug('auth_path=%s' % auth_path)

    return AWSAuthConnection.build_base_http_request(
        conn, method, path, auth_path, params, headers, data, host)

def request(connection, type_, resource, params=None, headers=None,
            data=None, expect_json=True, special_first_param=None):
    if headers is None:
        headers = {}

    if type_ in ['put', 'post']:
        headers['Content-Type'] = 'application/json; charset=UTF-8'

    request_data = data if data else ''
    request = _build_request(connection,
                             type_.upper(),
                             resource=resource,
                             special_first_param=special_first_param,
                             headers=headers,
                             data=request_data,
                             params=params)

    url = '{protocol}://{host}{path}'.format(protocol=request.protocol,
                                             host=request.host,
                                             path=request.path)

    request.authorize(connection=connection)

    handler = getattr(requests, type_)
    result = handler(url, params=params, headers=request.headers, data=data)

    if result.status_code / 100 != 2:
        raise code_to_exc.get(result.status_code,
                              HttpError)(result.status_code, result.content)

    if data or not expect_json:
        return result.raw
    return result.json()

def get_metadata(connection, section, name):
    return request(connection, 'get', '/admin/metadata/' + section,
                   params=dict(key=name))

def update_metadata(connection, section, name, metadata):
    if not isinstance(metadata, basestring):
        metadata = json.dumps(metadata)
    return request(connection, 'put', '/admin/metadata/' + section,
                   params=dict(key=name), data=metadata)

def delete_metadata(connection, section, name):
    return request(connection, 'delete', '/admin/metadata/' + section,
                   params=dict(key=name), expect_json=False)

def get_metadata_sections(connection):
    return request(connection, 'get', '/admin/metadata')

def list_metadata_keys(connection, section):
    return request(connection, 'get', '/admin/metadata/' + section)

def lock_shard(connection, lock_type, shard_num, zone_id, timeout, locker_id):
    return request(connection, 'post', '/admin/log',
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
    return request(connection, 'post', '/admin/log',
                   params={
                       'type': lock_type,
                       'id': shard_num,
                       'locker-id': locker_id,
                       'zone-id': zone_id,
                       },
                   special_first_param='unlock',
                   expect_json=False)

def get_meta_log(connection, shard_num, marker, max_entries):
    return request(connection, 'get', '/admin/log',
                   params={
                       'type': 'metadata',
                       'id': shard_num,
                       'marker': marker,
                       'max-entries': max_entries,
                       },
                   )

def get_log_info(connection, log_type, shard_num):
    return request(
        connection, 'get', '/admin/log',
        params=dict(
            type=log_type,
            id=shard_num,
            ),
        special_first_param='info',
        )

def num_log_shards(connection, shard_type):
    out = request(connection, 'get', '/admin/log', dict(type=shard_type))
    return out['num_objects']

def set_worker_bound(connection, type_, shard_num, marker, timestamp,
                     daemon_id):
    return request(
        connection, 'post', '/admin/replica_log',
        params=dict(
            type=type_,
            id=shard_num,
            marker=marker,
            time=timestamp,
            daemon_id=daemon_id,
            ),
        data='[]',
        special_first_param='work_bound',
        )

def del_worker_bound(connection, type_, shard_num, daemon_id):
    return request(
        connection, 'delete', '/admin/replica_log',
        params=dict(
            type=type_,
            id=shard_num,
            daemon_id=daemon_id,
            ),
        special_first_param='work_bound',
        expect_json=False,
        )

def get_min_worker_bound(connection, type_, shard_num):
    out = request(
        connection, 'get', '/admin/replica_log',
        params=dict(
            type=type_,
            id=shard_num,
            ),
        special_first_param='bounds',
        )
    return out['marker'], out['oldest_time']

def connection(endpoint, is_secure=False, debug=None):
    return S3Connection(
        aws_access_key_id=endpoint.access_key,
        aws_secret_access_key=endpoint.secret_key,
        is_secure=is_secure,
        host=endpoint.host,
        port=endpoint.port,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        debug=debug,
        )
