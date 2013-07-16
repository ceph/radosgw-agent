import boto
import collections
import json
import requests

from boto.connection import AWSAuthConnection

PUT_CMDS = ['create', 'link', 'add', 'metaput']
POST_CMDS = ['unlink', 'modify', 'lock', 'unlock']
DELETE_CMDS = ['trim', 'rm', 'process', 'delete']
GET_CMDS = ['check', 'info', 'show', 'list', 'get', 'metaget']
BUCKET_RESOURCES = ['object', 'policy', 'index']
USER_RESOURCES = ['subuser', 'key', 'caps']
ZONE_RESOURCES = ['pool', 'garbage']
MDLOG_RESOURCES = ['mdlog']
LOG_RESOURCES = ['lock', 'unlock', 'trim']

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

def _get_cmd_method_and_handler(cmd):
    if cmd[1] in PUT_CMDS:
        return 'PUT', requests.put
    elif cmd[1] in DELETE_CMDS:
        return 'DELETE', requests.delete
    elif cmd[1] in POST_CMDS:
        return 'POST', requests.post
    elif cmd[1] in GET_CMDS:
        return 'GET', requests.get

def _get_resource(cmd):
    if cmd[0] == 'bucket' or cmd[0] in BUCKET_RESOURCES:
        if len(cmd) == 3 and cmd[0] == 'object':
            return cmd[2], ''
        elif len(cmd) == 3 and cmd[1] == 'delete': # for deleting a bucket
            return cmd[2], ''
        elif cmd[0] == 'bucket':
            return 'bucket', ''
        else:
            return 'bucket', cmd[0]
    elif cmd[0] == 'user' or cmd[0] in USER_RESOURCES:
        if len(cmd) == 3 and cmd[0] == 'user': # used for deleteing users
            return 'metadata/user', 'key=' + cmd[2]
        elif cmd[0] == 'user':
            return 'user', ''
        else:
            return 'user', cmd[0]
    elif cmd[0] == 'usage':
        return 'usage', ''
    elif cmd[0] == 'zone' or cmd[0] in ZONE_RESOURCES:
        if cmd[0] == 'zone':
            return 'zone', ''
        else:
            return 'zone', cmd[0]
    elif cmd[0] == 'metadata':
        if len(cmd) == 3 and cmd[1] in ['metaget', 'metaput', 'delete']:
            return 'metadata/' + cmd[2], ''
        else:
            return 'metadata', ''
    elif cmd[0] == 'log':
        if len(cmd) == 3 and cmd[1]=='list':
            return 'log', cmd[2]
        elif len(cmd) == 2 and cmd[1]=='lock':
            return 'log', cmd[1]
        elif len(cmd) == 2 and cmd[1]=='unlock':
            return 'log', cmd[1]
        elif len(cmd) == 3 and cmd[1]=='trim':
            return 'log', cmd[2]
        else:
            return 'log', ''

"""
Adapted from the build_request() method of boto.connection
"""

def _build_request(conn, cmd, method, basepath='', resource = '', headers=None,
                   data=None, query_args=None, params=None):
    path = conn.calling_format.build_path_base(basepath, resource)
    auth_path = conn.calling_format.build_auth_path(basepath, resource)
    host = conn.calling_format.build_host(conn.server_name(), '')

    if query_args:
        path += '?' + query_args
        boto.log.debug('path=%s' % path)
        auth_path += '?' + query_args
        boto.log.debug('auth_path=%s' % auth_path)

    return AWSAuthConnection.build_base_http_request(
        conn, method, path, auth_path, params, headers, data, host)

def request(connection, cmd, params=None, headers=None,
            data=None, admin=True, expect_json=True):
    if headers is None:
        headers = {}

    headers['Content-Type'] = 'application/json; charset=UTF-8'

    method, handler = _get_cmd_method_and_handler(cmd)
    resource, query_args = _get_resource(cmd)

    basepath = 'admin' if admin else ''
    request_data = data if data else ''
    request = _build_request(connection, cmd, method, basepath, resource,
                             query_args=query_args,
                             headers=headers,
                             data=request_data,
                             params=params)

    url = '{protocol}://{host}{path}'.format(protocol=request.protocol,
                                             host=request.host,
                                             path=request.path)

    request.authorize(connection=connection)

    result = handler(url, params=params, headers=request.headers, data=data)

    if result.status_code / 100 != 2:
        raise code_to_exc.get(result.status_code,
                              HttpError)(result.status_code, result.content)

    if data or not expect_json:
        return result.raw
    return result.json()

def get_metadata(connection, section, name):
    return request(connection, ['metadata', 'metaget', section],
                   dict(key=name))

def update_metadata(connection, section, name, metadata):
    if not isinstance(metadata, basestring):
        metadata = json.dumps(metadata)
    return request(connection, ['metadata', 'metaput', section],
                   dict(key=name), data=metadata)

def delete_metadata(connection, section, name):
    return request(connection, ['metadata', 'delete', section],
                   dict(key=name))

def get_metadata_sections(connection):
    return request(connection, ['metadata', 'get'])

def list_metadata_keys(connection, section):
    return request(connection, ['metadata', 'metaget', section])

def lock_shard(connection, lock_type, shard_num, zone_id, timeout, locker_id):
    return request(connection, ['log', 'lock'],
                   {'type': lock_type, 'id': shard_num,
                    'length': timeout, 'zone-id': zone_id,
                    'locker-id': locker_id}, expect_json=False)

def unlock_shard(connection, lock_type, shard_num, zone_id, locker_id):
    return request(connection, ['log', 'unlock'],
                   {'type': lock_type, 'id': shard_num,
                    'locker-id': locker_id, 'zone-id': zone_id}, expect_json=False)

def get_meta_log(connection, shard_num, start_time, end_time):
    return request(connection, ['log', 'list', 'id=' + str(shard_num)],
                   {'type': 'metadata', 'id': shard_num,
                    'start-time': start_time, 'end-time': end_time})

def num_log_shards(connection, shard_type):
    out = request(connection, ['log', 'list', 'type=' + shard_type])
    return out['num_objects']
