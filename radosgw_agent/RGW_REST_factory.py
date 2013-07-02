
import boto
import logging
import requests

from boto.connection import AWSAuthConnection

#debug_commands = True
debug_commands = False

# a class that generates and executes REST calls against an RGW
class RGW_REST_factory:
    logging.basicConfig(filename='boto_rest.log', level=logging.DEBUG)


    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.put_cmds = ['create', 'link', 'add', 'metaput']
        self.post_cmds = ['unlink', 'modify', 'lock', 'unlock']
        self.delete_cmds = ['trim', 'rm', 'process', 'delete']
        self.get_cmds = ['check', 'info', 'show', 'list', 'get', 'metaget']

        self.bucket_sub_resources = ['object', 'policy', 'index']
        self.user_sub_resources = ['subuser', 'key', 'caps']
        #zone_sub_resources = ['pool', 'log', 'garbage']
        self.zone_sub_resources = ['pool', 'garbage']
        self.mdlog_sub_resources= ['mdlog']
        self.log_sub_resources = ['lock', 'unlock', 'trim']

    def get_cmd_method_and_handler(self, cmd):
        #print 'cmd: ', cmd
        if cmd[1] in self.put_cmds:
            return 'PUT', requests.put
        elif cmd[1] in self.delete_cmds:
            return 'DELETE', requests.delete
        elif cmd[1] in self.post_cmds:
            return 'POST', requests.post
        elif cmd[1] in self.get_cmds:
            return 'GET', requests.get

    def get_resource(self, cmd):
        if cmd[0] == 'bucket' or cmd[0] in self.bucket_sub_resources:
            if len(cmd) == 3 and cmd[0] == 'object':
                return cmd[2], ''
            elif len(cmd) == 3 and cmd[1] == 'delete': # for deleting a bucket
                return cmd[2], ''
            elif cmd[0] == 'bucket':
                return 'bucket', ''
            else:
                return 'bucket', cmd[0]
        elif cmd[0] == 'user' or cmd[0] in self.user_sub_resources:
            if len(cmd) == 3 and cmd[0] == 'user': # used for deleteing users
                return 'metadata/user', 'key=' + cmd[2]
            elif cmd[0] == 'user':
                return 'user', ''
            else:
                return 'user', cmd[0]
        elif cmd[0] == 'usage':
            return 'usage', ''
        elif cmd[0] == 'zone' or cmd[0] in self.zone_sub_resources:
            if cmd[0] == 'zone':
                return 'zone', ''
            else:
                return 'zone', cmd[0]
        elif cmd[0] == 'metadata':
            if len(cmd) == 3 and (cmd[1] == 'metaget' or cmd[1] == 'metaput'):
                return 'metadata/' + cmd[2], ''
            else:
                return 'metadata', ''
        elif cmd[0] == 'log':
            #if len(cmd) == 2 and cmd[1]=='list':
            #    return 'log/'+cmd[1], ''
            if len(cmd) == 3 and cmd[1]=='list':
                return 'log', cmd[2]
            elif len(cmd) == 2 and cmd[1]=='lock':
                return 'log', cmd[1]
            elif len(cmd) == 2 and cmd[1]=='unlock':
                return 'log', cmd[1]
            elif len(cmd) == 3 and cmd[1]=='trim':
                return 'log', cmd[2]
            else:
                #return 'log', cmd[1]
                return 'log', ''

    """
        Adapted from the build_request() method of boto.connection
    """
    def build_admin_request(self, conn, cmd, method, resource = '', headers=None, data=None,
            query_args=None, params=None):

        #print 'not an object, go nuts'
        path = conn.calling_format.build_path_base('admin', resource)
        auth_path = conn.calling_format.build_auth_path('admin', resource)
        host = conn.calling_format.build_host(conn.server_name(), 'admin')

        if query_args:
            path += '?' + query_args
            boto.log.debug('path=%s' % path)
            auth_path += '?' + query_args
            boto.log.debug('auth_path=%s' % auth_path)

        retRequest = AWSAuthConnection.build_base_http_request(conn, method, path,
                auth_path, params, headers, data, host)

        return retRequest

    def build_request(self, conn, cmd, method, resource = '', headers=None, data=None,
            query_args=None, params=None):

        path = conn.calling_format.build_path_base('', resource)
        auth_path = conn.calling_format.build_auth_path('', resource)
        host = conn.calling_format.build_host(conn.server_name(), '')

        if query_args:
            path += '?' + query_args
            boto.log.debug('path=%s' % path)
            auth_path += '?' + query_args
            boto.log.debug('auth_path=%s' % auth_path)

        retRequest = AWSAuthConnection.build_base_http_request(conn, method, path,
                auth_path, params, headers, data, host)

        return retRequest

    def rest_call(self, connection, cmd, params=None, headers=None, raw=False, data='', admin=True):
        self.log.info('radosgw-admin-rest: %s %s' % (cmd, params))

        if headers is None:
          headers = {}

        headers['Content-Type'] = 'application/json; charset=UTF-8'

        method, handler = self.get_cmd_method_and_handler(cmd)
        resource, query_args = self.get_resource(cmd)

        if admin:
          request = self.build_admin_request(connection, cmd, method, resource,
                    query_args=query_args, headers=headers, data=data, params=params)
        else:
          request = self.build_request(connection, cmd, method, resource,
                    query_args=query_args, headers=headers, data=data, params=params)

        #if data:
        #  request = self.build_admin_request(connection, cmd, method, resource,
        #            query_args=query_args, headers=headers, data=data, params=params)
        #else:
        #  request = self.build_admin_request(connection, cmd, method, resource,
        #            query_args=query_args, headers=headers)

        url = '{protocol}://{host}{path}'.format(protocol=request.protocol,
              host=request.host, path=request.path, params=params)

        tmpHeaders = request.headers
        request.authorize(connection=connection)

        if data:
          result = handler(url, params=params, headers=request.headers, data=data)
        else:
          result = handler(url, params=params, headers=request.headers)

        if raw:
            return result.status_code, result.txt
        else:
            return result.status_code, result.json

