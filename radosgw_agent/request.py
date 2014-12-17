import boto
import logging
from boto.connection import AWSAuthConnection
from urllib import urlencode

log = logging.getLogger(__name__)


class MetaData(object):
    """
    A basic container class that other than the method, it just registers
    all the keyword arguments passed in, so that it is easier/nicer to
    re-use the values
    """

    def __init__(self, conn, method, **kw):
        self.conn = conn
        self.method = method
        for k, v in kw.items():
            setattr(self, k, v)


def base_http_request(conn, method, basepath='', resource='', headers=None,
                      data=None, special_first_param=None, params=None):
    """
    Returns a ``AWSAuthConnection.build_base_http_request`` call with the
    preserving of the special params done by ``build``.
    """

    # request meta data
    md = build(
        conn,
        method,
        basepath=basepath,
        resource=resource,
        headers=headers,
        data=data,
        special_first_param=special_first_param,
        params=params,
    )

    return AWSAuthConnection.build_base_http_request(
        md.conn, md.method, md.path,
        md.auth_path, md.params, md.headers,
        md.data, md.host)


def make_request(conn, method, basepath='', resource='', headers=None,
                 data=None, special_first_param=None, params=None, _retries=3):
    """
    Returns a ``AWSAuthConnection.make_request`` call with the preserving
    of the special params done by ``build``.
    """
    # request meta data
    md = build(
        conn,
        method,
        basepath=basepath,
        resource=resource,
        headers=headers,
        data=data,
        special_first_param=special_first_param,
        params=params,
    )

    if params:
        # we basically need to do this ourselves now. BOTO doesn't do it for us
        # in make_request
        result = []
        for k, vs in params.items():
            if isinstance(vs, basestring) or not hasattr(vs, '__iter__'):
                vs = [vs]
            for v in vs:
                if v is not None:
                    result.append(
                        (k.encode('utf-8') if isinstance(k, str) else k,
                         v.encode('utf-8') if isinstance(v, str) else v))
        if md.special_first_param:
        appending_char = '&' if md.special_first_param else '?'
            md.path = '%s%s%s' % (md.path, appending_char, urlencode(result, doseq=True))

    return AWSAuthConnection.make_request(
        md.conn, md.method, md.path,
        headers=md.headers,
        data=md.data,
        host=md.host,
        auth_path=md.auth_path,
        params=md.params,
        override_num_retries=_retries
    )


def build(conn, method, basepath='', resource='', headers=None,
           data=None, special_first_param=None, params=None):
    """
    Adapted from the build_request() method of boto.connection
    """

    path = conn.calling_format.build_path_base(basepath, resource)
    auth_path = conn.calling_format.build_auth_path(basepath, resource)
    host = conn.calling_format.build_host(conn.server_name(), '')

    if special_first_param:
        path += '?' + special_first_param
        boto.log.debug('path=%s' % path)
        auth_path += '?' + special_first_param
        boto.log.debug('auth_path=%s' % auth_path)

    return MetaData(
        conn,
        method,
        path=path,
        auth_path=auth_path,
        basepath=basepath,
        resource=resource,
        headers=headers,
        data=data,
        special_first_param=special_first_param,
        params=params,
        host=host,
    )
