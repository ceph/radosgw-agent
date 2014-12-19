import boto
import logging
import sys
from boto.connection import AWSAuthConnection

log = logging.getLogger(__name__)


def urlencode(query, doseq=0):
    """
    Note: ported from urllib.urlencode, but with the ability to craft the query
    string without quoting the params again.

    Encode a sequence of two-element tuples or dictionary into a URL query
    string.

    If any values in the query arg are sequences and doseq is true, each
    sequence element is converted to a separate parameter.

    If the query arg is a sequence of two-element tuples, the order of the
    parameters in the output will match the order of parameters in the
    input.
    """

    if hasattr(query, "items"):
        # mapping objects
        query = query.items()
    else:
        # it's a bother at times that strings and string-like objects are
        # sequences...
        try:
            # non-sequence items should not work with len()
            # non-empty strings will fail this
            if len(query) and not isinstance(query[0], tuple):
                raise TypeError
            # zero-length sequences of all types will get here and succeed,
            # but that's a minor nit - since the original implementation
            # allowed empty dicts that type of behavior probably should be
            # preserved for consistency
        except TypeError:
            ty, va, tb = sys.exc_info()
            raise TypeError, "not a valid non-string sequence or mapping object", tb

    l = []
    if not doseq:
        # preserve old behavior
        for k, v in query:
            k = str(k)
            v = str(v)
            l.append(k + '=' + v)
        print l
    else:
        for k, v in query:
            k = str(k)
            if isinstance(v, str):
                l.append(k + '=' + v)
            elif isinstance(v, unicode):
                # is there a reasonable way to convert to ASCII?
                # encode generates a string, but "replace" or "ignore"
                # lose information and "strict" can raise UnicodeError
                v = v.encode("ASCII", "replace")
                l.append(k + '=' + v)
            else:
                try:
                    # is this a sufficient test for sequence-ness?
                    len(v)
                except TypeError:
                    # not a sequence
                    v = str(v)
                    l.append(k + '=' + v)
                else:
                    # loop over the sequence
                    for elt in v:
                        l.append(k + '=' + str(elt))
    return '&'.join(l)


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
