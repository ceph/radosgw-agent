import boto
import py.test
from mock import Mock
import httpretty
import re

from radosgw_agent import client

# parametrization helpers

def endpoints():
    return [
        ('http://example.org', 'example.org', 80, False),
        ('https://example.org', 'example.org', 443, True),
        ('https://example.org:8080', 'example.org', 8080, True),
        ('https://example.org:8080/', 'example.org', 8080, True),
        ('http://example.org:81/a/b/c?b#d', 'example.org', 81, False),
    ]


REGION_MAP = {
    "regions": [
        {
            "val": {
                "zones": [
                    {
                        "endpoints": [
                            "http://vit:8001/"
                            ],
                        "log_data": "true",
                        "log_meta": "true",
                        "name": "skinny-1"
                        },
                    {
                        "endpoints": [
                            "http://vit:8002/"
                            ],
                        "log_data": "false",
                        "log_meta": "false",
                        "name": "skinny-2"
                        }
                    ],
                "name": "skinny",
                "default_placement": "",
                "master_zone": "skinny-1",
                "api_name": "slim",
                "placement_targets": [],
                "is_master": "true",
                "endpoints": [
                    "http://skinny:80/"
                    ]
                },
            "key": "skinny"
            },
        {
            "val": {
                "zones": [
                    {
                        "endpoints": [
                            "http://vit:8003/"
                            ],
                        "log_data": "false",
                        "log_meta": "false",
                        "name": "swab-2"
                        },
                    {
                        "endpoints": [
                            "http://vit:8004/"
                            ],
                        "log_data": "false",
                        "log_meta": "false",
                        "name": "swab-3"
                        },
                    {
                        "endpoints": [
                            "http://vit:8000/"
                            ],
                        "log_data": "true",
                        "log_meta": "true",
                        "name": "swab-1"
                        }
                    ],
                "name": "swab",
                "default_placement": "",
                "master_zone": "swab-1",
                "api_name": "shady",
                "placement_targets": [],
                "is_master": "false",
                "endpoints": [
                    "http://vit:8000/"
                    ]
                },
            "key": "swab"
            },
        {
            "val": {
                "zones": [
                    {
                        "endpoints": [
                            "http://ro:80/"
                            ],
                        "log_data": "false",
                        "log_meta": "false",
                        "name": "ro-1"
                        },
                    {
                        "endpoints": [
                            "http://ro:8080/"
                            ],
                        "log_data": "false",
                        "log_meta": "false",
                        "name": "ro-2"
                        },
                    ],
                "name": "readonly",
                "default_placement": "",
                "master_zone": "ro-1",
                "api_name": "readonly",
                "placement_targets": [],
                "is_master": "false",
                "endpoints": [
                    "http://ro:80/",
                    "http://ro:8080/"
                    ]
                },
            "key": "readonly"
            },
        {
            "val": {
                "zones": [
                    {
                        "endpoints": [
                            "http://meta:80/"
                            ],
                        "log_data": "false",
                        "log_meta": "true",
                        "name": "meta-1"
                        },
                    {
                        "endpoints": [
                            "http://meta:8080/"
                            ],
                        "log_data": "false",
                        "log_meta": "false",
                        "name": "meta-2"
                        },
                    ],
                "name": "metaonly",
                "default_placement": "",
                "master_zone": "meta-1",
                "api_name": "metaonly",
                "placement_targets": [],
                "is_master": "false",
                "endpoints": [
                    "http://meta:80/",
                    "http://meta:8080/"
                    ]
                },
            "key": "metaonly"
            }
        ],
    "master_region": "skinny"
    }

def test_endpoint_default_port():
    endpoint = client.Endpoint('example.org', None, True)
    assert endpoint.port == 443
    endpoint = client.Endpoint('example.org', None, False)
    assert endpoint.port == 80

def test_endpoint_port_specified():
    endpoint = client.Endpoint('example.org', 80, True)
    assert endpoint.port == 80
    endpoint = client.Endpoint('example.org', 443, True)
    assert endpoint.port == 443

def test_endpoint_equality():
    default_port = client.Endpoint('a.org', None, True)
    secure = client.Endpoint('a.org', 443, True)
    insecure = client.Endpoint('a.org', 80, False)
    assert default_port == secure
    assert secure == insecure
    assert insecure == default_port

def test_endpoint_inequality():
    base = client.Endpoint('a.org', 80, True)
    diff_host = client.Endpoint('b.org', 80, True)
    diff_port = client.Endpoint('a.org', 81, True)
    insecure = client.Endpoint('a.org', 8080, False)
    assert base != diff_host
    assert base != diff_port
    assert base != insecure


@py.test.mark.parametrize('url, host, port, secure', endpoints())
def test_parse_endpoint(url, host, port, secure):
    endpoint = client.parse_endpoint(url)
    assert endpoint.port == port
    assert endpoint.host == host
    assert endpoint.secure == secure


@py.test.mark.parametrize('url, host, port, secure', endpoints())
def test_parse_repr(url, host, port, secure):
    endpoint = repr(client.parse_endpoint(url))
    assert str(secure) in endpoint
    assert str(host) in endpoint
    assert str(port) in endpoint


def test_parse_endpoint_bad_input():
    with py.test.raises(client.InvalidProtocol):
        client.parse_endpoint('ftp://example.com')
    with py.test.raises(client.InvalidHost):
        client.parse_endpoint('http://:80/')

def _test_configure_endpoints(dest_url, dest_region, dest_zone,
                              expected_src_url, expected_src_region,
                              expected_src_zone, specified_src_url=None,
                              meta_only=False):
    dest = client.parse_endpoint(dest_url)
    if specified_src_url is not None:
        src = client.parse_endpoint(specified_src_url)
    else:
        src = client.Endpoint(None, None, None)
    region_map = client.RegionMap(REGION_MAP)
    client.configure_endpoints(region_map, dest, src, meta_only)
    assert dest.region.name == dest_region
    assert dest.zone.name == dest_zone
    assert src == client.parse_endpoint(expected_src_url)
    assert src.region.name == expected_src_region
    assert src.zone.name == expected_src_zone

def test_configure_endpoints_2nd_region_master_zone_meta():
    _test_configure_endpoints('http://vit:8000', 'swab', 'swab-1',
                              'http://vit:8001', 'skinny', 'skinny-1',
                              meta_only=True)

def test_configure_endpoints_2nd_region_master_zone_data():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://vit:8000', 'swab', 'swab-1',
                                  'http://vit:8001', 'skinny', 'skinny-1',
                                  meta_only=False)

def test_configure_endpoints_master_region_2nd_zone():
    _test_configure_endpoints('http://vit:8002', 'skinny', 'skinny-2',
                              'http://vit:8001', 'skinny', 'skinny-1')

def test_configure_endpoints_2nd_region_2nd_zone():
    _test_configure_endpoints('http://vit:8003', 'swab', 'swab-2',
                              'http://vit:8000', 'swab', 'swab-1')

def test_configure_endpoints_2nd_region_readonly_meta():
    _test_configure_endpoints('http://ro:8080', 'readonly', 'ro-2',
                              'http://vit:8001', 'skinny', 'skinny-1',
                              meta_only=True)

def test_configure_endpoints_2nd_region_readonly_data():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://ro:8080', 'readonly', 'ro-2',
                                  'http://vit:8001', 'skinny', 'skinny-1',
                                  meta_only=False)

def test_configure_endpoints_2nd_region_metaonly_meta():
    _test_configure_endpoints('http://meta:8080', 'metaonly', 'meta-2',
                              'http://meta:80', 'metaonly', 'meta-1',
                              meta_only=True)

def test_configure_endpoints_2nd_region_metaonly_data():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://meta:8080', 'metaonly', 'meta-2',
                                  'http://vit:8001', 'skinny', 'skinny-1',
                                  meta_only=False)

def test_configure_endpoints_master_region_master_zone():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://vit:8001', 'skinny', 'skinny-1',
                                  'http://vit:8001', 'skinny', 'skinny-1')

def test_configure_endpoints_specified_src_same_region():
    _test_configure_endpoints('http://vit:8003', 'swab', 'swab-2',
                              'http://vit:8000', 'swab', 'swab-1',
                              'http://vit:8000')

def test_configure_endpoints_specified_src_master_region_meta():
    _test_configure_endpoints('http://vit:8003', 'swab', 'swab-2',
                              'http://vit:8001', 'skinny', 'skinny-1',
                              'http://vit:8001', meta_only=True)

def test_configure_endpoints_specified_src_master_region_data():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://vit:8003', 'swab', 'swab-2',
                                  'http://vit:8001', 'skinny', 'skinny-1',
                                  'http://vit:8001', meta_only=False)

def test_configure_endpoints_bad_src_same_region():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://vit:8003', 'swab', 'swab-2',
                                  'http://vit:8004', 'swab', 'swab-3',
                                  'http://vit:8004')

def test_configure_endpoints_bad_src_master_region():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://vit:8003', 'swab', 'swab-2',
                                  'http://vit:8002', 'skinny', 'skinny-2',
                                  'http://vit:8002')

def test_configure_endpoints_bad_src_same_zone():
    with py.test.raises(client.InvalidZone):
        _test_configure_endpoints('http://vit:8000', 'swab', 'swab-1',
                                  'http://vit:8000', 'swab', 'swab-1',
                                  'http://vit:8000')

def test_configure_endpoints_specified_nonexistent_src():
    with py.test.raises(client.ZoneNotFound):
        _test_configure_endpoints('http://vit:8005', 'skinny', 'skinny-1',
                                  'http://vit:8001', 'skinny', 'skinny-1',
                                  'http://vit:80')

def test_configure_endpoints_unknown_zone():
    with py.test.raises(client.ZoneNotFound):
        _test_configure_endpoints('http://vit:8005', 'skinny', 'skinny-1',
                                  'http://vit:8001', 'skinny', 'skinny-1')

def http_invalid_status_codes():
    return [
        101, 102, 300, 301, 302, 303, 304, 305, 306, 307, 308,
    ]

def http_valid_status_codes():
    return [
        200, 201, 202, 203, 204, 205, 207, 208, 226,
    ]

class TestCheckResultStatus(object):

    @py.test.mark.parametrize('code', http_invalid_status_codes())
    def test_check_raises_http_error(self, code):
        response = Mock()
        response.status = code
        with py.test.raises(client.HttpError):
            client.check_result_status(response)

    @py.test.mark.parametrize('code', http_valid_status_codes())
    def test_check_does_not_raise_http_error(self, code):
        response = Mock()
        response.status = code
        assert client.check_result_status(response) is None


    def test_check_raises_not_found(self):
        response = Mock()
        response.status = 404
        with py.test.raises(client.NotFound):
            client.check_result_status(response)


class TestBotoCall(object):

    def test_return_val(self):
        @client.boto_call
        def foo(*args, **kwargs):
            return (args, kwargs)
        assert foo(1) == ((1,), {})
        assert foo(b=2) == (tuple(), {'b': 2})

    def test_boto_exception_not_found(self):
        @client.boto_call
        def foo():
            raise boto.exception.S3ResponseError(404, '')

        with py.test.raises(client.NotFound):
            foo()

    def test_non_boto_exception(self):
        @client.boto_call
        def foo():
            raise ValueError('')

        with py.test.raises(ValueError):
            foo()


class TestRequest(object):

    @httpretty.activate
    def test_url(self):

        httpretty.register_uri(
            httpretty.GET,
            re.compile("http://localhost:8888/(.*)"),
            body='{}',
            content_type="application/json",
        )
        connection = client.connection(
            client.Endpoint('localhost', 8888, False, 'key', 'secret'),
            True,
        )

        client.request(connection, 'get', '/%7E~', _retries=0)
        server_request = httpretty.last_request()
        assert server_request.path == '/%257E%7E'

    @httpretty.activate
    def test_url_response(self):

        httpretty.register_uri(
            httpretty.GET,
            re.compile("http://localhost:8888/(.*)"),
            body='{"msg": "ok"}',
            content_type="application/json",
        )
        connection = client.connection(
            client.Endpoint('localhost', 8888, False, 'key', 'secret'),
            True,
        )

        result = client.request(connection, 'get', '/%7E~', _retries=0)
        assert result == {'msg': 'ok'}

    @httpretty.activate
    def test_url_bad(self):

        httpretty.register_uri(
            httpretty.GET,
            re.compile("http://localhost:8888/(.*)"),
            body='{}',
            content_type="application/json",
            status=500,
        )
        connection = client.connection(
            client.Endpoint('localhost', 8888, False, 'key', 'secret'),
            True,
        )

        with py.test.raises(client.HttpError):
            client.request(connection, 'get', '/%7E~', _retries=0)


class TestBotoCall(object):

    def test_return_val(self):
        @client.boto_call
        def foo(*args, **kwargs):
            return (args, kwargs)
        assert foo(1) == ((1,), {})
        assert foo(b=2) == (tuple(), {'b': 2})

    def test_boto_exception_not_found(self):
        @client.boto_call
        def foo():
            raise boto.exception.S3ResponseError(404, '')

        with py.test.raises(client.NotFound):
            foo()

    def test_non_boto_exception(self):
        @client.boto_call
        def foo():
            raise ValueError('')

        with py.test.raises(ValueError):
            foo()


class TestGETClientRequests(object):

    def setup(self):
        self.connection = client.connection(
            client.Endpoint('localhost', 8888, False, 'key', 'secret'),
            True,
        )

    def register(self):
        httpretty.register_uri(
            httpretty.GET,
            re.compile("http://localhost:8888/(.*)"),
            body='{}',
            content_type="application/json",
        )

    @httpretty.activate
    def test_get_metadata(self):
        self.register()
        client.get_metadata(self.connection, 'bucket.instance', 'foo')
        server_request = httpretty.last_request()
        assert server_request.path == '/admin/metadata/bucket.instance?key=foo'

    @httpretty.activate
    def test_get_metadata_no_re_encoding(self):
        self.register()
        #client.get_metadata(self.connection, 'bucket.instance', 'mybar%3Ar0z0.4140.1')
        client.get_metadata(self.connection, 'bucket.instance', 'mybar:r0z0.4140.1')
        server_request = httpretty.last_request()
        assert server_request.path == '/admin/metadata/bucket.instance?key=mybar%3Ar0z0.4140.1'

    @httpretty.activate
    def test_get_metadata_sections(self):
        self.register()
        client.get_metadata_sections(self.connection)
        server_request = httpretty.last_request()
        assert server_request.path == '/admin/metadata'

    @httpretty.activate
    def test_list_metadata_keys(self):
        self.register()
        client.list_metadata_keys(self.connection, 'foo')
        server_request = httpretty.last_request()
        assert server_request.path == '/admin/metadata/foo'

    @httpretty.activate
    def test_get_bucket_list(self):
        self.register()
        client.get_bucket_list(self.connection)
        server_request = httpretty.last_request()
        assert server_request.path == '/admin/metadata/bucket'


    @httpretty.activate
    def test_url_response(self):

        httpretty.register_uri(
            httpretty.GET,
            re.compile("http://localhost:8888/(.*)"),
            body='{"msg": "ok"}',
            content_type="application/json",
        )
        result = client.request(self.connection, 'get', '/%7E~')
        assert result == {'msg': 'ok'}
