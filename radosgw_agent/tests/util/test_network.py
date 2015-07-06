import pytest
from radosgw_agent.util import network
import random


def valid_ipv6_addr(ports=False, brackets=False, addresses=20):
    max_rand_int = 16**4

    def generate(brackets, ports):
        address = ":".join(
            ("%x" % random.randint(0, max_rand_int) for i in range(8))
        )
        if brackets:
            address =  '[%s]' % address
        if ports:
            address = '%s:8080' % address
        return address
    return [generate(brackets, ports) for i in range(addresses)]


def invalid_ipv6_addr():
    return [
        '',
        1,
        'some address',
        '192.1.1.1',
        '::!',
    ]


class TestIsIPV6(object):

    @pytest.mark.parametrize('address', valid_ipv6_addr())
    def test_passes_valid_addresses(self, address):
        assert network.is_ipv6(address) is True

    @pytest.mark.parametrize('address', valid_ipv6_addr(brackets=True))
    def test_passes_valid_addresses_with_brackets(self, address):
        assert network.is_ipv6(address) is True

    @pytest.mark.parametrize('address', invalid_ipv6_addr())
    def test_catches_invalid_addresses(self, address):
        assert network.is_ipv6(address) is False

    @pytest.mark.parametrize('address', valid_ipv6_addr(ports=True, brackets=True))
    def test_passes_valid_addresses_with_brackets_and_ports(self, address):
        assert network.is_ipv6(address) is True

