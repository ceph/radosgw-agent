import socket


def is_ipv6(address):
    """
    Check if an address is an IPV6 one, but trim commonly used brackets as the
    ``socket`` module complains about them.
    """
    if not isinstance(address, str):
        return False

    if address.startswith('['):  # assume we need to split on possible port
        address = address.split(']:')[0]
    # strip leading/trailing brackets so inet_pton understands the address
    address = address.strip('[]')
    try:
        socket.inet_pton(socket.AF_INET6, address)
    except socket.error:  # not a valid address
        return False
    return True
