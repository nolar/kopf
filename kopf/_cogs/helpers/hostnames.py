import ipaddress
import socket
from typing import List, Optional, Tuple


def get_descriptive_hostname() -> str:
    """
    Look for non-numeric hostnames of the machine where the operator runs.

    The purpose is the host identification, not the actual host accessability.

    Similar to :func:`socket.getfqdn`, but IPv6 pseudo-hostnames are excluded --
    they are not helpful in identifying the actual host running the operator:
    e.g. "1.0.0...0.ip6.arpa".
    """
    try:
        hostname, aliases, ipaddrs = socket.gethostbyaddr(socket.gethostname())
    except socket.error:
        pass
    else:
        ipv4: Optional[ipaddress.IPv4Address]
        ipv6: Optional[ipaddress.IPv6Address]
        parsed: List[Tuple[str, Optional[ipaddress.IPv4Address], Optional[ipaddress.IPv6Address]]]
        parsed = []
        for name in [hostname] + list(aliases) + list(ipaddrs):
            try:
                ipv4 = ipaddress.IPv4Address(name)
            except ipaddress.AddressValueError:
                ipv4 = None
            try:
                ipv6 = ipaddress.IPv6Address(name)
            except ipaddress.AddressValueError:
                ipv6 = None
            parsed.append((name, ipv4, ipv6))

        # Dotted hostname (fqdn) is always better, unless it is an ARPA-name or an IP-address.
        for name, ipv4, ipv6 in parsed:
            if '.' in name and not name.endswith('.arpa') and not ipv4 and not ipv6:
                return remove_useless_suffixes(name)

        # Non-dotted hostname is fine too, unless it is ARPA-name/IP-address or a localhost.
        for name, ipv4, ipv6 in parsed:
            if name != 'localhost' and not name.endswith('.arpa') and not ipv4 and not ipv6:
                return remove_useless_suffixes(name)

    return remove_useless_suffixes(socket.gethostname())


def remove_useless_suffixes(hostname: str) -> str:
    suffixes = ['.local', '.localdomain']
    while any(hostname.endswith(suffix) for suffix in suffixes):
        for suffix in suffixes:
            if hostname.endswith(suffix):
                hostname = hostname[:-len(suffix)]
    return hostname
