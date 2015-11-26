# -*- coding: utf-8 -*-
import re

from flumelogger.errors import ConfigurationError


def parse_host(entity, default_port):
    """@Author: pymongo driver

    Validates a host string

    Returns a 2-tuple of host followed by port where port is default_port
    if it wasn't specified in the string.

    :Parameters:
        - `entity`: A host or host:port string where host could be a
                    hostname or IP address.
        - `default_port`: The port number to use when one wasn't
                          specified in entity.
    """
    host = entity
    port = default_port
    if entity.find(':') != -1:
        if entity.count(':') > 1:
            raise ValueError("Reserved characters such as ':' must be "
                             "escaped according RFC 2396. An IPv6 "
                             "address literal must be enclosed in '[' "
                             "and ']' according to RFC 2732.")
        host, port = host.split(':', 1)
    if isinstance(port, str):
        if not port.isdigit() or int(port) > 65535 or int(port) <= 0:
            raise ValueError("Port must be an integer between 0 and 65535: %s"
                             % (port,))
        port = int(port)

    # Normalize hostname to lowercase, since DNS is case-insensitive:
    # http://tools.ietf.org/html/rfc4343
    # This prevents useless rediscovery if "foo.com" is in the node list but
    # "FOO.com" is in the ismaster response.
    return host.lower(), port

def split_hosts(hosts, default_port):
    """@Author: pymongo driver

    takes a string of the form host1[:port],host2[:port]... and
    splits it into (host, port) tuples. if [:port] isn't present the
    default_port is used.

    returns a set of 2-tuples containing the host name (or ip) followed by
    port number.

    :parameters:
        - `hosts`: a string of the form host1[:port],host2[:port],...
        - `default_port`: the port number to use when one wasn't specified
          for a host.
    """
    nodes = []
    for entity in hosts.split(','):
        if not entity:
            raise ConfigurationError("empty host "
                "(or extra comma in host list).")
        port = default_port
        nodes.append(parse_host(entity, port))
    return nodes

def log_debug(msg, debug):
    """ Simple helper to display message to avoid logging module.

    Args:
        msg (str): the message to display.
        debug (bool): True display message, False otherwise.
    """
    if debug:
        print msg
