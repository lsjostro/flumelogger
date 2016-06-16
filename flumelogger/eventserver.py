#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import copy
from random import shuffle

from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from flumelogger.errors import (
    ConfigurationError,
    ConnectionFailure,
    ServerSelectionError,
    OperationFailure)
from flumelogger.flumeng import ThriftSourceProtocol
from flumelogger.flumeog import ThriftFlumeEventServer
from flumelogger.utils import log_debug
from flumelogger.utils import split_hosts


class FlumeEventServer(object):
    def __init__(self, host="localhost", port=9090, timeout=1000, type='ng', reuse=True, debug=False):
        """ Init the event server stuff (nodes and co).

        Args:
            host (Optional[str or list]): hostname or IP address, string or a list of hostname.
            port (Optional[int]): port number on which to connect.
            timeout (Optional[int]): Controls how long the socket will wait for a response.
            type (Optional[str]): The flume agent generation.
            reuse (bool): using a single TCP connection, otherwhise open/close connection for each operation.
            debug (bool): True show debug messages, otherwhise is quiet.
        """
        self.host = host
        if isinstance(self.host, basestring):
            self.host = [self.host]
        self.port = port
        self.timeout = timeout
        self.type = type
        self.reuse = reuse
        self.debug = debug

        # NOTE: parse the host URI.
        for entity in self.host:
            self.default_nodes = split_hosts(entity, self.port)
            log_debug("nodes available {0}".format(self.default_nodes), debug=self.debug)

        if not self.default_nodes:
            raise ConfigurationError("need to specify at least one host")

        # NOTE: init stuff to manage the pool.
        self.active_nodes = [node for node in self.default_nodes]
        self.open_connections = {}

    def _connect(self, host, port):
        """ Connect to the thrift source.

        Args:
            host (str): hostname or IP address.
            port (int): port number on which to connect.

        Returns:
            client, transport: The client/transport from thrift.

        Raises:
            ConnectionFailure: connection to the thrift source cannot be made os is lost.
        """
        try:
            socket = TSocket.TSocket(host, port)
            socket._timeout = self.timeout
            if self.type == 'ng':
                transport = TTransport.TFramedTransport(socket)
                protocol = TCompactProtocol.TCompactProtocol(transport)
                client = ThriftSourceProtocol.Client(protocol)
            elif self.type == 'og':
                transport = TTransport.TBufferedTransport(socket)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = ThriftFlumeEventServer.Client(protocol)
            transport.open()
        except Exception as e:
            raise ConnectionFailure(e)
        else:
            return client, transport

    def _is_connected(self, node):
        """ Check if a TCP connection is already opened.

        Args:
            host (str): Hostname/port combo.

        Returns:
            bool: True if an active connection is available, False otherwise.
        """
        return node in self.open_connections and self.open_connections[node]['transport'].isOpen()

    def _remove_node(self, node):
        """ Remove a node from the active nodes pool.

        Args:
            host (str): Hostname/port combo.
        """
        log_debug("remove {0} from the active nodes".format(node), debug=self.debug)
        self.open_connections.pop(node, None)
        if node in self.active_nodes:
            self.active_nodes.remove(node)

    def _add_node(self, node, client, transport):
        """ Add a node to the active nodes pool.

        Args:
            host (str): Hostname/port combo.
        """
        log_debug("add {0} to the active nodes".format(node), debug=self.debug)
        self.open_connections[node] = {'client': client, 'transport': transport}
        if node not in self.active_nodes:
            self.active_nodes.append(node)

    def __enter__(self):
        """ Connect to an active node.
        """
        try:
            if not self.active_nodes:
                raise StopIteration

            shuffle(self.active_nodes)
            for node in copy(self.active_nodes):
                try:
                    if not self.active_nodes:
                        raise StopIteration

                    # NOTE: try to re-use a connection.
                    self.current_node = node
                    if self.reuse and self._is_connected(node=self.current_node):
                        log_debug('re-using connection to {0}'.format(self.current_node), debug=self.debug)
                        return self.open_connections[self.current_node]['client']

                    # NOTE: new connection.
                    self.client, self.transport = self._connect(*self.current_node)
                    self._add_node(node=self.current_node, client=self.client, transport=self.transport)
                    log_debug('opened new connection to {0}'.format(self.current_node), debug=self.debug)
                except ConnectionFailure as e:
                    self._remove_node(node=self.current_node)
                else:
                    return self.client
        except StopIteration:
            raise ServerSelectionError("no server available")

    def __exit__(self, type, value, unpack):
        """ Ensure to close the transport connection after each operation.
        """
        if not self.reuse and hasattr(self, "transport"):
            self.transport.close()

    def reconnect(self):
        """ Helper will attempt to reconnect on nodes.
        """
        for node in self.default_nodes:
            try:
                if node not in self.active_nodes:
                    log_debug('reconnecting to {0}'.format(node), debug=self.debug)
                    client, transport = self._connect(*node)
                    self._add_node(node=node, client=client, transport=transport)
            except ConnectionFailure as e:
                log_debug('failed to reconnect to {0}'.format(node), debug=self.debug)

    def append(self, event, client):
        try:
            return client.append(event)
        except Exception as e:
            self._remove_node(node=self.current_node)
            raise OperationFailure(e)

    def append_batch(self, events, client):
        try:
            return client.appendBatch(events)
        except Exception as e:
            self._remove_node(node=self.current_node)
            raise OperationFailure(e)
