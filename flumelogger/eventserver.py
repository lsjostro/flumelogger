#!/usr/bin/env python

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from flumelogger.flumeng import ThriftSourceProtocol
from flumelogger.flumeog import ThriftFlumeEventServer


class FlumeEventServer(object):
    def __init__(self, host="localhost", port=9090, type='ng'):
        self.host = host
        self.port = port
        self.type = type
        self.transport = None
        self.client = None

    def connect(self):
        socket = TSocket.TSocket(self.host, self.port)
        socket._timeout = 1000
        if self.type == 'ng':
            self.transport = TTransport.TFramedTransport(socket)
            protocol = TCompactProtocol.TCompactProtocol(self.transport)
            self.client = ThriftSourceProtocol.Client(protocol)
        elif self.type == 'og':
            self.transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.client = ThriftFlumeEventServer.Client(protocol)
        self.transport.open()

    def append(self, event):
        try:
            if self.client is None:
                self.connect()

            self.client.append(event)
        except Exception, tx:
            self.client = None
            self.transport.close()
            raise Exception('Thrift: %s' % tx)
