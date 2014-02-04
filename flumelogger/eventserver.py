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
        self.client = None

    def connect(self):
        if self.type == 'ng':
            self.socket = TSocket.TSocket(self.host, self.port)
            self.socket._timeout = 1000
            self.transport = TTransport.TFramedTransport(self.socket)
            self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
            self.client = ThriftSourceProtocol.Client(self.protocol)
        elif self.type == 'og':
            self.socket = TSocket.TSocket(self.host, self.port)
            self.socket._timeout = 1000
            self.transport = TTransport.TBufferedTransport(self.socket)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.client = ThriftFlumeEventServer.Client(self.protocol)
        else:
            raise
        self.transport.open()

    def append(self, event):
        #event = ThriftFlumeEvent(
        #    headers = headers,
        #    body = body,
        #)
        if not self.client:
            self.connect()
        try:
            self.client.append(event)
        except:
            self.close()

    def close(self):
        self.client = None
        self.transport.close()
