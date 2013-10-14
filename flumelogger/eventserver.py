#!/usr/bin/env python

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TCompactProtocol
from flumelogger.flumeng import ThriftSourceProtocol
from flumelogger.flumeng.ttypes import ThriftFlumeEvent

class FlumeEventServer(object):
    def __init__(self, host="localhost", port=9090):
        self.socket = TSocket.TSocket(host, port)
        self.socket._timeout = 1000
        self.transport = TTransport.TFramedTransport(self.socket)
        self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
        self.client = ThriftSourceProtocol.Client(self.protocol)
        self.transport.open()

    def append(self, headers, body):
        event = ThriftFlumeEvent(
            headers = headers,
            body = body,
        )
        self.client.append(event)

    def close(self):
        self.transport.close()
