#!/usr/bin/env python

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TCompactProtocol
from flumelogger.flumeng import ThriftSourceProtocol
from flumelogger.flumeng.ttypes import ThriftFlumeEvent

class FlumeEventServer(object):
    PRIORITY = { "FATAL"   : 0,
                 "CRITICAL": 0,
                 "ERROR"   : 1,
                 "WARNING" : 2,
                 "INFO"    : 3,
                 "DEBUG"   : 4,
                 "TRACE"   : 5 }

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
        print event
        self.client.append(event)

    def close(self):
        self.transport.close()
