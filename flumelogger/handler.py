import socket
import logging
import time
import ast
from datetime import datetime
from flumelogger.eventserver import FlumeEventServer
from flumelogger.flumeng.ttypes import ThriftFlumeEvent as ThriftFlumeNGEvent
from flumelogger.flumeog.ttypes import ThriftFlumeEvent as ThriftFlumeOGEvent

PRIORITY = { "FATAL"   : 0,
             "CRITICAL": 0,
             "ERROR"   : 1,
             "WARNING" : 2,
             "INFO"    : 3,
             "DEBUG"   : 4,
             "TRACE"   : 5 }

class FlumeHandler(logging.Handler):
    def __init__(self, host="localhost", port=9090, type='ng', headers={}):
        # run the regular Handler __init__
        logging.Handler.__init__(self)

        # default header detection
        if 'host' not in headers:
            headers['host'] = socket.gethostname()

        self.host = host
        self.port = port
        self.type = type
        self.headers = headers
        self.eventserver = FlumeEventServer(host=self.host, port=self.port, type=self.type)

    def event_ng(self, headers):
        self.event = ThriftFlumeNGEvent(
            headers = headers,
            body = self.body
        )

    def event_og(self, fields):
        pri = PRIORITY[fields['pri']]
        dt = int(time.time() * 1000)
        ns = datetime.now().microsecond * 1000
        host = fields['host']

        del fields['pri']
        del fields['host']

        self.event = ThriftFlumeOGEvent(
            timestamp = dt,
            priority = pri,
            body = self.body,
            host = host,
            nanos = ns,
            fields = fields
        )

    def emit(self, record):
        try:
            self.body = self.format(record)
            try:
                msg = ast.literal_eval(self.body)
            except:
                msg = None

            headers = self.headers.copy()
            if isinstance(msg, dict):
                if 'message' in msg:
                    self.body = msg['message']
                    del msg['message']
                else:
                    self.body = ""
                headers.update(msg)

            event = { 'ng': self.event_ng,
                      'og': self.event_og }
            headers['pri'] = record.levelname.upper()
            try:
                event[self.type](headers)
            except KeyError:
                raise Exception('Wrong flume type specified')

            # send event
            self.eventserver.append(self.event)

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
