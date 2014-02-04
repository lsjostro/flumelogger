import socket
import logging
import time
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

        self.host = host
        self.port = port
        self.type = type
        self.headers = headers
        self.eventserver = FlumeEventServer(host=self.host, port=self.port, type=self.type)

    def event_ng(self):
        self.event = ThriftFlumeNGEvent(
            body = self.body,
            headers = self.headers
        )

    def event_og(self):
        pri = PRIORITY[self.headers['pri']]
        dt = int(time.time() * 1000)
        ns = datetime.now().microsecond * 1000
        host = self.headers['host']

        del self.headers['pri']
        del self.headers['host']

        self.event = ThriftFlumeOGEvent(
            timestamp = dt,
            priority = pri,
            body = self.body,
            host = host,
            nanos = ns,
            fields = self.headers
        )

    def emit(self, record):
        try:
            self.body = self.format(record)

            try:
                msg = eval(self.body)
            except SyntaxError:
                msg = None

            if isinstance(msg, dict):
                if msg.has_key('message'):
                    self.body = msg['message']
                    del msg['message']
                else:
                    self.body = ""
                self.headers = msg

            if not self.headers.has_key('host'):
                self.headers['host'] = socket.gethostname()
            self.headers['pri'] = record.levelname.upper()

            event = { 'ng': self.event_ng,
                      'og': self.event_og }
            event[self.type]()

            # record is the log message
            self.eventserver.append(self.event)

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
