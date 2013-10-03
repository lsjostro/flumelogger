import logging
import time
import socket
from datetime import datetime
from flumelogger.eventserver import FlumeEventServer

class FlumeHandler(logging.Handler):
    def __init__(self, host="localhost", port=9090, fields={}):
        # run the regular Handler __init__
        logging.Handler.__init__(self)

        self.host = host
        self.port = port
        self.fields = fields

    def emit(self, record):
        try:
            evt = FlumeEventServer(host=self.host, port=self.port)
            pri = FlumeEventServer.PRIORITY[record.levelname.upper()]
            dt = int(time.time() * 1000)
            ns = datetime.now().microsecond * 1000
            # record is the log message
            evt.append(pri, self.format(record), socket.gethostname(), dt, ns, fields=self.fields)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
