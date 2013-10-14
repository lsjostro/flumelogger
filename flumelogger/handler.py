import socket
import logging
from flumelogger.eventserver import FlumeEventServer

class FlumeHandler(logging.Handler):
    def __init__(self, host="localhost", port=9090, headers={}):
        # run the regular Handler __init__
        logging.Handler.__init__(self)

        self.host = host
        self.port = port
        self.headers = headers
        if not self.headers.has_key('source_host'):
            self.headers['source_host'] = socket.gethostname()

    def emit(self, record):
        try:
            evt = FlumeEventServer(host=self.host, port=self.port)
            self.headers['pri'] = record.levelname.upper()
            # record is the log message
            evt.append(self.headers, self.format(record))
            evt.close()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
