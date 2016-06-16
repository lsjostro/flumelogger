#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ast
import logging
import socket
import time

from datetime import datetime

from flumelogger.eventserver import FlumeEventServer
from flumelogger.flumeng.ttypes import ThriftFlumeEvent as ThriftFlumeNGEvent
from flumelogger.flumeog.ttypes import ThriftFlumeEvent as ThriftFlumeOGEvent

PRIORITY = {"FATAL": 0,
            "CRITICAL": 0,
            "ERROR": 1,
            "WARNING": 2,
            "INFO": 3,
            "DEBUG": 4,
            "TRACE": 5}


class FlumeHandler(logging.Handler):
    def __init__(self, host="localhost", port=9090, timeout=1000, type='ng',
                 reuse=True, debug=False, headers=None):
        if headers is None:
            headers = {}

        # run the regular Handler __init__
        logging.Handler.__init__(self)

        # default header detection
        if 'host' not in headers:
            headers['host'] = socket.gethostname()

        self.host = host
        self.port = port
        self.timeout = timeout
        self.type = type
        self.reuse = reuse
        self.debug = debug
        self.headers = headers
        self.eventserver = FlumeEventServer(host=self.host,
                                            port=self.port,
                                            timeout=self.timeout,
                                            type=self.type,
                                            reuse=self.reuse,
                                            debug=self.debug)
        self.reconnect = self.eventserver.reconnect

    def event_ng(self, body, headers):
        return ThriftFlumeNGEvent(headers=headers, body=body)

    def event_og(self, body, fields):
        pri = PRIORITY[fields['pri']]
        dt = int(time.time() * 1000)
        ns = datetime.now().microsecond * 1000
        host = fields['host']

        del fields['pri']
        del fields['host']

        return ThriftFlumeOGEvent(timestamp=dt,
                                  priority=pri,
                                  body=body,
                                  host=host,
                                  nanos=ns,
                                  fields=fields)

    def parse_record(self, record):
        body = self.format(record)
        headers = self.headers.copy()
        try:
            msg = ast.literal_eval(body)
        except Exception:
            msg = None
        if isinstance(msg, dict):
            if 'message' in msg:
                body = msg['message']
                del msg['message']
            else:
                body = ""
            headers.update(msg)
        headers['pri'] = record.levelname.upper()
        return body, headers

    def emit(self, record):
        if isinstance(record.msg, list):
            self.emit_many(record)
        else:
            self.emit_one(record)

    def emit_one(self, record):
        try:
            event = {'ng': self.event_ng, 'og': self.event_og}
            body, headers = self.parse_record(record)
            try:
                tevent = event[self.type](body, headers)
            except KeyError:
                raise Exception('Wrong flume type specified')

            # send event
            with self.eventserver as client:
                result = self.eventserver.append(tevent, client)
                if result != 0:
                    raise Exception('Thrift error {}'.format(result))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            record.msg = e
            self.handleError(record)

    def emit_many(self, records):
        try:
            events = []
            for obj in records.msg:
                record = logging.LogRecord(name=records.name,
                                           level=records.levelno,
                                           pathname=records.pathname,
                                           lineno=records.lineno,
                                           msg=obj,
                                           args=records.args,
                                           exc_info=records.exc_info)
                event = {'ng': self.event_ng, 'og': self.event_og}
                body, headers = self.parse_record(record)
                try:
                    tevent = event[self.type](body, headers)
                except KeyError:
                    raise Exception('Wrong flume type specified')
                else:
                    events.append(tevent)

            # send events
            with self.eventserver as client:
                result = self.eventserver.append_batch(events, client)
                if result != 0:
                    raise Exception('Thrift error {}'.format(result))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            record = logging.LogRecord(name=records.name,
                                       level=records.levelno,
                                       pathname=records.pathname,
                                       lineno=records.lineno,
                                       msg=e,
                                       args=records.args,
                                       exc_info=records.exc_info)
            self.handleError(record)
