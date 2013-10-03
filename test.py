#!/usr/bin/env python
# test.py
import logging
from flumelogger import handler

# Create a logging object (after configuring logging)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test")

# and finally we add the handler to the logging object
logger.addHandler(handler.FlumeHandler(fields={'application': 'myTestApp' ,'somefield': 'foobar'}))

# And finally a test
logger.debug('Hello debug')
logger.info('Hello info')
logger.warning('Hello warn')
logger.error('Hello error')
logger.critical('Hello critical')
