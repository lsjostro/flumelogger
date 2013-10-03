#!/usr/bin/env python
# test.py
import logging
from flumelogger import handler

# Create a logging object (after configuring logging)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test")

# and finally we add the handler to the logging object
logger.addHandler(handler.FlumeHandler(host='elastic01.horisont.svenskaspel.se',
                                       fields={"application": "myTestApp" ,"tpns": "itp1"}))

# And finally a test
logger.debug('kingen debug')
logger.info('kingen info')
logger.warning('kingen warn')
logger.error('kingen error')
logger.critical('kingen crit')
