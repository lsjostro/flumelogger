flumelogger
===========

Python library for sending log events to flume.

### Requirements

  * pip install thrift

### Usage

```
>>> import logging
>>> from flumelogger import handler

>>> fh = handler.FlumeHandler(host='my-flume-agent.example.com', port=9090, 
...                           fields={'application': 'Skyline.Analyzer'})
>>> logger = logging.getLogger("AnalyzerLog")
>>> logger.setLevel(logging.DEBUG)
>>> logger.addHandler(fh)

>>> logger.info("python is cool")
>>>
```

