#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

class TestEventServerParsing(unittest.TestCase):

    def test_default(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer()
        self.assertEqual(eventserver.active_nodes, [('localhost', 9090)])

    def test_port(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer(port=7777)
        self.assertEqual(eventserver.active_nodes, [('localhost', 7777)])

    def test_multiple_host(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer(host='node1,node2')
        self.assertEqual(eventserver.active_nodes, [('node1', 9090), ('node2', 9090)])

    def test_multiple_host_port(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer(host='node1,node2:7777')
        self.assertEqual(eventserver.active_nodes, [('node1', 9090), ('node2', 7777)])

    def test_multiple_same_port(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer(host='node1,node1:7777')
        self.assertEqual(eventserver.active_nodes, [('node1', 9090), ('node1', 7777)])

    def test_missing_host(self):
        from flumelogger.eventserver import FlumeEventServer
        from flumelogger.errors import ConfigurationError
        with self.assertRaises(ConfigurationError):
            eventserver = FlumeEventServer(host="")

    def test_list_host(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer(host=['agent1'])
        self.assertEqual(eventserver.active_nodes, [('agent1', 9090)])

    def test_list_host_port(self):
        from flumelogger.eventserver import FlumeEventServer
        eventserver = FlumeEventServer(host=['agent1:7777'])
        self.assertEqual(eventserver.active_nodes, [('agent1', 7777)])

if __name__ == '__main__':
    unittest.main()
