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


class TestEventServer(unittest.TestCase):

    def test_connect_failed(self):
        """ Ensure ServerSelectionError is raise when the application start
            and no nodes is available.

            Fix:
                2015-11-27: the default_cycle variable block the exception.
        """
        from flumelogger.eventserver import FlumeEventServer
        from flumelogger.errors import ServerSelectionError

        eventserver = FlumeEventServer(host='localhost:7777')
        with self.assertRaises(ServerSelectionError):
            with eventserver as client:
                pass

    def test_autoreconnect_failed(self):
        """ Ensure the default_nodes keep safe when we remove a node.

            Fix:
                2015-11-27: the default and active nodes variable was link (memory ref).
        """
        from flumelogger.eventserver import FlumeEventServer
        from flumelogger.errors import ServerSelectionError

        eventserver = FlumeEventServer(host='localhost:7777')
        eventserver._remove_node(node=('localhost', 7777))
        self.assertListEqual([('localhost', 7777)], eventserver.default_nodes)

    def test_active_nodes_unicity(self):
        """ Ensure the nodes unicity in the actives nodes list.

            Fix:
                2015-11-27: we use a list and we don't check the presence of the same node into the list.
        """
        from flumelogger.eventserver import FlumeEventServer
        from flumelogger.errors import ServerSelectionError

        eventserver = FlumeEventServer(host='localhost:7777,localhost:8888')
        eventserver._remove_node(node=('localhost', 7777))
        self.assertEqual([('localhost', 8888)], eventserver.active_nodes)


if __name__ == '__main__':
    unittest.main()
