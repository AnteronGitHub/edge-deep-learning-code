"""This module includes cluster orchestrator unit tests.
"""
import logging
import unittest
from unittest.mock import Mock

from ..node import SparseNode

from .protocols import ClusterServerProtocol

class TestClusterOrchestrator(unittest.TestCase):
    """This class implements cluster orchestrator unit tests.
    """
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.node = SparseNode()

    def test_when_new_cluster_connection_is_created_cluster_data_is_synchronized(self):
        """When a new node is added to the cluster, existing streams and modules should be sent to the new node.
        """
        self.node.stream_repository.get_stream()
        self.node.module_repo.add_app_module("TestModule", "test_module_path")
        protocol = ClusterServerProtocol(self.node)
        protocol.send_create_connector_stream = Mock()
        protocol.module_sender_protocol.transfer_module = Mock()

        self.node.cluster_orchestrator.add_cluster_connection(protocol, direction="ingress")

        protocol.send_create_connector_stream.assert_called()
        protocol.module_sender_protocol.transfer_module.assert_called()
