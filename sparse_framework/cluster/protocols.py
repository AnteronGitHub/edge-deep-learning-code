"""This module includes functionality related to cluster state communication.
"""
import asyncio
from ..protocols import SparseTransportProtocol, MultiplexerProtocol
from ..stream.protocols import StreamDataReceiverProtocol, \
                              StreamDataSenderProtocol, \
                              StreamPublishProtocol, \
                              StreamMigratorProtocol, \
                              StreamReceiverProtocol
from ..module import SparseModule
from ..module.protocols import ModuleReceiverProtocol, ModuleSenderProtocol
from ..deployment.protocols import DeploymentServerProtocol

class ChildNodeProtocol(SparseTransportProtocol):
    """Child node creates an egress connection to another cluster node.
    """
    def __init__(self, cluster_protocol, cluster_orchestrator):
        super().__init__()
        self.cluster_protocol = cluster_protocol
        self.cluster_orchestrator = cluster_orchestrator

    def send_connect_downstream(self):
        """Initiates a downstream connection.
        """
        self.logger.info("Connecting to cluster parent %s", self)
        self.send_payload({"op": "connect_downstream"})

    def object_received(self, obj : dict):
        if obj["op"] == "connect_downstream" and "status" in obj:
            if obj["status"] == "success":
                self.connect_downstream_ok_received()

    def connect_downstream_ok_received(self):
        """Callback for when a downstream connection has been acknowledged successful.
        """
        self.cluster_orchestrator.add_cluster_connection(self.cluster_protocol, direction="egress")

class ParentNodeProtocol(SparseTransportProtocol):
    """Parent node receives an ingress connection from another cluster node.
    """
    def __init__(self, cluster_protocol, cluster_orchestrator):
        super().__init__()
        self.cluster_protocol = cluster_protocol
        self.cluster_orchestrator = cluster_orchestrator

    def object_received(self, obj : dict):
        if obj["op"] == "connect_downstream" and "status" not in obj:
            self.connect_downstream_received()

    def connect_downstream_received(self):
        """Callback for when a new downstream connection request has been received.
        """
        self.cluster_orchestrator.add_cluster_connection(self.cluster_protocol, "ingress")
        self.send_connect_downstream_ok()

    def send_connect_downstream_ok(self):
        """Replies that the peer has connected as a downstream.
        """
        self.send_payload({"op": "connect_downstream", "status": "success"})

class ClusterProtocol(MultiplexerProtocol):
    """Super class for cluster node transport protocols.
    """
    def __init__(self, node):
        self.node = node
        self.module_sender_protocol = ModuleSenderProtocol()
        self.stream_migrator_protocol = StreamMigratorProtocol()
        self.stream_data_sender_protocol = StreamDataSenderProtocol()

        super().__init__({ DeploymentServerProtocol(node.cluster_orchestrator.create_deployment),
                           ModuleReceiverProtocol(node.module_repo, node.cluster_orchestrator),
                           StreamDataReceiverProtocol(self.node.stream_router.tuple_received),
                           StreamReceiverProtocol(self.create_connector_stream_received),
                           StreamPublishProtocol(self.stream_data_sender_protocol, node.stream_router),
                           self.module_sender_protocol,
                           self.stream_migrator_protocol,
                           self.stream_data_sender_protocol })

    def transfer_module(self, module : SparseModule):
        """Transfers module to peer.
        """
        self.module_sender_protocol.transfer_module(module)

    def send_create_connector_stream(self, stream_id : str = None, stream_alias : str = None):
        """Migrates stream to peer.
        """
        self.stream_migrator_protocol.send_create_connector_stream(stream_id, stream_alias)

    def create_connector_stream_received(self, stream_id : str = None, stream_alias : str = None):
        """Callback triggered when a stream has been received.
        """
        stream = self.node.stream_router.create_connector_stream(self, stream_id, stream_alias)
        self.node.cluster_orchestrator.distribute_stream(self, stream)

    def connection_lost(self, exc):
        self.node.cluster_orchestrator.remove_cluster_connection(self.transport)
        self.logger.debug("Connection %s disconnected.", self)

class ClusterClientProtocol(ClusterProtocol):
    """Cluster client protocol creates an egress connection to another cluster node.
    """
    def __init__(self, on_con_lost : asyncio.Future, node, *args, **kwargs):
        super().__init__(node, *args, **kwargs)

        self.child_node_protocol = ChildNodeProtocol(self, node.cluster_orchestrator)

        self.on_con_lost = on_con_lost

        self.protocols.add(self.child_node_protocol)

    def connection_made(self, transport):
        super().connection_made(transport)

        self.child_node_protocol.send_connect_downstream()

class ClusterServerProtocol(ClusterProtocol):
    """Cluster client protocol creates an ingress connection to another cluster node.
    """
    def __init__(self, node, *args, **kwargs):
        super().__init__(node, *args, **kwargs)

        self.protocols.add(ParentNodeProtocol(self, node.cluster_orchestrator))
