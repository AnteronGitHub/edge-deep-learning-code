"""This module contains the basic transport protocol implementations used by the cluster.
"""
import asyncio
import io
import logging
import pickle
import struct
import uuid

from .module_repo import SparseModule

class SparseTransportProtocol(asyncio.Protocol):
    """Sparse transport protocol implements low-level communication for transmitting dictionary data and files over
    network.
    """
    def __init__(self):
        self.connection_id = str(uuid.uuid4())
        self.logger = logging.getLogger("sparse")
        self.transport = None

        self.data_buffer = io.BytesIO()
        self.receiving_data = False
        self.data_type = None
        self.data_size = 0

    def __str__(self):
        """Returns the peer IP or 'unconnected' if no transport object is yet created.
        """
        return "unconnected" if self.transport is None else self.transport.get_extra_info('peername')[0]

    def clear_buffer(self):
        """Initializes the byte buffer and the related counters.
        """
        self.data_buffer = io.BytesIO()
        self.receiving_data = False
        self.data_type = None
        self.data_size = 0

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data : bytes):
        if self.receiving_data:
            payload = data
        else:
            self.receiving_data = True
            header = data[:9]
            [self.data_type, self.data_size] = struct.unpack("!sQ", header)
            payload = data[9:]

        self.data_buffer.write(payload)

        if self.data_buffer.getbuffer().nbytes >= self.data_size:
            payload_type = self.data_type.decode()
            self.data_buffer.seek(0)
            payload_bytes = self.data_buffer.read(self.data_size)

            if self.data_buffer.getbuffer().nbytes - self.data_size == 0:
                self.clear_buffer()
            else:
                header = self.data_buffer.read(9)
                [self.data_type, self.data_size] = struct.unpack("!sQ", header)

                payload = self.data_buffer.read()
                self.data_buffer = io.BytesIO()
                self.data_buffer.write(payload)

            self.message_received(payload_type, payload_bytes)

    def message_received(self, payload_type : str, data : bytes):
        """Callback function that is triggered when all the bytes specified by a message header been received.
        """
        if payload_type == "f":
            self.file_received(data)
        elif payload_type == "o":
            try:
                self.object_received(pickle.loads(data))
            except pickle.UnpicklingError:
                self.logger.error("Deserialization error. %s payload size, %s buffer size.",
                                  len(data),
                                  self.data_buffer.getbuffer().nbytes)

    def file_received(self, data : bytes):
        """Callback function that is triggered when all the bytes specified by a message header that specifies file
        type message.
        """

    def object_received(self, obj : dict):
        """Callback function that is triggered when all the bytes specified by a message header that specifies object
        type message.
        """

    def send_file(self, file_path : str):
        """Transmits a byte file specified by the file path to the peer.
        """
        with open(file_path, "rb") as f:
            data_bytes = f.read()
            file_size = len(data_bytes)

            self.transport.write(struct.pack("!sQ", b"f", file_size))
            self.transport.write(data_bytes)

    def send_payload(self, payload : dict):
        """Transmits a given object to the peer.
        """
        payload_data = pickle.dumps(payload)
        payload_size = len(payload_data)

        self.transport.write(struct.pack("!sQ", b"o", payload_size))
        self.transport.write(payload_data)

class MultiplexerProtocol(SparseTransportProtocol):
    """Multiplexes protocols into the same network connection. Provides the same external interface as each of the
    multiplexed protocol.
    """
    protocols : set

    def __init__(self, protocols : set = None):
        super().__init__()
        if protocols is None:
            self.protocols = set()
        else:
            self.protocols = protocols

    def connection_made(self, transport):
        for protocol in self.protocols:
            protocol.connection_made(transport)
        super().connection_made(transport)

    def object_received(self, obj : dict):
        for protocol in self.protocols:
            protocol.object_received(obj)

    def file_received(self, data : bytes):
        for protocol in self.protocols:
            protocol.file_received(data)

    def connection_lost(self, exc):
        for protocol in self.protocols:
            protocol.connection_lost(exc)
        super().connection_lost(exc)

class ModuleReceiverProtocol(SparseTransportProtocol):
    """Module receiver receives a Sparse module from another node.
    """
    def __init__(self, module_repo, cluster_orchestrator):
        super().__init__()
        self.module_repo = module_repo
        self.cluster_orchestrator = cluster_orchestrator
        self.receiving_module_name = None

    def object_received(self, obj : dict):
        if obj["op"] == "init_module_transfer" and "status" not in obj:
            module_name = obj["module_name"]

            self.init_module_transfer_received(module_name)

    def init_module_transfer_received(self, module_name : str):
        """Callback for when a module transfer is received.
        """
        if self.receiving_module_name is None:
            self.receiving_module_name = module_name
            self.send_init_module_transfer_ok()
        else:
            self.send_init_module_transfer_error()


    def send_init_module_transfer_ok(self):
        """Replies that a requested module transfer has been initialized successfully.
        """
        self.send_payload({"op": "init_module_transfer", "status": "accepted"})

    def send_init_module_transfer_error(self):
        """Replies that a requested module transfer has failed to initialize.
        """
        self.send_payload({"op": "init_module_transfer", "status": "rejected"})

    def file_received(self, data : bytes):
        app_archive_path = f"/tmp/{self.receiving_module_name}.zip"
        with open(app_archive_path, "wb") as f:
            f.write(data)

        self.logger.info("Received module '%s' from %s", self.receiving_module_name, self)
        module = self.module_repo.add_app_module(self.receiving_module_name, app_archive_path)
        self.cluster_orchestrator.distribute_module(self, module)
        self.receiving_module_name = None

        self.send_transfer_file_ok()

    def send_transfer_file_ok(self):
        """Replies that a file has been transferred successfully.
        """
        self.send_payload({"op": "transfer_file", "status": "success"})

class ModuleSenderProtocol(SparseTransportProtocol):
    """Module sender transfers a Sparse module to another node.
    """
    def __init__(self):
        super().__init__()

        self.transferring_module = None

    def transfer_module(self, module : SparseModule):
        """Starts a module transfer for the specified locally available module.
        """
        self.transferring_module = module

        self.send_init_module_transfer(self.transferring_module.name)

    def send_init_module_transfer(self, module_name : str):
        """Initiates a module transfer to a peer.
        """
        self.send_payload({ "op": "init_module_transfer", "module_name": module_name })

    def object_received(self, obj : dict):
        if obj["op"] == "init_module_transfer" and "status" in obj:
            if obj["status"] == "accepted":
                self.init_module_transfer_ok_received()
            else:
                self.init_module_transfer_error_received()
        elif obj["op"] == "transfer_file" and "status" in obj:
            if obj["status"] == "success":
                self.transfer_file_ok_received()

    def init_module_transfer_ok_received(self):
        """Callback for when a module transfer has been acknowledged to have succeeded by the peer.
        """
        self.send_file(self.transferring_module.zip_path)

    def init_module_transfer_error_received(self):
        """Callback for when a module transfer has been acknowledged to be failed by the peer.
        """
        self.logger.error("Module transfer initialization failed")

    def transfer_file_ok_received(self):
        """Callback for when a file transfer has been acknowledged by the peer.
        """

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

class StreamPublishProtocol(SparseTransportProtocol):
    """Stream publish protocol receives subscriptions to streams, and publishes new tuples to the subscribed nodes.
    """
    def __init__(self, stream_data_sender_protocol, stream_router):
        super().__init__()
        self.stream_data_sender_protocol = stream_data_sender_protocol
        self.stream_router = stream_router

    def object_received(self, obj : dict):
        if obj["op"] == "subscribe" and "status" not in obj:
            stream_alias = obj["stream_alias"]
            self.subscribe_received(stream_alias)

    def subscribe_received(self, stream_alias : str):
        """Callback triggered when a stream subscription is received.
        """
        self.stream_router.subscribe(stream_alias, self.stream_data_sender_protocol)
        self.send_subscribe_ok(stream_alias)

    def send_subscribe_ok(self, stream_alias : str):
        """Replies that a requested stream subscription was successful.
        """
        self.send_payload({"op": "subscribe", "stream_alias": stream_alias, "status": "success"})

    def send_subscribe_error(self, stream_alias : str):
        """Replies that a requested stream subscription failed.
        """
        self.send_payload({"op": "subscribe", "stream_alias": stream_alias, "status": "error"})

class StreamSubscribeProtocol(SparseTransportProtocol):
    """Stream subscribe protocol subscribes to streams in other cluster nodes to new tuples.
    """
    def __init__(self, on_subscribe_ok_received = None):
        super().__init__()
        self.on_subscribe_ok_received = on_subscribe_ok_received

    def send_subscribe(self, stream_alias : str):
        """Initiates a stream subscription to a given stream alias.
        """
        self.send_payload({"op": "subscribe", "stream_alias": stream_alias})

    def object_received(self, obj : dict):
        if obj["op"] == "subscribe" and "status" in obj:
            stream_alias = obj["stream_alias"]
            if obj["status"] == "success":
                if self.on_subscribe_ok_received is not None:
                    self.on_subscribe_ok_received(stream_alias)
            else:
                self.subscribe_error_received(stream_alias)

    def subscribe_error_received(self, stream_alias : str):
        """Callback for when a requested stream has been failed.
        """

class StreamMigratorProtocol(SparseTransportProtocol):
    """Stream migrator protocol transfers information about an existing stream to another node.
    """
    def __init__(self, on_create_connector_stream_ok_received = None):
        super().__init__()
        self.on_create_connector_stream_ok_received = on_create_connector_stream_ok_received

    def send_create_connector_stream(self, stream_id : str = None, stream_alias : str = None):
        """Propagates a stream to the peer.
        """
        self.logger.info("Migrating stream %s to peer %s", stream_id, self)
        self.send_payload({"op": "create_connector_stream", \
                           "stream_id": stream_id, \
                           "stream_alias": stream_alias})

    def object_received(self, obj : dict):
        if obj["op"] == "create_connector_stream" and "status" in obj:
            if obj["status"] == "success":
                stream_id = obj["stream_id"]
                stream_alias = obj["stream_alias"]

                if self.on_create_connector_stream_ok_received is not None:
                    self.on_create_connector_stream_ok_received(stream_id, stream_alias)

class StreamReceiverProtocol(SparseTransportProtocol):
    """Stream receiver protocol receives streams created in other nodes.
    """
    def __init__(self, on_create_connector_stream_received = None):
        super().__init__()
        self.on_create_connector_stream_received = on_create_connector_stream_received

    def object_received(self, obj : dict):
        """Callback for handling the received messages.
        """
        if obj["op"] == "create_connector_stream" and "status" not in obj:
            stream_id = obj["stream_id"] if "stream_id" in obj.keys() else None
            stream_alias = obj["stream_alias"] if "stream_alias" in obj.keys() else None

            if self.on_create_connector_stream_received is not None:
                self.on_create_connector_stream_received(stream_id, stream_alias)
            self.send_create_connector_stream_ok(stream_id, stream_alias)

    def send_create_connector_stream_ok(self, stream_id : str, stream_alias : str):
        """Replies to the sender that a stream connector was successfully.
        """
        self.send_payload({"op": "create_connector_stream",
                           "status": "success",
                           "stream_id": stream_id,
                           "stream_alias" : stream_alias})

from .stream.protocols import StreamDataReceiverProtocol, StreamDataSenderProtocol
from .deployment.protocols import DeploymentServerProtocol

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
