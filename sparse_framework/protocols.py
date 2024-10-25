import asyncio
import io
import logging
import pickle
import struct
import uuid

from .deployment import Deployment
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
        return "unconnected" if self.transport is None else self.transport.get_extra_info('peername')[0]

    def clear_buffer(self):
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
        pass

    def object_received(self, obj : dict):
        pass

    def send_file(self, file_path):
        with open(file_path, "rb") as f:
            data_bytes = f.read()
            file_size = len(data_bytes)

            self.transport.write(struct.pack("!sQ", b"f", file_size))
            self.transport.write(data_bytes)

    def send_payload(self, payload : dict):
        payload_data = pickle.dumps(payload)
        payload_size = len(payload_data)

        self.transport.write(struct.pack("!sQ", b"o", payload_size))
        self.transport.write(payload_data)

class SparseProtocol(SparseTransportProtocol):
    """Class includes application level messages used by sparse nodes.
    """
    def send_create_deployment(self, deployment : Deployment):
        self.send_payload({"op": "create_deployment", "deployment": deployment})

    def create_deployment_received(self, deployment : Deployment):
        pass

    def send_create_deployment_ok(self):
        self.send_payload({"op": "create_deployment", "status": "success"})

    def send_create_connector_stream(self, stream_id : str = None, stream_alias : str = None):
        self.send_payload({"op": "create_connector_stream", \
                           "stream_id": stream_id, \
                           "stream_alias": stream_alias})

    def create_connector_stream_received(self, stream_id : str = None, stream_alias : str = None):
        pass

    def send_create_connector_stream_ok(self, stream_id : str, stream_alias : str):
        self.send_payload({"op": "create_connector_stream",
                           "status": "success",
                           "stream_id": stream_id,
                           "stream_alias" : stream_alias})

    def create_connector_stream_ok_received(self, stream_id : str, stream_alias : str):
        pass

    def send_subscribe(self, stream_alias : str):
        self.send_payload({"op": "subscribe", "stream_alias": stream_alias})

    def subscribe_received(self, stream_alias : str):
        pass

    def send_subscribe_ok(self, stream_alias : str):
        self.send_payload({"op": "subscribe", "stream_alias": stream_alias, "status": "success"})

    def subscribe_ok_received(self, stream_alias : str):
        pass

    def send_subscribe_error(self, stream_alias : str):
        self.send_payload({"op": "subscribe", "stream_alias": stream_alias, "status": "error"})

    def subscribe_error_received(self, stream_alias : str):
        pass

    def send_data_tuple(self, stream, data_tuple):
        self.send_payload({"op": "data_tuple", "stream_selector": stream.__str__(), "tuple": data_tuple })

    def data_tuple_received(self, stream_selector : str, data_tuple : str):
        pass

    def send_init_module_transfer(self, module_name : str):
        self.send_payload({ "op": "init_module_transfer", "module_name": module_name })

    def init_module_transfer_received(self):
        pass

    def send_init_module_transfer_ok(self):
        self.send_payload({"op": "init_module_transfer", "status": "accepted"})

    def send_init_module_transfer_error(self):
        self.send_payload({"op": "init_module_transfer", "status": "rejected"})

    def send_transfer_file_ok(self):
        self.send_payload({"op": "transfer_file", "status": "success"})

    def transfer_file_ok_received(self):
        pass

    def send_connect_downstream(self):
        self.send_payload({"op": "connect_downstream"})

    def send_connect_downstream_ok(self):
        self.send_payload({"op": "connect_downstream", "status": "success"})

    def object_received(self, obj : dict):
        if obj["op"] == "connect_downstream":
            if "status" in obj:
                if obj["status"] == "success":
                    self.connect_downstream_ok_received()
                else:
                    pass
            else:
                self.connect_downstream_received()
        elif obj["op"] == "create_connector_stream":
            if "status" in obj:
                if obj["status"] == "success":
                    stream_id = obj["stream_id"]
                    stream_alias = obj["stream_alias"]

                    self.create_connector_stream_ok_received(stream_id, stream_alias)
                else:
                    pass
            else:
                stream_id = obj["stream_id"] if "stream_id" in obj.keys() else None
                stream_alias = obj["stream_alias"] if "stream_alias" in obj.keys() else None

                self.create_connector_stream_received(stream_id, stream_alias)
        elif obj["op"] == "subscribe":
            stream_alias = obj["stream_alias"]
            if "status" in obj:
                if obj["status"] == "success":
                    self.subscribe_ok_received(stream_alias)
                else:
                    self.subscribe_error_received(stream_alias)
            else:
                self.subscribe_received(stream_alias)
        elif obj["op"] == "init_module_transfer":
            if "status" in obj:
                if obj["status"] == "accepted":
                    self.init_module_transfer_ok_received()
                else:
                    self.init_module_transfer_error_received()
            else:
                module_name = obj["module_name"]

                self.init_module_transfer_received(module_name)
        elif obj["op"] == "transfer_file":
            if obj["status"] == "success":
                self.transfer_file_ok_received()
        elif obj["op"] == "create_deployment":
            if "status" in obj:
                if obj["status"] == "success":
                    self.create_deployment_ok_received()
                else:
                    self.logger.info("Unable to create a deployment")
            else:
                deployment = obj["deployment"]
                self.create_deployment_received(deployment)
        elif obj["op"] == "data_tuple":
            stream_selector = obj["stream_selector"]
            data_tuple = obj["tuple"]

            self.data_tuple_received(stream_selector, data_tuple)
        else:
            super().object_received(obj)

class ClusterProtocol(SparseProtocol):
    def __init__(self, node):
        super().__init__()
        self.node = node

        self.app_name = None
        self.app_dag = None

        self.transferring_module = None
        self.receiving_module_name = None

    def connection_lost(self, exc):
        self.node.cluster_orchestrator.remove_cluster_connection(self.transport)
        self.logger.debug("Connection %s disconnected.", self)

    def transfer_module(self, module : SparseModule):
        self.transferring_module = module

        self.send_init_module_transfer(self.transferring_module.name)

    def init_module_transfer_ok_received(self):
        self.send_file(self.transferring_module.zip_path)

    def init_module_transfer_error_received(self):
        self.logger.error("Module transfer initialization failed")

    def init_module_transfer_received(self, module_name : str):
        if self.receiving_module_name is None:
            self.receiving_module_name = module_name
            self.send_init_module_transfer_ok()
        else:
            self.send_init_module_transfer_error()

    def create_deployment_received(self, deployment : Deployment):
        self.node.cluster_orchestrator.create_deployment(deployment)

        self.send_create_deployment_ok()

    def data_tuple_received(self, stream_selector : str, data_tuple : str):
        self.node.stream_router.tuple_received(stream_selector, data_tuple)

    def file_received(self, data : bytes):
        app_archive_path = f"/tmp/{self.app_name}.zip"
        with open(app_archive_path, "wb") as f:
            f.write(data)

        self.logger.info("Received module '%s' from %s", self.receiving_module_name, self)
        module = self.node.module_repo.add_app_module(self.receiving_module_name, app_archive_path)
        self.node.cluster_orchestrator.distribute_module(self, module)
        self.receiving_module_name = None

        self.send_transfer_file_ok()

class ClusterClientProtocol(ClusterProtocol):
    """Cluster client protocol creates an egress connection to another cluster node.
    """
    def __init__(self, on_con_lost : asyncio.Future, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        super().connection_made(transport)

        self.send_connect_downstream()

    def connect_downstream_ok_received(self):
        self.node.cluster_orchestrator.add_cluster_connection(self, direction="egress")

class ClusterServerProtocol(ClusterProtocol):
    """Cluster client protocol creates an ingress connection to another cluster node.
    """

    def connect_downstream_received(self):
        self.node.cluster_orchestrator.add_cluster_connection(self, "ingress")
        self.send_connect_downstream_ok()

    def create_connector_stream_received(self, stream_id : str = None, stream_alias : str = None):
        stream = self.node.stream_router.create_connector_stream(self, stream_id, stream_alias)
        self.node.cluster_orchestrator.distribute_stream(self, stream)

        self.send_create_connector_stream_ok(stream.stream_id, stream.stream_alias)

    def subscribe_received(self, stream_alias : str):
        self.node.stream_router.subscribe(stream_alias, self)
        self.send_subscribe_ok(stream_alias)
