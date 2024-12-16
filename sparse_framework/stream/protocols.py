"""This module includes transport functionality related to streams.
"""
from ..protocols import SparseTransportProtocol

class StreamDataReceiverProtocol(SparseTransportProtocol):
    """Stream data protocol transmits new tuples for a stream to subscribed nodes.
    """
    def __init__(self, on_data_tuple_received = None):
        super().__init__()
        self.on_data_tuple_received = on_data_tuple_received

    def object_received(self, obj : dict):
        if obj["op"] == "data_tuple":
            stream_selector = obj["stream_selector"]
            data_tuple = obj["tuple"]

            self.logger.debug("Received tuple for stream %s from peer %s", stream_selector, self)
            if self.on_data_tuple_received is not None:
                self.on_data_tuple_received(stream_selector, data_tuple)

class StreamDataSenderProtocol(SparseTransportProtocol):
    """Stream data protocol transmits new tuples for a stream to subscribed nodes.
    """
    def send_data_tuple(self, stream_selector : str, data_tuple):
        """Sends a new data tuple for a stream.
        """
        self.logger.debug("Sending tuple for stream %s to peer %s", stream_selector, self)
        self.send_payload({"op": "data_tuple", "stream_selector": stream_selector, "tuple": data_tuple })

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
