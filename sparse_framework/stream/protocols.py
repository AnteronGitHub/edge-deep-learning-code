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
