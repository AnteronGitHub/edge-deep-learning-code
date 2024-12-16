"""This module includes functionality related to stream api.
"""
import uuid
import logging

from .protocols import SparseTransportProtocol
from .runtime.operator import StreamOperator

__all__ = ["SparseStream", "StreamDataReceiverProtocol", "StreamDataSenderProtocol"]

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

class SparseStream:
    """Sparse stream is an abstraction for an unbounded set of data tuples.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, stream_id : str = None, stream_alias : str = None, runtime = None):
        self.logger = logging.getLogger("sparse")

        self.stream_id = str(uuid.uuid4()) if stream_id is None else stream_id
        self.stream_alias = stream_alias
        self.runtime = runtime

        self.sequence_no = 0
        self.protocols = set()
        self.operators = set()
        self.streams = set()

    def __str__(self):
        return self.stream_alias or self.stream_id

    def matches_selector(self, stream_selector : str) -> bool:
        """Checks if a specified stream selector matches this stream.
        """
        return stream_selector in (self.stream_alias, self.stream_id)

    def subscribe(self, protocol : StreamDataSenderProtocol):
        """Subscribes a protocol to receive stream tuples.
        """
        self.protocols.add(protocol)
        self.logger.info("Stream %s connected to peer %s", self, protocol)

    def connect_to_operator(self, operator : StreamOperator, output_stream):
        """Connects a stream to operator with given output stream.
        """
        self.operators.add((operator, output_stream))
        self.logger.info("Stream %s connected to operator %s with output stream %s", self, operator.name, output_stream)

    def connect_to_stream(self, stream):
        """Connects this stream to another, sending all new tuples to the target.
        """
        self.streams.add(stream)
        self.logger.info("Connected stream %s to stream %s", self, stream)

    def emit(self, data_tuple):
        """Sends a new data tuple to the connected operators and subscribed connections.
        """
        if self.stream_alias is not None:
            self.logger.debug("Stream %s emitting data (seq. no. %s) to %s operators and %s connections",
                             self,
                             self.sequence_no,
                             len(self.operators),
                             len(self.protocols))
        for operator, output_stream in self.operators:
            self.runtime.call_operator(operator, self, data_tuple, output_stream)

        for protocol in self.protocols:
            protocol.send_data_tuple(str(self), data_tuple)

        for stream in self.streams:
            stream.emit(data_tuple)

        self.sequence_no += 1
