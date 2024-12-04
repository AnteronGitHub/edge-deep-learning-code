"""This module implements stream routing functionality in the cluster.
"""
from .sparse_slice import SparseSlice
from .protocols import SparseProtocol
from .runtime import SparseRuntime
from .stream_api import SparseStream

class StreamRouter(SparseSlice):
    """Stream router then ensures that streams are routed according to application specifications. It receives
    applications to be deployed in the cluster, and decides the placement of sources, operators and sinks in the
    cluster.
    """
    def __init__(self, runtime : SparseRuntime, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.runtime = runtime

        self.streams = set()

    def create_connector_stream(self, \
                                source : SparseProtocol, \
                                stream_id : str = None, \
                                stream_alias : str = None):
        """Adds a new connector stream. A connector stream receives tuples over the network, either from another
        cluster node or a data source.
        """
        connector_stream = self.get_stream(stream_id, stream_alias)
        if source in connector_stream.protocols:
            connector_stream.protocols.remove(source)

        self.logger.info("Stream %s listening to source %s", connector_stream.stream_alias, source)

        return connector_stream

    def tuple_received(self, stream_selector : str, data_tuple):
        """Called to route a tuple needs in a node.
        """
        for stream in self.streams:
            if stream.matches_selector(stream_selector):
                stream.emit(data_tuple)
                self.logger.debug("Received data for stream %s", stream)
                return
        self.logger.warning("Received data for stream %s without a connector", stream_selector)

    def subscribe(self, stream_alias : str, protocol : SparseProtocol):
        """Subscribes a protocol to receive tuples in a data stream.
        """
        for stream in self.streams:
            if stream.matches_selector(stream_alias):
                stream.subscribe(protocol)
                return

        stream = self.get_stream(stream_alias=stream_alias)
        stream.subscribe(protocol)

    def connect_to_operators(self, stream : SparseStream, operator_names : set):
        """Adds destinations to a stream.
        """
        for o in self.runtime.operators:
            if o.name in operator_names:
                output_stream = self.get_stream(stream_alias=o.name)
                stream.connect_to_operator(o, output_stream)

    def get_stream(self, stream_id : str = None, stream_alias : str = None):
        """Returns a stream that matches the provided stream alias or stream id. If no stream exists, one is created.
        """
        stream_selector = stream_alias or stream_id
        if stream_selector is not None:
            for stream in self.streams:
                if stream.matches_selector(stream_selector):
                    return stream

        stream = SparseStream(stream_id=stream_id, stream_alias=stream_alias, runtime=self.runtime)
        self.streams.add(stream)
        self.logger.debug("Created stream %s", stream)

        return stream
