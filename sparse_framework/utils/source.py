"""This module implements helpers for creating simulated data sources for testing and prototyping purposes.
"""
import asyncio
import uuid
import logging

from ..protocols import MultiplexerProtocol
from ..stream import SparseStream
from ..stream.protocols import StreamDataSenderProtocol, StreamMigratorProtocol

class SourceProtocol(MultiplexerProtocol):
    """Source protocol connects to a cluster end point and receives a stream id that can be used to transmit data
    tuples using the established connection.
    """
    def __init__(self, on_stream_initialized : asyncio.Future, stream_alias : str = None):
        self.migrator_protocol = StreamMigratorProtocol(self.create_connector_stream_ok_received)
        self.data_protocol = StreamDataSenderProtocol()
        self.on_stream_initialized = on_stream_initialized
        self.stream_alias = stream_alias

        super().__init__({ self.migrator_protocol, self.data_protocol })

    def connection_made(self, transport):
        super().connection_made(transport)
        self.migrator_protocol.send_create_connector_stream(stream_alias=self.stream_alias)

    def create_connector_stream_ok_received(self, stream_id : str, stream_alias : str):
        """Callback for when a stream has been migrates succesfully
        """
        stream = SparseStream(stream_id, stream_alias)
        stream.subscribe(self.data_protocol)
        self.on_stream_initialized.set_result(stream)

    def send_data_tuple(self, stream, data_tuple):
        """Sends new data tuple for a stream.
        """
        self.data_protocol.send_data_tuple(str(stream), data_tuple)

class SparseSource:
    """Implementation of a simulated Sparse cluster data source. A Sparse source connects to a cluster end point and
    receives a stream id that it can use to send data to.
    """
    def __init__(self, stream_alias : str = None, target_latency : int = 200):
        self.logger = logging.getLogger("sparse")
        logging.basicConfig(format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s', level=logging.INFO)

        self.id = str(uuid.uuid4())
        self.stream_alias = stream_alias
        self.target_latency = target_latency

        self.stream = None

    def get_tuple(self):
        """This function should be implemented by inheriting classes to specify how the next tuple is read.
        """

    async def connect(self, endpoint_host : str = "sparse-root", endpoint_port : int = 50006):
        """Called to start the data source. Connects the source to a cluster host and starts streaming data.
        """
        stream = await self.initialize_stream(endpoint_host, endpoint_port)
        self.stream = stream
        await self.start_stream()

    async def initialize_stream(self, endpoint_host : str, endpoint_port : int):
        """Initializes the stream in the cluster before starting to transmit data tuples.
        """
        loop = asyncio.get_running_loop()
        on_stream_initialized = loop.create_future()

        while True:
            try:
                self.logger.debug("Connecting to cluster endpoint on %s:%s.", endpoint_host, endpoint_port)
                await loop.create_connection(lambda: SourceProtocol(on_stream_initialized, \
                                                                    self.stream_alias), \
                                             endpoint_host, \
                                             endpoint_port)
                stream = await on_stream_initialized
                return stream
            except ConnectionRefusedError:
                self.logger.warning("Connection refused. Re-trying in 5 seconds.")
                await asyncio.sleep(5)

    async def start_stream(self):
        """Starts streaming data.
        """
        while True:
            self.stream.emit(self.get_tuple())
            await asyncio.sleep(self.target_latency / 1000.0)
