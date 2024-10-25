import asyncio

from ..protocols import SparseProtocol

class SinkProtocol(SparseProtocol):
    """Sink protocol connects to a cluster end point and subscribes to a stream.
    """
    def __init__(self, stream_alias : str, on_tuple_received, on_con_lost : asyncio.Future):
        super().__init__()
        self.stream_alias = stream_alias
        self.on_tuple_received = on_tuple_received
        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        super().connection_made(transport)
        self.logger.debug("Subscribing to stream '%s'...", self.stream_alias)
        self.send_subscribe(self.stream_alias)

    def subscribe_ok_received(self, stream_alias : str):
        self.logger.info("Subscribed to stream '%s'", stream_alias)

    def connection_lost(self, transport):
        self.on_con_lost.set_result(True)

    def data_tuple_received(self, stream_id : str, data_tuple):
        self.on_tuple_received(data_tuple)

class SparseSink:
    @property
    def type(self):
        return self.__class__.__name__

    def tuple_received(self, new_tuple):
        pass

    async def connect(self, stream_alias : str, endpoint_host : str, endpoint_port : int = 50006):
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()

        while True:
            try:
                self.logger.debug("Connecting to cluster endpoint on %s:%s.", endpoint_host, endpoint_port)
                await loop.create_connection(lambda: SinkProtocol(stream_alias, self.tuple_received, on_con_lost), \
                                             endpoint_host, \
                                             endpoint_port)
                await on_con_lost
                break
            except ConnectionRefusedError:
                self.logger.warn("Connection refused. Re-trying in 5 seconds.")
                await asyncio.sleep(5)
