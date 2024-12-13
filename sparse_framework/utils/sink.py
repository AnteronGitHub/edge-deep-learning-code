"""This module contains a utility sink interface, that can be used in python applications.
"""
import asyncio

from ..protocols import MultiplexerProtocol, StreamSubscribeProtocol
from ..stream_api import StreamDataReceiverProtocol

from .helper_functions import retry_connection_until_successful

class SinkProtocol(MultiplexerProtocol):
    """Sink protocol connects to a cluster end point and subscribes to a stream.
    """
    def __init__(self, stream_alias : str, on_tuple_received, on_con_lost : asyncio.Future):
        self.stream_subscribe_protocol = StreamSubscribeProtocol(self.subscribe_ok_received)
        self.stream_alias = stream_alias
        self.on_tuple_received = on_tuple_received
        self.on_con_lost = on_con_lost

        super().__init__({ self.stream_subscribe_protocol,
                           StreamDataReceiverProtocol(lambda stream_id, data_tuple: on_tuple_received(data_tuple)) })

    def connection_made(self, transport):
        super().connection_made(transport)
        self.logger.debug("Subscribing to stream '%s'...", self.stream_alias)
        self.stream_subscribe_protocol.send_subscribe(self.stream_alias)

    def subscribe_ok_received(self, stream_alias : str):
        """Callback for when a subscription has been acknowledged
        """
        self.logger.info("Subscribed to stream '%s'", stream_alias)

    def connection_lost(self, exc):
        self.on_con_lost.set_result(True)

class SparseSink:
    """Utility class that uses Sink Protocol to connect to a cluster end point and subscribe to a stream.
    """
    @property
    def type(self):
        """The type of the sink.
        """
        return self.__class__.__name__

    def tuple_received(self, new_tuple):
        """Function that can be implemented by inheriting classes to determine what to do when a new tuple is received
        from a stream.
        """

    async def connect(self, stream_alias : str, endpoint_host : str, endpoint_port : int = 50006):
        """Connects to a cluster end point to subscribe to a stream.
        """
        await retry_connection_until_successful(lambda on_con_lost: SinkProtocol(stream_alias, \
                                                                                 self.tuple_received, \
                                                                                 on_con_lost), \
                                                endpoint_host, \
                                                endpoint_port)
