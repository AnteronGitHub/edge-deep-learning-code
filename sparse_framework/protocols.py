"""This module contains the basic transport protocol implementations used by the cluster.
"""
import asyncio
import io
import logging
import pickle
import struct
import uuid

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
