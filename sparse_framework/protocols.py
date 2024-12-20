"""This module contains the basic transport protocol implementations used by the cluster.
"""
import asyncio
import io
import logging
import pickle
import struct
import uuid

SPARSE_MESSAGE_HEADER_SIZE = 9

class SparseMessageHeader:
    """Sparse messages include type and size (in bytes) of the message.
    """
    def __init__(self, data_type : str, data_size : int):
        self.data_type = data_type
        self.data_size = data_size

    def __str__(self):
        if self.data_type == "f":
            type_str = "file"
        else:
            type_str = "object"
        return f"{self.data_size} byte {type_str} header"

    @classmethod
    def from_bytes(cls, header_bytes : bytes):
        """Parses a message header from bytes.
        """
        [data_type, data_size] = struct.unpack("!sQ", header_bytes)

        return cls(data_type.decode(), data_size)

    def to_bytes(self) -> bytes:
        """Encodes a message header to bytes.
        """
        return struct.pack("!sQ", self.data_type.encode(), self.data_size)

class SparseTransportProtocol(asyncio.Protocol):
    """Sparse transport protocol implements low-level communication for transmitting dictionary data and files over
    network.
    """
    def __init__(self):
        self.connection_id = str(uuid.uuid4())
        self.logger = logging.getLogger(self.__class__.__name__)
        self.transport = None

        self.data_buffer = io.BytesIO()
        self.message_header = None

    def __str__(self):
        """Returns the peer IP or 'unconnected' if no transport object is yet created.
        """
        if self.transport is None:
            return "unconnected"

        return f"{self.transport.get_extra_info('peername')[0]}:{self.transport.get_extra_info('peername')[1]}"

    def connection_made(self, transport):
        self.transport = transport

    def _read_header_bytes(self, buffer_bytes : int) -> bytes:
        """Reads the header bytes from the beginning of the buffer, then moves the pointer back to the end of the
        buffer to resume writing.
        """
        self.data_buffer.seek(0)
        header_bytes = self.data_buffer.read(SPARSE_MESSAGE_HEADER_SIZE)
        self.data_buffer.seek(buffer_bytes)

        return header_bytes

    def _read_data_bytes(self) -> bytes:
        """Reads the message payload from the end of the buffer, then creates an empty buffer and writes potential
        tailing bytes from the previous one.
        """
        self.data_buffer.seek(SPARSE_MESSAGE_HEADER_SIZE)
        message_payload = self.data_buffer.read(self.message_header.data_size)
        message_header = self.message_header

        self.data_buffer.seek(SPARSE_MESSAGE_HEADER_SIZE + self.message_header.data_size)
        buffer_tail = self.data_buffer.read()

        self.message_header = None
        self.data_buffer = io.BytesIO()
        self.data_buffer.write(buffer_tail)

        return message_header, message_payload

    def data_received(self, data : bytes):
        self.data_buffer.write(data)
        buffer_bytes = self.data_buffer.getbuffer().nbytes

        if buffer_bytes < SPARSE_MESSAGE_HEADER_SIZE:
            return

        if self.message_header is None:
            self.message_header = SparseMessageHeader.from_bytes(self._read_header_bytes(buffer_bytes))

        if buffer_bytes >= SPARSE_MESSAGE_HEADER_SIZE + self.message_header.data_size:
            message_header, message_payload = self._read_data_bytes()
            self.message_received(message_header, message_payload)
            self.data_received(b"")

    def message_received(self, message_header : str, message_payload : bytes):
        """Callback function that is triggered when all the bytes specified by a message header been received.
        """
        if message_header.data_type == "f":
            self.file_received(message_payload)
        elif message_header.data_type == "o":
            try:
                self.object_received(pickle.loads(message_payload))
            except pickle.UnpicklingError:
                self.logger.error("Unable to deserialize message with %s and actual payload size %s bytes.",
                                  message_header,
                                  len(message_payload))

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

            self.logger.debug("Sending %s byte file to %s", file_size, str(self))
            self.transport.write(SparseMessageHeader("f", file_size).to_bytes() + data_bytes)

    def send_payload(self, payload : dict):
        """Transmits a given object to the peer.
        """
        payload_data = pickle.dumps(payload)
        payload_size = len(payload_data)

        self.logger.debug("Sending %s byte message to %s", payload_size, str(self))
        self.transport.write(SparseMessageHeader("o", payload_size).to_bytes() + payload_data)

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
