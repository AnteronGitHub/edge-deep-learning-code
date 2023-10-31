import asyncio
import io
import logging
import pickle
import struct

class SparseProtocol(asyncio.Protocol):
    def __init__(self, on_con_lost = None):
        self.logger = logging.getLogger("sparse")
        self.input_buffer = io.BytesIO()
        self.transport = None

        self.receiving_payload = False
        self.payload_size = 0
        self.on_con_lost = on_con_lost

    def send_payload(self, payload):
        payload_data = pickle.dumps(payload)
        payload_size = len(payload_data)

        self.transport.write(struct.pack("!Q", payload_size))
        self.transport.write(payload_data)

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self.transport = transport
        self.logger.info(f"Connected to {peername}.")

    def data_received(self, data):
        if self.receiving_payload:
            payload = data
        else:
            self.receiving_payload = True
            header = data[:8]
            self.payload_size = struct.unpack("!Q", header)[0]
            payload = data[8:]

        self.input_buffer.write(payload)

        if self.input_buffer.getbuffer().nbytes >= self.payload_size:
            payload_data = self.input_buffer.getvalue()
            try:
                payload = pickle.loads(payload_data)
                self.input_buffer = io.BytesIO()
                self.payload_received(payload)
                self.receiving_payload = False
            except pickle.UnpicklingError:
                self.logger.error(f"Deserialization error. {len(payload_data)} payload size, {self.input_buffer.getbuffer().nbytes} buffer size.")

    def connection_lost(self, exc):
        if self.on_con_lost:
            self.on_con_lost.set_result(True)

