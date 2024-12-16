"""This module implements stream repository which maintains collection of streams in the cluster.
"""
import logging

from ..runtime import SparseRuntime
from .stream_api import SparseStream

class StreamRepository:
    """Stream repository maintains the collection of streams deployed in the cluster.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, runtime : SparseRuntime):
        self.logger = logging.getLogger("sparse")
        self.runtime = runtime
        self.streams = set()

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
