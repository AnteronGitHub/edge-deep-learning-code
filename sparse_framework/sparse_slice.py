"""This module includes abstract class for node slice.
"""

import logging

class SparseSlice:
    """Common super class for Sparse Node Slices.

    Slices are analogious to services in service oriented architecture. Each slice provides a coherent feature for a
    node. Additionally, slices may utilize features from other slices to provide higher-level features.
    """
    def __init__(self, config):
        self.logger = logging.getLogger("sparse")
        self.config = config

    def get_futures(self, futures):
        """Collects the futures created by the slice, and adds them to the list
        """
        return futures
