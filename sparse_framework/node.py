"""This module implements basic cluster node functionality.
"""
import asyncio
import logging
import os
import uuid

from dotenv import load_dotenv

__all__ = ["SparseNode", "SparseSlice"]

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

class SparseNodeConfig:
    """Data class for the configuration options available for a cluster node.
    """
    def __init__(self):
        self.upstream_host = None
        self.upstream_port = None
        self.listen_address = None
        self.listen_port = None
        self.root_server_address = None
        self.root_server_port = None
        self.app_repo_path = None

    def load_config(self):
        """Loads the configuration.
        """
        load_dotenv(dotenv_path=".env")

        self.upstream_host = os.environ.get('MASTER_UPSTREAM_HOST') or '127.0.0.1'
        self.upstream_port = os.environ.get('MASTER_UPSTREAM_PORT') or 50007
        self.listen_address = os.environ.get('WORKER_LISTEN_ADDRESS') or '127.0.0.1'
        self.listen_port = os.environ.get('WORKER_LISTEN_PORT') or 50007
        self.root_server_address = os.environ.get('SPARSE_ROOT_SERVER_ADDRESS')
        self.root_server_port = os.environ.get('SPARSE_ROOT_SERVER_PORT') or 50006
        self.app_repo_path = os.environ.get('SPARSE_APP_REPO_PATH') or '/usr/lib/sparse_framework/apps'

from .cluster_orchestrator import ClusterOrchestrator
from .module_repo import ModuleRepository
from .runtime import SparseRuntime
from .stream_router import StreamRouter
from .stats import QoSMonitor

class SparseNode:
    """Common base class for each Node in a Sparse cluster.

    Nodes maintain the task loop for its components. Each functionality, including the runtime is implemented by slices
    that the node houses.
    """

    def __init__(self, node_id : str = str(uuid.uuid4()), log_level : int = logging.INFO):
        self.node_id = node_id

        logging.basicConfig(format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s', level=log_level)
        self.logger = logging.getLogger("sparse")

        self.config = SparseNodeConfig()
        self.config.load_config()

        self.init_slices()

    def init_slices(self):
        """Initializes node slices.
        """
        self.qos_monitor = QoSMonitor(self.config)
        self.module_repo = ModuleRepository(self.config)
        self.runtime = SparseRuntime(self.module_repo, self.qos_monitor, self.config)
        self.stream_router = StreamRouter(self.runtime, self.config)
        self.cluster_orchestrator = ClusterOrchestrator(self.runtime,
                                                        self.stream_router,
                                                        self.config)

        self.slices = [
                self.qos_monitor,
                self.module_repo,
                self.runtime,
                self.stream_router,
                self.cluster_orchestrator
                ]

    def get_futures(self):
        """Collects node coroutines to be executed on startup.
        """
        futures = [self.start_app_server()]

        for node_slice in self.slices:
            futures = node_slice.get_futures(futures)

        if self.config.root_server_address is not None:
            futures.append(self.connect_to_downstream_server())

        return futures

    async def start_app_server(self, listen_address = '0.0.0.0'):
        """Starts the cluster server.
        """
        from .protocols import ClusterServerProtocol

        loop = asyncio.get_running_loop()

        server = await loop.create_server(lambda: ClusterServerProtocol(self), \
                                          listen_address, \
                                          self.config.root_server_port)
        self.logger.info("Server listening to %s:%d", listen_address, self.config.root_server_port)
        async with server:
            await server.serve_forever()

    async def connect_to_downstream_server(self):
        """Connects to another cluster node.
        """
        from .protocols import ClusterClientProtocol
        from .utils.helper_functions import retry_connection_until_successful

        await retry_connection_until_successful(lambda on_con_lost: ClusterClientProtocol(on_con_lost, self), \
                                                self.config.root_server_address, \
                                                self.config.root_server_port, \
                                                self.logger)

    async def start(self):
        """Starts the main task loop by collecting all of the future objects.

        NB! When subclassing SparseNode instead of extending this function the user should use the get_futures
        function.
        """
        futures = self.get_futures()

        await asyncio.gather(*futures)
