"""This module implements basic cluster node functionality.
"""
import asyncio
import logging
import uuid

from .node_config import SparseNodeConfig
from .cluster import ClusterOrchestrator
from .module import ModuleRepository
from .runtime import SparseRuntime
from .stream import StreamRepository, StreamRouter
from .stats import QoSMonitor
from .cluster.protocols import ClusterClientProtocol, ClusterServerProtocol
from .utils.helper_functions import retry_connection_until_successful

__all__ = ["SparseNode"]

class SparseNode:
    """Common base class for each Node in a Sparse cluster.

    Nodes maintain the task loop for its components. Each functionality, including the runtime is implemented by slices
    that the node houses.
    """
    # pylint: disable=too-many-instance-attributes
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
        self.stream_repository = StreamRepository(self.runtime)
        self.stream_router = StreamRouter(self.runtime, self.stream_repository, self.config)
        self.cluster_orchestrator = ClusterOrchestrator(self.runtime, self.stream_repository, self.config)

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
