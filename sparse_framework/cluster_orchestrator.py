from .deployment import Deployment
from .module_repo import SparseModule
from .node import SparseSlice
from .protocols import SparseProtocol
from .runtime import SparseRuntime
from .stream_api import SparseStream
from .stream_router import StreamRouter

class ClusterConnection:
    """Data class for maintaining data about connected cluster nodes.
    """
    protocol : SparseProtocol
    direction : str

    def __init__(self, protocol : SparseProtocol, direction : str):
        self.protocol = protocol
        self.direction = direction

    def transfer_module(self, app : SparseModule):
        self.protocol.transfer_module(app)

    def create_deployment(self, app_dag : dict):
        self.protocol.create_deployment(app_dag)

class ClusterOrchestrator(SparseSlice):
    def __init__(self, runtime : SparseRuntime, stream_router : SparseRuntime, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.runtime = runtime
        self.stream_router = stream_router

        self.cluster_connections = set()

    def add_cluster_connection(self, protocol : SparseProtocol, direction : str):
        """Adds a connection to another cluster node for stream routing and operator migration.
        """
        cluster_connection = ClusterConnection(protocol, direction)
        self.cluster_connections.add(cluster_connection)
        self.logger.info("Added %s connection with node %s", direction, protocol)

        for connector_stream in self.stream_router.streams:
            cluster_connection.protocol.send_create_connector_stream(connector_stream.stream_id,
                                                                     connector_stream.stream_alias)
            connector_stream.subscribe(cluster_connection.protocol)

    def remove_cluster_connection(self, protocol):
        """Removes a cluster connection.
        """
        for connection in self.cluster_connections:
            if connection.protocol == protocol:
                self.cluster_connections.discard(connection)
                self.logger.info("Removed %s connection with node %s", connection.direction, protocol)
                return

    def distribute_module(self, source : SparseProtocol, module : SparseModule):
        """Distributes a module to other cluster nodes.
        """
        for connection in self.cluster_connections:
            if connection.protocol != source:
                self.logger.info("Distributing module %s to node %s", module.name, connection.protocol)
                connection.transfer_module(module)

    def distribute_stream(self, source : SparseProtocol, stream : SparseStream):
        """Distributes a stream to other cluster nodes.
        """
        if source in stream.protocols:
            stream.protocols.remove(source)

        for connection in self.cluster_connections:
            if connection.protocol != source:
                self.logger.debug("Broadcasting stream %s to peer %s", stream, connection.protocol)

                connection.protocol.send_create_connector_stream(stream.stream_id, stream.stream_alias)

                # TODO: Subscribe to streams separately
                stream.subscribe(connection.protocol)

    def deploy_pipelines(self, streams : set, pipelines : dict, source : SparseStream = None):
        for stream_selector in pipelines.keys():
            if stream_selector in streams:
                output_stream = self.stream_router.get_stream(stream_alias=stream_selector)
            else:
                operator = self.runtime.place_operator(stream_selector)
                if source is None:
                    self.logger.warn("Placed operator '%s' with no input stream", operator)
                else:
                    output_stream = self.stream_router.get_stream()
                    source.connect_to_operator(operator, output_stream)

            destinations = pipelines[stream_selector]
            if type(destinations) == dict:
                self.deploy_pipelines(streams, destinations, output_stream)
            elif type(destinations) == list:
                for selector in destinations:
                    if selector in streams:
                        final_stream = self.stream_router.get_stream(selector)
                        output_stream.connect_to_stream(final_stream)
                    else:
                        self.logger.warn("Leaf operator %s not created", selector)

    def create_deployment(self, deployment : Deployment):
        """Deploys a Sparse pipelines to a cluster.
        """
        self.logger.debug("Creating deployment %s", deployment)

        self.deploy_pipelines(deployment.streams, deployment.pipelines)
