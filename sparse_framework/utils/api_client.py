"""This module implements helper functionality for connecting to the cluster from python programs.
"""
import asyncio
import logging
import os
import shutil
import tempfile

from ..deployment import Deployment
from ..deployment.protocols import DeploymentClientProtocol
from ..module_repo import SparseModule
from ..protocols import ModuleSenderProtocol

from .helper_functions import retry_connection_until_successful

class ModuleUploaderProtocol(ModuleSenderProtocol):
    """App uploader protocol uploads a Sparse module including an application deployment to an open Sparse API.

    Application is deployed in two phases. First its DAG is deployed as a dictionary, and then the application modules
    are deployed as a ZIP archive.
    """
    def __init__(self, module_name : str, archive_path : str, on_con_lost : asyncio.Future, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.on_con_lost = on_con_lost
        self.module_name = module_name
        self.archive_path = archive_path

    def connection_made(self, transport):
        super().connection_made(transport)

        self.transfer_module(SparseModule(self.module_name, self.archive_path))

    def transfer_file_ok_received(self):
        self.logger.info("Module '%s' uploaded successfully.", self.module_name)
        self.transport.close()

    def connection_lost(self, exc):
        if self.on_con_lost is not None:
            self.on_con_lost.set_result(True)

class SparseAPIClient:
    """Sparse API client can be used to communicate with the Sparse API to upload applications.

    Sparse applications comprise of software modules defining the sources, operators and sinks, as well as Directed
    Asyclic Graphs that describe the data flow among the sources, operators and sinks.
    """
    def __init__(self, api_host : str, api_port : int = 50006):
        self.logger = logging.getLogger("sparse")
        logging.basicConfig(format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s', level=logging.INFO)
        self.api_host = api_host
        self.api_port = api_port

    def archive_module(self, module_name : str, module_dir : str) -> str:
        """Creates a ZIP archive of a sparse module and returns the file path of the module archive.
        """
        self.logger.debug("Creating Sparse module from directory %s", module_dir)
        shutil.make_archive(os.path.join(tempfile.gettempdir(), module_name), 'zip', module_dir)

        return os.path.join(tempfile.gettempdir(), f"{module_name}.zip")

    async def upload_module(self, module_name : str, module_archive_path : str):
        """Uploads an application module to the cluster.
        """
        await retry_connection_until_successful(lambda on_con_lost: ModuleUploaderProtocol(module_name, \
                                                                                           module_archive_path, \
                                                                                           on_con_lost), \
                                                self.api_host, \
                                                self.api_port, \
                                                self.logger)

    async def post_deployment(self, deployment : Deployment):
        """Creates a new deployment to the cluster.
        """
        await retry_connection_until_successful(lambda on_con_lost: DeploymentClientProtocol(deployment, on_con_lost), \
                                                self.api_host, \
                                                self.api_port, \
                                                self.logger)

    def create_module(self, module_dir : str = '.'):
        """Archives a Sparse module and uploads it to a running cluster.
        """
        module_name = os.path.basename(os.path.realpath(module_dir))
        module_archive_path = self.archive_module(module_name, module_dir)
        asyncio.run(self.upload_module(module_name, module_archive_path))

    def create_deployment(self, deployment : Deployment):
        """Creates a Sparse application deployment in a running cluster.
        """
        asyncio.run(self.post_deployment(deployment))
