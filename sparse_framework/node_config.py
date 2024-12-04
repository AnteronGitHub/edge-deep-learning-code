"""This module includes implementation for for node configuration.
"""
import dataclasses
import os

from dotenv import load_dotenv

@dataclasses.dataclass
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
