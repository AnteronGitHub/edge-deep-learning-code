"""This module includes transport protocols for deployments.
"""
import asyncio

from ..protocols import SparseTransportProtocol
from .deployment import Deployment

class DeploymentClientProtocol(SparseTransportProtocol):
    """App uploader protocol uploads a Sparse module including an application deployment to an open Sparse API.

    Application is deployed in two phases. First its DAG is deployed as a dictionary, and then the application modules
    are deployed as a ZIP archive.
    """
    def __init__(self, deployment : Deployment, on_con_lost : asyncio.Future, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.on_con_lost = on_con_lost
        self.deployment = deployment

    def connection_made(self, transport):
        super().connection_made(transport)

        self.send_create_deployment(self.deployment)

    def send_create_deployment(self, deployment : Deployment):
        """Propagates a deployment in the cluster.
        """
        self.send_payload({"op": "create_deployment", "deployment": deployment})

    def object_received(self, obj : dict):
        if obj["op"] == "create_deployment" and "status" in obj:
            if obj["status"] == "success":
                self.create_deployment_ok_received()
            else:
                self.logger.info("Unable to create a deployment")

    def create_deployment_ok_received(self):
        """Called when a deployment has been acknowledged.
        """
        self.logger.info("Deployment '%s' created successfully.", self.deployment)
        self.transport.close()

    def connection_lost(self, exc):
        if self.on_con_lost is not None:
            self.on_con_lost.set_result(True)

class DeploymentServerProtocol(SparseTransportProtocol):
    """Deployment server protocol receives and handles requests to create new deployments into a sparse cluster.
    """
    def __init__(self, on_create_deployment_received = None):
        super().__init__()
        self.on_create_deployment_received = on_create_deployment_received

    def object_received(self, obj : dict):
        if obj["op"] == "create_deployment" and "status" not in obj:
            deployment = obj["deployment"]
            if self.on_create_deployment_received is not None:
                self.on_create_deployment_received(deployment)
            self.send_create_deployment_ok()

    def send_create_deployment_ok(self):
        """Replies to the sender that a deployment was created successfully.
        """
        self.send_payload({"op": "create_deployment", "status": "success"})
