"""This module includes sparse module transport protocols.
"""
from .module_repo import SparseModule
from ..protocols import SparseTransportProtocol

class ModuleReceiverProtocol(SparseTransportProtocol):
    """Module receiver receives a Sparse module from another node.
    """
    def __init__(self, module_repo, cluster_orchestrator):
        super().__init__()
        self.module_repo = module_repo
        self.cluster_orchestrator = cluster_orchestrator
        self.receiving_module_name = None

    def object_received(self, obj : dict):
        if obj["op"] == "init_module_transfer" and "status" not in obj:
            module_name = obj["module_name"]

            self.init_module_transfer_received(module_name)

    def init_module_transfer_received(self, module_name : str):
        """Callback for when a module transfer is received.
        """
        if self.receiving_module_name is None:
            self.receiving_module_name = module_name
            self.send_init_module_transfer_ok()
        else:
            self.send_init_module_transfer_error()


    def send_init_module_transfer_ok(self):
        """Replies that a requested module transfer has been initialized successfully.
        """
        self.send_payload({"op": "init_module_transfer", "status": "accepted"})

    def send_init_module_transfer_error(self):
        """Replies that a requested module transfer has failed to initialize.
        """
        self.send_payload({"op": "init_module_transfer", "status": "rejected"})

    def file_received(self, data : bytes):
        app_archive_path = f"/tmp/{self.receiving_module_name}.zip"
        with open(app_archive_path, "wb") as f:
            f.write(data)

        self.logger.info("Received module '%s' from %s", self.receiving_module_name, self)
        module = self.module_repo.add_app_module(self.receiving_module_name, app_archive_path)
        self.cluster_orchestrator.distribute_module(self, module)
        self.receiving_module_name = None

        self.send_transfer_file_ok()

    def send_transfer_file_ok(self):
        """Replies that a file has been transferred successfully.
        """
        self.send_payload({"op": "transfer_file", "status": "success"})

class ModuleSenderProtocol(SparseTransportProtocol):
    """Module sender transfers a Sparse module to another node.
    """
    def __init__(self):
        super().__init__()

        self.transferring_module = None

    def transfer_module(self, module : SparseModule):
        """Starts a module transfer for the specified locally available module.
        """
        self.transferring_module = module

        self.send_init_module_transfer(self.transferring_module.name)

    def send_init_module_transfer(self, module_name : str):
        """Initiates a module transfer to a peer.
        """
        self.send_payload({ "op": "init_module_transfer", "module_name": module_name })

    def object_received(self, obj : dict):
        if obj["op"] == "init_module_transfer" and "status" in obj:
            if obj["status"] == "accepted":
                self.init_module_transfer_ok_received()
            else:
                self.init_module_transfer_error_received()
        elif obj["op"] == "transfer_file" and "status" in obj:
            if obj["status"] == "success":
                self.transfer_file_ok_received()

    def init_module_transfer_ok_received(self):
        """Callback for when a module transfer has been acknowledged to have succeeded by the peer.
        """
        self.send_file(self.transferring_module.zip_path)

    def init_module_transfer_error_received(self):
        """Callback for when a module transfer has been acknowledged to be failed by the peer.
        """
        self.logger.error("Module transfer initialization failed")

    def transfer_file_ok_received(self):
        """Callback for when a file transfer has been acknowledged by the peer.
        """
