"""This module includes functionality related to software modules uploaded into a Sparse cluster.
"""
import importlib
import os
import shutil

from .sparse_slice import SparseSlice

class SparseModule:
    """A sparse app is a Python module that provides a set of sources, operators, and sinks.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, name : str, zip_path : str):
        self.name = name
        self.zip_path = zip_path
        self.app_module = None

    def load(self, app_repo_path : str):
        """Loads a module into memory from the specified module path.
        """
        if self.app_module is None:
            shutil.unpack_archive(self.zip_path, os.path.join(app_repo_path, f"sparseapp_{self.name}"))
            self.app_module = importlib.import_module(f".sparseapp_{self.name}", package="sparse_framework.apps")

        return self.app_module

class OperatorNotFoundError(Exception):
    """Raised a module including a referenced operator cannot be found."""
    def __init__(self, operator_name : str):
        self.operator_name = operator_name

    def __str__(self):
        return f"A module containing operator '{self.operator_name}' cound not be found."

class ModuleRepository(SparseSlice):
    """Module repository serves Sparse application modules. To distribute stream applications in the cluster, modules
    can be migrated over the network.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.apps = set()

    def add_app_module(self, name : str, zip_path : str):
        """Adds a Sparse module into the repository.
        """
        module = SparseModule(name, zip_path)
        self.apps.add(module)
        return module

    def get_operator_factory(self, operator_name : str):
        """Returns a factory for placing an operator into the local runtime.
        """
        for app in self.apps:
            app_module = app.load(self.config.app_repo_path)
            for operator_factory in app_module.__all__:
                if operator_factory.__name__ == operator_name:
                    return operator_factory

        raise OperatorNotFoundError(operator_name)
