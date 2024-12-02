"""This module includes software abstractions for application deployment.
"""
import yaml

class Deployment:
    """Application deployment defines data pipelines from input streams, and produces new output streams.
    """
    name : str
    streams : list
    pipelines : dict

    def __init__(self, name : str, streams : set, pipelines : dict):
        self.name = name
        self.streams = streams
        self.pipelines = pipelines

    def __str__(self):
        return self.name

    @classmethod
    def from_yaml(cls, file_path : str):
        """Parses a deployment from yaml file.
        """
        with open(file_path, encoding='utf-8') as f:
            data = yaml.safe_load(f)

        return cls(data["name"], data["streams"], data["pipelines"])
