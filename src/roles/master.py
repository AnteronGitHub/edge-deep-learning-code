from .task_deployer import TaskDeployer, get_supported_task_deployer

class Master:
    def __init__(self,
                 upstream_host : str = '127.0.0.1',
                 upstream_port : int = 50007,
                 task_deployer : TaskDeployer = None,
                 legacy_asyncio : bool = False):
        if task_deployer:
            self.task_deployer = task_deployer
        else:
            self.task_deployer = get_supported_task_deployer(upstream_host=upstream_host,
                                                             upstream_port=upstream_port,
                                                             legacy_asyncio=legacy_asyncio)
