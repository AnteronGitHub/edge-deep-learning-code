import asyncio
import functools
import logging

from concurrent.futures import ThreadPoolExecutor

from .operator import StreamOperator

class TaskDispatcher:
    """Task dispatcher receives operator calls in a queue and execute them in a separate thread.

    Currently, only a single operator is executed at once. While it may be preferable to do so in order to consolidate
    hardware (mainly CPU) resources, more complicated schedulers could also be implemented in the future.
    """
    def __init__(self, task_queue : asyncio.Queue):
        self.logger = logging.getLogger("TaskDispatcher")
        self.executor = ThreadPoolExecutor()

        self.queue = task_queue

    async def start(self):
        loop = asyncio.get_running_loop()
        while True:
            operator = await self.queue.get()
            self.logger.debug("Dispatched tuple from queue (size %s)", self.queue.qsize())
            await loop.run_in_executor(self.executor, operator.execute_task)
            self.queue.task_done()
