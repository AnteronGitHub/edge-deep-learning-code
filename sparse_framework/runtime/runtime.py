import asyncio

from ..module_repo import ModuleRepository, OperatorNotFoundError
from ..node import SparseSlice
from ..stats import QoSMonitor

from .operator import StreamOperator
from .task_dispatcher import TaskDispatcher

class SparseRuntime(SparseSlice):
    """Sparse runtime maintains task queue, task executor, and stream processing operations.
    """
    def __init__(self, module_repo : ModuleRepository, qos_monitor : QoSMonitor, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.module_repo = module_repo
        self.qos_monitor = qos_monitor

        self.task_queue = None
        self.task_dispatcher = None
        self.operators = set()

    def get_futures(self, futures):
        self.task_queue = asyncio.Queue()
        self.task_dispatcher = TaskDispatcher(self.task_queue)

        futures.append(self.task_dispatcher.start())

        return futures

    def place_operator(self, operator_name : str):
        """Places a stream operator to the local runtime.
        """
        for operator in self.operators:
            if operator.name == operator_name:
                return operator

        try:
            operator_factory = self.module_repo.get_operator_factory(operator_name)

            o = operator_factory(qos_monitor=self.qos_monitor)
            o.set_runtime(self)
            self.operators.add(o)

            self.logger.info("Placed operator %s", o)

            return o
        except OperatorNotFoundError as e:
            self.logger.warn(e)

    def call_operator(self, operator : StreamOperator, source, input_tuple, output):
        sequence_no = source.sequence_no
        batch_index = operator.buffer_input(input_tuple,
                                            source,
                                            sequence_no,
                                            lambda output_tuple: self.result_received(operator,
                                                                                      source,
                                                                                      sequence_no,
                                                                                      output_tuple,
                                                                                      output))
        if not operator.use_batching or batch_index == 0:
            self.logger.debug("Created task for operator %s", operator)
            self.task_queue.put_nowait(operator)

        self.qos_monitor.operator_input_buffered(operator, source, sequence_no)

    def result_received(self, operator : StreamOperator, source, sequence_no, output_tuple, output):
        self.qos_monitor.operator_result_received(operator, source, sequence_no)
        output.emit(output_tuple)

    def find_operator(self, operator_name : str):
        for operator in self.operators:
            if operator.name == operator_name:
                return operator

        return None

    def sync_received(self, protocol, stream_id, sync):
        self.logger.debug(f"Received {sync} s sync")
        if self.source is not None:
            if (self.source.no_samples > 0):
                offload_latency = protocol.request_statistics.get_offload_latency(protocol.current_record)

                if not self.source.use_scheduling:
                    sync = 0.0

                target_latency = self.source.target_latency

                loop = asyncio.get_running_loop()
                loop.call_later(target_latency-offload_latency + sync if target_latency > offload_latency else 0, self.source.emit)
            else:
                protocol.transport.close()
