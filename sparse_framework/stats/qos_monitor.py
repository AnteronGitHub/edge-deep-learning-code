"""This module contains Quality of Service (QoS) monitor implementation.
"""
from time import time

from ..runtime.operator import StreamOperator
from ..sparse_slice import SparseSlice

class OperatorRuntimeStatisticsRecord:
    """Operator runtime statistics record tracks tuple processing latency for a given operator.
    """
    operator_id : str
    source_stream_id : str
    input_buffered_at : float
    input_dispatched_at : float
    result_received_at : float

    def __init__(self, operator_id : str, source_stream_id : str, source_stream_sequence_no : int):
        self.operator_id = operator_id
        self.source_stream_id = source_stream_id
        self.source_stream_sequence_no = source_stream_sequence_no
        self.input_buffered_at = None
        self.input_dispatched_at = None
        self.result_received_at = None

    def input_buffered(self):
        """Called when an input tuple has been buffered for the task executor.
        """
        self.input_buffered_at = time()

    def input_dispatched(self):
        """Called when an input tuple has been dispatched from the buffered by the task executor.
        """
        self.input_dispatched_at = time()

    def result_received(self):
        """Called when a result for a task has been computed.
        """
        self.result_received_at = time()

    @property
    def queueing_time(self) -> float:
        """Time waited in queue for the operator in milliseconds.
        """
        if self.result_received_at is None or self.input_dispatched_at is None:
            return None
        return (self.input_dispatched_at - self.input_buffered_at)*1000.0

    @property
    def processing_latency(self) -> float:
        """Processing latency for the operator in milliseconds.
        """
        if self.result_received_at is None or self.input_dispatched_at is None:
            return None
        return (self.result_received_at - self.input_dispatched_at)*1000.0

class OperatorRuntimeStatisticsService:
    """This class maintains a set of active statistics records.
    """
    def __init__(self):
        self.active_records = set()

    def get_operator_runtime_statistics_record(self, operator, source, sequence_no):
        """Returns an operator runtime statistics records matching given operator and source stream. If one is not
        already found it will be created.
        """
        for record in self.active_records:
            if record.operator_id == operator.id \
                    and record.source_stream_id == source.stream_id \
                    and record.source_stream_sequence_no == sequence_no:
                return record

        record = OperatorRuntimeStatisticsRecord(operator.id, source.stream_id, sequence_no)
        self.active_records.add(record)
        return record

    def record_complete(self, record):
        """Mark a record complete, and remove it from the active records.

        """
        # TODO: Log the record after completion.
        self.active_records.remove(record)

class QoSMonitor(SparseSlice):
    """Quality of Service Monitor Slice maintains a coroutine for monitoring the runtime performance of the node.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.statistics_service = OperatorRuntimeStatisticsService()

    def operator_input_buffered(self, operator : StreamOperator, source, sequence_no):
        """Called when an input for a given operator from a source stream is buffered.
        """
        record = self.statistics_service.get_operator_runtime_statistics_record(operator, source, sequence_no)
        record.input_buffered()

    def operator_input_dispatched(self, operator : StreamOperator, source, sequence_no):
        """Called when an input for a given operator from a source stream is dispatched from buffer.
        """
        record = self.statistics_service.get_operator_runtime_statistics_record(operator, source, sequence_no)
        record.input_dispatched()

    def operator_result_received(self, operator : StreamOperator, source, sequence_no):
        """Called when a result for a given operator from a source stream is dispatched from buffer.
        """
        record = self.statistics_service.get_operator_runtime_statistics_record(operator, source, sequence_no)
        record.result_received()
        self.statistics_service.record_complete(record)
        self.logger.info("Operator %s queueing time: %.2f ms, processing latency: %.2f ms",
                          operator,
                          record.queueing_time,
                          record.processing_latency)
