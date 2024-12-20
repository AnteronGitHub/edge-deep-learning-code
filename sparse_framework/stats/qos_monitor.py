"""This module contains Quality of Service (QoS) monitor implementation.
"""
import multiprocessing

from ..runtime.operator import StreamOperator
from ..sparse_slice import SparseSlice

from .file_logger import FileLogger
from .operator_runtime_statistics_record import OperatorRuntimeStatisticsRecord

class OperatorRuntimeStatisticsService:
    """This class maintains a set of active statistics records.
    """
    def __init__(self, data_path : str):
        self.lock = multiprocessing.Manager().Lock()
        self.active_records = set()
        self.file_logger = FileLogger(data_path)

    def get_operator_runtime_statistics_record(self, operator : StreamOperator, source, sequence_no):
        """Returns an operator runtime statistics records matching given operator and source stream. If one is not
        already found it will be created.
        """
        with self.lock:
            for record in self.active_records:
                if record.operator_id == operator.id \
                        and record.source_stream_id == source.stream_id \
                        and record.source_stream_sequence_no == sequence_no:
                    return record

            record = OperatorRuntimeStatisticsRecord(operator.id, operator.name, source.stream_id, sequence_no)
            self.active_records.add(record)
            return record

    def record_complete(self, record : OperatorRuntimeStatisticsRecord):
        """Mark a record complete, and remove it from the active records.

        """
        with self.lock:
            self.active_records.remove(record)

        self.file_logger.write_runtime_statistics(record)

class QoSMonitor(SparseSlice):
    """Quality of Service Monitor Slice maintains a coroutine for monitoring the runtime performance of the node.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.statistics_service = None

    def get_futures(self, futures):
        self.statistics_service = OperatorRuntimeStatisticsService(self.config.data_path)

        return futures

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
        self.logger.debug("Operator %s queueing time: %.2f ms, processing latency: %.2f ms",
                          operator,
                          record.queueing_time,
                          record.processing_latency)
