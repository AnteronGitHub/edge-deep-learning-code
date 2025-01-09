"""This module implements data classes for runtime statistics records.
"""
from time import time

class OperatorRuntimeStatisticsRecord:
    """Operator runtime statistics record tracks tuple processing latency for a given operator.
    """
    # pylint: disable=too-many-instance-attributes
    operator_id : str
    operator_name : str
    source_stream_id : str
    batch_no : int
    input_buffered_at : float
    input_dispatched_at : float
    result_received_at : float

    def __init__(self, operator_id : str, operator_name : str, source_stream_id : str, source_stream_sequence_no : int):
        self.operator_id = operator_id
        self.operator_name = operator_name
        self.source_stream_id = source_stream_id
        self.source_stream_sequence_no = source_stream_sequence_no
        self.batch_no = None
        self.input_buffered_at = None
        self.input_dispatched_at = None
        self.result_received_at = None

    def input_buffered(self, batch_no : int):
        """Called when an input tuple has been buffered for the task executor.
        """
        self.batch_no = batch_no
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
        if self.input_buffered_at is None or self.input_dispatched_at is None:
            return None
        return (self.input_dispatched_at - self.input_buffered_at)*1000.0

    @property
    def processing_latency(self) -> float:
        """Processing latency for the operator in milliseconds.
        """
        if self.result_received_at is None or self.input_dispatched_at is None:
            return None
        return (self.result_received_at - self.input_dispatched_at)*1000.0

    def csv_header(self) -> str:
        """Returns csv header row matching the record.
        """
        return "operator_id,operator_name,source_stream_id,batch_no," \
             + "input_buffered_at,input_dispatched_at,result_received_at\n"

    def to_csv(self, started_at) -> str:
        """Formats the record into csv.
        """
        return f"{self.operator_id},{self.operator_name},{self.source_stream_id},{self.batch_no}," \
             + f"{self.input_buffered_at - started_at},{self.input_dispatched_at - started_at}," \
             + f"{self.result_received_at - started_at}\n"
