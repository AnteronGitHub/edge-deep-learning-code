"""This module implements logging statistics records into files.
"""
import logging
import os
from datetime import datetime
from time import time

from .operator_runtime_statistics_record import OperatorRuntimeStatisticsRecord

class FileLogger:
    """This class maintains log files for logging statistics records into.
    """
    def __init__(self, data_path : str):
        self.logger = logging.getLogger("FileLogger")

        self.data_path = data_path
        os.makedirs(data_path, exist_ok=True)

        self.started_at = time()

        self.files = {}

    def get_runtime_statistics_file(self, record : OperatorRuntimeStatisticsRecord):
        """Returns a runtime statistics file matching the given record.
        """
        time_formatted = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name = f"rtstats_{record.operator_name}_{time_formatted}.csv"

        if record.operator_name not in self.files:
            filepath = os.path.join(self.data_path, log_file_name)
            self.files[record.operator_name] = filepath
            if not os.path.exists(filepath):
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(record.csv_header())

        return self.files[record.operator_name]

    def write_runtime_statistics(self, record : OperatorRuntimeStatisticsRecord):
        """Appends record into the appropriate statistics file.
        """
        file_path = self.get_runtime_statistics_file(record)
        self.logger.info("Writing to log file %s", file_path)

        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(record.to_csv(self.started_at))
