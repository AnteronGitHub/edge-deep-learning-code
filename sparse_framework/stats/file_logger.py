"""This module implements logging statistics records into files.
"""
import logging
import os

class FileLogger:
    """This class maintains log files for logging statistics records into.
    """
    def __init__(self, data_dir = '/data/stats'):
        self.logger = logging.getLogger("sparse")

        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self.files = {}

    def get_log_file(self, record):
        """Returns a log file matching the given record.
        """
        statistics = type(record).__name__
        log_file_name = f"{statistics}.csv"
        if statistics not in self.files:
            filepath = os.path.join(self.data_dir, log_file_name)
            self.files[statistics] = filepath
            if not os.path.exists(filepath):
                with open(self.files[statistics], 'w', encoding='utf-8') as f:
                    f.write(record.csv_header())
        return self.files[statistics]

    def log_record(self, record):
        """Logs a record to the corresponding log file.
        """
        file_path = self.get_log_file(record)
        if file_path is not None:
            with open(file_path, 'a', 'w', encoding='utf-8') as f:
                f.write(record.to_csv())
