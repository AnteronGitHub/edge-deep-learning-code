"""This module implements statistics file loader.
"""
import pandas as pd

class StatisticsFileLoader:
    """Statistics file loader parses statistics data from files created by sparse statistics module.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, stats_path = '/var/lib/sparse/stats'):
        self.stats_path = stats_path

    def load_dataframe(self, filepath):
        """Loads a data frame from file selected by the user.
        """
        try:
            return pd.read_csv(filepath)
        except FileNotFoundError:
            print(f"File '{filepath}' was not found in directory '{self.stats_path}'.")
            return None
