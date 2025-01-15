"""This moudle implements statistics data formatting functionality.
"""
import pandas as pd

from pandas.core.frame import DataFrame

class DataFormatter:
    """Transforms data frames from format generated by sparse cluster to formats needed by the plotters.
    """
    def get_available_operator_names(self, df : DataFrame):
        """Returns a list of operator names included in the data frame.
        """
        return pd.unique(df["operator_name"])

    def to_runtime_latency(self, df : DataFrame, start_at = None, period_length = None):
        """Calculates latencies from a Dataframe with time stamps, and returns a new DataFrame with the results.

        Result DataFrame uses 'request_sent_at' timestamp as the index.
        """
        df['queueing_latency'] = df['input_dispatched_at'] - df['input_buffered_at']
        df['processing_latency'] = df['result_received_at'] - df['input_dispatched_at']

        # Scale latencies to milliseconds
        df['queueing_latency'] = df['queueing_latency'].apply(lambda x: 1000.0*x)
        df['processing_latency'] = df['processing_latency'].apply(lambda x: 1000.0*x)

        if start_at is not None:
            df = df[df["input_buffered_at"] >= start_at]
        if period_length is not None:
            df = df[df["input_buffered_at"] <= start_at + period_length]

        # Use the first task start as the time origo
        first_input_buffered_at = df["input_buffered_at"][0]
        df["input_buffered_at"] = df["input_buffered_at"].apply(lambda x: x-first_input_buffered_at)

        df = df.drop(columns=["operator_id", "input_dispatched_at", "result_received_at"])

        return df.set_index("input_buffered_at")

    def filter_by_operator_name(self, df : DataFrame, operator_name):
        """Filters rows in a dataframe by a given operator name.
        """
        return df.loc[df['operator_name']==operator_name]

    def group_by_no_sources(self, df : DataFrame):
        """Groups dataframe by the number of distinct source streams.
        """
        len(pd.unique(df["source_stream_id"]))
        return df.assign(no_sources=len(pd.unique(df["source_stream_id"])), scheduling="pause-frames")

    def count_batch_sizes(self, df : DataFrame):
        """Groups dataframe by the number of distinct source streams.
        """
        batch_sizes = {}
        for batch_no in pd.unique(df["batch_no"]):
            batch_size = df[df.batch_no == batch_no].shape[0]
            batch_sizes[batch_no] = batch_size

        df["batch_no"] = df["batch_no"].apply(lambda x: batch_sizes[x])

        return df.rename(columns={ 'batch_no': 'batch_size' })
