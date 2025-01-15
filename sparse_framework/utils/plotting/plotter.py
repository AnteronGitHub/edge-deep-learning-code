"""This module includes plotting related functionality.
"""
import dataclasses
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from pandas.core.frame import DataFrame

pd.options.mode.chained_assignment = None

@dataclasses.dataclass
class PlottingOptions:
    """Options available for plotting.
    """
    operator_name : str
    use_markers : bool

    def __init__(self, operator_name : str, use_markers : bool = False):
        self.operator_name = operator_name
        self.use_markers = use_markers

class StatisticsGraphPlotter:
    """Plots graphs from benchmark data.
    """
    def __init__(self, write_path = "dist"):
        self.write_path = write_path

    @property
    def file_name(self):
        """File name for this plot type.
        """
        return "statistics_plot.png"

    def plot(self, df : DataFrame, options : PlottingOptions):
        """Creates the plot in the specified write path.
        """

class LatencyTimelinePlotter(StatisticsGraphPlotter):
    """Latency timeline plot shows the latency as a function of the time when the task data was buffered.
    """
    @property
    def file_name(self):
        return "latency_timeline.png"

    def plot(self, df : DataFrame, options : PlottingOptions):
        marker = 'o' if options.use_markers else ''

        ylabel = "processing_latency"
        xlabel = "input_buffered_at"
        plt.rcParams.update({ 'font.size': 32 })
        _, ax = plt.subplots(figsize=(12,8))
        for label, data in df.groupby("source_stream_id"):
            data.plot(y=ylabel, ax=ax, label=label, marker=marker)

        plt.title(options.operator_name)
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)
        plt.grid()

        ax.get_legend().remove()

        filepath = os.path.join(self.write_path, self.file_name)
        plt.tight_layout()
        plt.savefig(filepath, dpi=400)
        print(f"Saved column plot to '{filepath}'")

class LatencyBarplotPlotter(StatisticsGraphPlotter):
    """Latency barplot shows the average latency of different phases.
    """
    @property
    def file_name(self):
        return "latency_barplot.png"

    def plot(self, df : DataFrame, options : PlottingOptions):
        plt.figure(figsize=(16,8))

        plt.rcParams.update({ 'font.size': 18 })

        barplot_data = pd.DataFrame([], columns=["Streams", "Queueing", "Processing"])

        no_sources = df["no_sources"][0]

        barplot_data.loc[len(barplot_data.index)] = [int(no_sources),
                                                     df.loc[:, 'queueing_latency'].mean(),
                                                     df.loc[:, 'processing_latency'].mean()]

        barplot_data.Streams = barplot_data.Streams.astype(int)
        barplot_data = barplot_data.set_index("Streams")

        barplot_data.plot.bar(rot=0, stacked=True)

        plt.title(options.operator_name)
        plt.ylabel("Avg. Latency (ms)")

        filepath = os.path.join(self.write_path, self.file_name)
        plt.tight_layout()
        plt.savefig(filepath, dpi=400)

class LatencyBoxplotPlotter(StatisticsGraphPlotter):
    """Latency box shows the confidence intervals for latencies in all of the requests.
    """
    @property
    def file_name(self):
        return "latency_boxplot.png"

    def plot(self, df : DataFrame, options : PlottingOptions):
        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(12,8))


        sns.boxplot(x="no_sources",
                    y="processing_latency",
                    hue="scheduling",
                    data=df,
                    palette="dark:grey",
                    showfliers=False)

        filepath = os.path.join(self.write_path, self.file_name)
        plt.title(options.operator_name)
        plt.ylabel("Processing Latency (ms)")
        plt.xlabel("Streams")
        plt.tight_layout()
        plt.savefig(filepath, dpi=400)

class BatchSizeHistogramPlotter(StatisticsGraphPlotter):
    """Batch size distribution plot shows the distribution of batch sizes for an operator.
    """
    @property
    def file_name(self):
        return "batch_size_histogram.png"

    def plot(self, df : DataFrame, options : PlottingOptions):
        max_batch_size = df["batch_size"].max()

        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(24,8))

        df.hist(column="batch_size", legend=False, bins=max_batch_size)

        plt.grid(None)
        plt.xlabel("Batch Size")
        plt.title(options.operator_name)
        plt.xlim([0, max_batch_size + 1])
        plt.tick_params(left = False, labelleft = False)
        plt.tight_layout()

        filepath = os.path.join(self.write_path, self.file_name)
        plt.savefig(filepath, dpi=400)

class BatchLatencyPlotter(StatisticsGraphPlotter):
    """Batch latency plotter plots batch processing latency as a function of the batch size.
    """
    @property
    def file_name(self):
        return "batch_size_latency_boxplot.png"

    def plot(self, df : DataFrame, options : PlottingOptions):
        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(24,8))

        max_batch_size = df["batch_size"].max()

        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(12,8))
        sns.boxplot(x="batch_size", \
                    y="processing_latency", \
                    data=df, \
                    showfliers=False)

        plt.title(options.operator_name)
        plt.xlabel("Batch Size")
        plt.ylabel("Processing latency (ms)")
        plt.xticks(np.arange(max_batch_size, step=10))
        plt.tight_layout()

        filepath = os.path.join(self.write_path, self.file_name)
        plt.savefig(filepath, dpi=400)
