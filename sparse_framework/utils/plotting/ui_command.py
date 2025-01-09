"""This module contains ui command abstraction.
"""
import logging

from .statistics_file_loader import StatisticsFileLoader
from .data_formatter import DataFormatter
from .plotter import LatencyTimelinePlotter, \
                     LatencyBarplotPlotter, \
                     LatencyBoxplotPlotter, \
                     BatchSizeHistogramPlotter, \
                     BatchLatencyPlotter

class UICommand:
    """UI command represents functionality available to be invoked through the ui.
    """
    def __init__(self, ui, file_loader : StatisticsFileLoader, data_formatter : DataFormatter):
        self.ui = ui
        self.file_loader = file_loader
        self.data_formatter = data_formatter

        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s', level=logging.INFO)

    def __str__(self):
        return ""

    def run(self):
        """Runs the command.
        """

class PrintDataCommand(UICommand):
    """Command to print data from a file to output.
    """
    def __str__(self):
        return "Print DataFrame"

    def run(self):
        filepath = self.ui.select_data_file()
        df = self.file_loader.load_dataframe(filepath)
        df = self.data_formatter.to_runtime_latency(df)
        df = self.data_formatter.count_batch_sizes(df)
        self.logger.info("\n%s", str(df))

class PlotLatencyTimelineCommand(UICommand):
    """Command to plot latency timeline.
    """
    def __str__(self):
        return "Plot timeline"

    def run(self):
        filepath = self.ui.select_data_file()
        df = self.file_loader.load_dataframe(filepath)
        df = self.data_formatter.to_runtime_latency(df)
        LatencyTimelinePlotter().plot(df)
        self.logger.info("Plotted latency barplot.")

class PlotLatencyBarplotCommand(UICommand):
    """Command to plot latency barplot.
    """
    def __str__(self):
        return "Plot latency barplot"

    def run(self):
        filepath = self.ui.select_data_file()
        df = self.file_loader.load_dataframe(filepath)
        df = self.data_formatter.to_runtime_latency(df)
        df = self.data_formatter.group_by_no_sources(df)
        LatencyBarplotPlotter().plot(df)
        self.logger.info("Plotted latency barplot.")

class PlotLatencyBoxplotCommand(UICommand):
    """Command to plot latency boxplot.
    """
    def __str__(self):
        return "Plot latency boxplot"

    def run(self):
        filepath = self.ui.select_data_file()
        df = self.file_loader.load_dataframe(filepath)
        df = self.data_formatter.to_runtime_latency(df)
        df = self.data_formatter.group_by_no_sources(df)
        LatencyBoxplotPlotter().plot(df)
        self.logger.info("Plotted latency boxplot.")

class PlotBatchSizeHistogramCommand(UICommand):
    """Command to plot batch size distribution.
    """
    def __str__(self):
        return "Plot batch size distribution"

    def run(self):
        filepath = self.ui.select_data_file()
        df = self.file_loader.load_dataframe(filepath)
        df = self.data_formatter.to_runtime_latency(df)
        df = self.data_formatter.count_batch_sizes(df)
        BatchSizeHistogramPlotter().plot(df)
        self.logger.info("Plotted batch size distribution.")

class PlotBatchLatencyCommand(UICommand):
    """Command to plot batch processing latency.
    """
    def __str__(self):
        return "Plot batch latency variance"

    def run(self):
        filepath = self.ui.select_data_file()
        df = self.file_loader.load_dataframe(filepath)
        df = self.data_formatter.to_runtime_latency(df)
        df = self.data_formatter.count_batch_sizes(df)
        BatchLatencyPlotter().plot(df)
        self.logger.info("Plotted batch latency.")
