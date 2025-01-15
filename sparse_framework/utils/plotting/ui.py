"""This module implements plotting functionality for visualizing sparse benchmark data.
"""
import logging
import os

from simple_term_menu import TerminalMenu

from .data_formatter import DataFormatter
from .statistics_file_loader import StatisticsFileLoader
from .ui_command import PrintDataCommand, \
                        PlotLatencyTimelineCommand, \
                        PlotLatencyBarplotCommand, \
                        PlotLatencyBoxplotCommand, \
                        PlotBatchSizeHistogramCommand, \
                        PlotBatchLatencyCommand

class PlotterUserInterface:
    """Plotter user interface interacts with the user to collects specifications for the plots to be created.
    """
    def __init__(self):
        self.logger = logging.getLogger("PlotterUserInterface")
        self.data_formatter = DataFormatter()
        self.file_loader = StatisticsFileLoader()
        logging.basicConfig(format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s', level=logging.INFO)

        self.commands = []
        self.commands.append(PrintDataCommand(self, self.file_loader, self.data_formatter))
        self.commands.append(PlotLatencyTimelineCommand(self, self.file_loader, self.data_formatter))
        self.commands.append(PlotLatencyBarplotCommand(self, self.file_loader, self.data_formatter))
        self.commands.append(PlotLatencyBoxplotCommand(self, self.file_loader, self.data_formatter))
        self.commands.append(PlotBatchSizeHistogramCommand(self, self.file_loader, self.data_formatter))
        self.commands.append(PlotBatchLatencyCommand(self, self.file_loader, self.data_formatter))

    def select_from_options(self, options, title) -> str:
        """Prompts use options and selects one from them.
        """
        terminal_menu = TerminalMenu(options, title=title)
        menu_entry_index = terminal_menu.show()
        if menu_entry_index is None:
            return None
        print(f"{title} {options[menu_entry_index]}")
        return options[menu_entry_index]

    def select_data_file(self):
        """Selects a data frame file.
        """
        data_files = [path for path in os.listdir(self.file_loader.stats_path) if path.endswith(".csv")]
        data_files.sort()
        filename = self.select_from_options(data_files, "Dataframe file name:")

        if filename is None:
            return None
        return os.path.join(self.file_loader.stats_path, filename)

    def start(self):
        """Starts the user interface.
        """
        operation = self.select_from_options([str(cmd) for cmd in self.commands], "Select operation:")

        for cmd in self.commands:
            if str(cmd) == operation:
                cmd.run()
                return
