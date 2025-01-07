"""This module implements plotting functionality for visualizing sparse benchmark data.
"""
import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

from simple_term_menu import TerminalMenu

pd.options.mode.chained_assignment = None

class StatisticsFileLoader:
    """Statistics file loader parses statistics data from files created by sparse statistics module.
    """
    def __init__(self, stats_path = '/var/lib/sparse/stats'):
        self.stats_path = stats_path

    def select_from_options(self, options, title) -> str:
        """Prompts use options and selects one from them.
        """
        terminal_menu = TerminalMenu(options, title=title)
        menu_entry_index = terminal_menu.show()
        if menu_entry_index is None:
            return None
        print(f"{title} {options[menu_entry_index]}")
        return options[menu_entry_index]

    def load_dataframe(self, dataframe_type = None):
        """Loads a data frame from file selected by the user.
        """
        if dataframe_type is None:
            dataframe_type = self.select_from_options(["ClientRequestStatisticsRecord", \
                                                       "ServerRequestStatisticsRecord"], \
                                                      "Data frame type:")

        data_files = [path for path in os.listdir(self.stats_path) \
                                    if path.startswith(dataframe_type) and path.endswith(".csv")]
        data_files.sort()
        filename = self.select_from_options(data_files, "Dataframe file name:")

        if filename is None:
            return None, None
        filepath = os.path.join(self.stats_path, filename)

        try:
            return dataframe_type, pd.read_csv(filepath)
        except FileNotFoundError:
            print(f"File '{filename}' was not found in directory '{self.stats_path}'.")
            return None, None

    def parse_boxplot_frame(self):
        """Parses boxplot data from file selected by the user.
        """
        dataframe_type = "ClientRequestStatisticsRecord"
        _, df = self.load_dataframe(dataframe_type)

        if df is None:
            return None

        no_datasources = input("Number of data sources in experiment: ")
        used_scheduling = self.select_from_options(["FCFS", "B-FCFS", "PS", "B-PS"], "Scheduling method:")
        df = df.assign(Connections=int(no_datasources), Scheduling=used_scheduling)

        # Statistics only for offloaded tasks
        df = df.loc[df['request_op']=='offload_task']

        return df

    def parse_scales(self):
        """Parse plot options from the user.
        """
        title = input("Plot title: ")
        try:
            start_at = float(input("Start at timestamp: "))
        except ValueError:
            start_at = 0.0
        try:
            period_length = float(input("Period length: "))
        except ValueError:
            period_length = -1.0
        try:
            y_min = float(input("Min y: "))
        except ValueError:
            y_min = -1
        try:
            y_max = float(input("Max y: "))
        except ValueError:
            y_max = -1

        return title, start_at, period_length, y_min, y_max

class StatisticsGraphPlotter:
    """Plots graphs from benchmark data.
    """
    def __init__(self, write_path = '/var/lib/sparse/stats'):
        self.file_loader = StatisticsFileLoader()
        self.write_path = write_path

    def count_offload_task_client_statistics_legacy(self, df, start_at = 0.0, period_length = -1.0):
        """Transforms statistics data to appropriate format for plotting.
        """
        df = df.rename(columns={ 'processing_started':'request_sent_at',
                                 'latency': 'Latency (ms)',
                                 'node_id': 'connection_id',
                                 'offload_latency': 'Offload latency (ms)' })

        # Scale latencies to milliseconds
        df['Latency (ms)'] = df['Latency (ms)'].apply(lambda x: 1000.0*x)
        df['Offload latency (ms)'] = df['Offload latency (ms)'].apply(lambda x: 1000.0*x)

        # Drop first rows to ignore 'slow start'
        # (See e.g: https://stackoverflow.com/questions/64420777/opencv-cuda-api-very-slow-at-the-first-call)
        df = df[df["request_sent_at"] >= start_at]
        if period_length > 0.0:
            df = df[df["request_sent_at"] <= start_at + period_length]
        df["request_sent_at"] = df["request_sent_at"].apply(lambda x: x-start_at)

        return df.set_index("request_sent_at")

    def count_offload_task_client_statistics(self, df, start_at = 0.0, period_length = -1.0):
        """Calculates latencies from a Dataframe with time stamps, and returns a new DataFrame with the results.

        Result DataFrame uses 'request_sent_at' timestamp as the index.
        """
        df = df.loc[df['request_op']=='offload_task']

        df['e2e_latency'] = df['response_received_at'] - df['processing_started_at']
        df['offload_latency'] = df['response_received_at'] - df['request_sent_at']

        df = df.rename(columns={ 'e2e_latency': 'Latency (ms)',
                                 'offload_latency': 'Offload latency (ms)',
                                 'node_id': 'connection_id' })

        # Scale latencies to milliseconds
        df['Latency (ms)'] = df['Latency (ms)'].apply(lambda x: 1000.0*x)
        df['Offload latency (ms)'] = df['Offload latency (ms)'].apply(lambda x: 1000.0*x)

        # Drop first rows to ignore 'slow start'
        # (See e.g: https://stackoverflow.com/questions/64420777/opencv-cuda-api-very-slow-at-the-first-call)
        df = df[df["request_sent_at"] >= start_at]
        if period_length > 0.0:
            df = df[df["request_sent_at"] <= start_at + period_length]
        df["request_sent_at"] = df["request_sent_at"].apply(lambda x: x-start_at)

        return df.set_index("request_sent_at")

    def count_offload_task_server_statistics(self, df, start_at = 0.0, period_length = -1.0):
        """Transforms server task statistics into appropriate format for plotting.
        """
        df = df.loc[df['request_op']=='offload_task']

        df['e2e_latency'] = df['response_sent_at'] - df['request_received_at']
        df['rx_latency'] = df['task_queued_at'] - df['request_received_at']
        df['queueing_time'] = df['task_started_at'] - df['task_queued_at']
        df['task_latency'] = df['task_completed_at'] - df['task_started_at']
        df['tx_latency'] = df['response_sent_at'] - df['task_completed_at']

        # Scale latencies to milliseconds
        for column in ['e2e_latency', 'rx_latency', 'queueing_time', 'task_latency', 'tx_latency']:
            df[column] = df[column].apply(lambda x: 1000.0*x)

        # Rename columns
        df = df.rename(columns={ 'e2e_latency': 'Service time (ms)',
                                 'rx_latency': 'RX latency (ms)',
                                 'queueing_time': 'Queueing time (ms)',
                                 'task_latency': 'Task latency (ms)',
                                 'tx_latency': 'TX latency (ms)' })

        # Translate time axis
        df = df[df["request_received_at"] >= start_at]
        if period_length > 0.0:
            df = df[df["request_received_at"] <= start_at + period_length]
        df["request_received_at"] = df["request_received_at"].apply(lambda x: x-start_at)

        return df.set_index("request_received_at")

    def count_offload_task_server_batch_statistics(self, df, start_at = 0.0, period_length = -1.0):
        """Transforms server batch statistics into appropriate format for plotting.
        """
        df = df.loc[df['request_op']=='offload_task']
        df = df[df["task_started_at"] >= start_at]

        if period_length > 0.0:
            df = df[df["task_started_at"] <= start_at + period_length]

        result_df = pd.DataFrame([], columns=["Task started (s)", "Task latency (ms)", "Batch Size"])

        batch_size = task_started = task_latency = 0
        batch_no = -1
        for i in df.index:
            if df["batch_no"][i] == batch_no:
                batch_size += 1
            else:
                if batch_size > 0:
                    result_df.loc[len(result_df.index)] = [task_started, task_latency, batch_size]
                batch_no = df["batch_no"][i]

                task_started = df["task_started_at"][i]
                task_latency = 1000.0 * (df["task_completed_at"][i] - df["task_started_at"][i])
                batch_size = 1

        result_df["Task started (s)"] = result_df["Task started (s)"].apply(lambda x: x-start_at)
        return result_df.set_index("Task started (s)")

    def print_statistics(self):
        """Prints statistics into output instead of plotting.
        """
        _, df = self.file_loader.load_dataframe()
        batch_stats = self.count_offload_task_server_batch_statistics(df)
        print(batch_stats)

        # if dataframe_type == "ClientRequestStatisticsRecord":
        #     # Calculate statistics for offloaded tasks
        #     if legacy:
        #         stats = self.count_offload_task_client_statistics_legacy(df)
        #     else:
        #         stats = self.count_offload_task_client_statistics(df)
        #     # Print statistics
        #     print(stats[['Latency (ms)', 'Offload latency (ms)']].describe())
        # else:
        #     stats = self.count_offload_task_server_statistics(df)
        #     print(stats[['Service time (ms)', \
        #                  'RX latency (ms)', \
        #                  'Queueing time (ms)', \
        #                  'Task latency (ms)', \
        #                  'TX latency (ms)']].describe())

    def plot_latency_timeline(self, latency = False):
        """Creates a latency timeline plot.
        """
        dataframe_type, df = self.file_loader.load_dataframe()
        title, start_at, period_length, y_min, y_max = self.file_loader.parse_scales()

        marker = 'o' if (input("Use marker in plot points (y/N): ")) == "y" else ''

        # Count analytics
        if dataframe_type == "ClientRequestStatisticsRecord":
            if latency:
                stats = self.count_offload_task_client_statistics_legacy(df, start_at, period_length)
            else:
                stats = self.count_offload_task_client_statistics(df, start_at, period_length)
        else:
            stats = self.count_offload_task_server_statistics(df, start_at, period_length)

        # Plot graph
        ylabel = "Offload latency (ms)" if dataframe_type == "ClientRequestStatisticsRecord" else "Service time (ms)"
        xlabel = "Request sent (s)" if dataframe_type == "ClientRequestStatisticsRecord" else "Request received (s)"
        plt.rcParams.update({ 'font.size': 32 })
        _, ax = plt.subplots(figsize=(12,8))
        for label, data in stats.groupby("connection_id"):
            data.plot(y=ylabel, ax=ax, label=label, marker=marker)

        plt.title(title)
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)
        plt.grid()

        ax.get_legend().remove()
        if period_length > 0:
            ax.set_xlim([0, period_length])

        if y_min > 0 or y_max > 0:
            ax.set_ylim([y_min, y_max])

        filepath = os.path.join(self.write_path, f"{dataframe_type}_latency_timeline.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=400)
        print(f"Saved column plot to '{filepath}'")

    def plot_benchmark_barplot(self):
        """Creates a server-side latency barplot.
        """
        title, _, _, _, y_max = self.file_loader.parse_scales()

        plt.figure(figsize=(16,8))

        plt.rcParams.update({ 'font.size': 18 })

        barplot_data = pd.DataFrame([], columns=["Connections", "RX", 'Queueing', 'Task', "TX"])
        dataframe_type = "ServerRequestStatisticsRecord"
        while True:
            _, df = self.file_loader.load_dataframe(dataframe_type)
            if df is None:
                break

            no_connections = int(input("Number of connections: "))
            stats = self.count_offload_task_server_statistics(df)

            barplot_data.loc[len(barplot_data.index)] = [int(no_connections),
                                                         stats.loc[:, 'RX latency (ms)'].mean(),
                                                         stats.loc[:, 'Queueing time (ms)'].mean(),
                                                         stats.loc[:, 'Task latency (ms)'].mean(),
                                                         stats.loc[:, 'TX latency (ms)'].mean()]

        barplot_data.Connections = barplot_data.Connections.astype(int)
        barplot_data = barplot_data.set_index("Connections")

        ax = barplot_data.plot.bar(rot=0, stacked=True)

        plt.title(title)
        plt.ylabel("Latency (ms)")

        if y_max > 0:
            ax.set_ylim([0, y_max])

        filepath = os.path.join(self.write_path, f"{dataframe_type}_latency_barplot.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=400)
        print(f"Saved benchmark barplot to '{filepath}'")

    def plot_offload_latency_boxplot(self):
        """Creates a client-side latency boxplot.
        """
        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(12,8))

        dataframe_type = "ClientRequestStatisticsRecord"
        frames = []
        _, start_at, period_length, y_min, y_max = self.file_loader.parse_scales()
        while True:
            df = self.file_loader.parse_boxplot_frame()
            if df is None:
                break

            frames.append(self.count_offload_task_client_statistics(df, start_at, period_length))

        ax = sns.boxplot(x="Connections",
                         y="Offload latency (ms)",
                         hue="Scheduling",
                         data=pd.concat(frames),
                         palette="dark:grey",
                         showfliers=False)

        if y_min > 0 or y_max > 0:
            ax.set_ylim([y_min, y_max])

        filepath = os.path.join(self.write_path, f"{dataframe_type}_latency_boxplot.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=400)
        print(f"Saved offload latency boxplot to '{filepath}'")

    def plot_batch_distribution(self):
        """Creates a server-side batch size distribution plot.
        """
        title, start_at, period_length, _, _ = self.file_loader.parse_scales()

        dataframe_type = "ServerRequestStatisticsRecord"
        _, df = self.file_loader.load_dataframe(dataframe_type)
        no_connections = int(input("Number of connections: "))
        stats = self.count_offload_task_server_batch_statistics(df, start_at, period_length)

        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(24,8))

        stats["Batch Size"] = stats["Batch Size"].astype(int)
        stats.hist(column="Batch Size", legend=False, bins=no_connections)

        plt.grid(None)
        plt.xlabel("Batch size")
        plt.title(title)
        plt.xlim([0, no_connections + 1])
        plt.tick_params(left = False, labelleft = False)
        plt.tight_layout()

        filepath = os.path.join(self.write_path, f"{dataframe_type}_batch_size_histogram.png")
        plt.savefig(filepath, dpi=400)
        print(f"Saved batch size histogram to '{filepath}'")

    def plot_batch_latency_boxplot(self):
        """Creates a server-side batch latency box plot.
        """
        title, start_at, period_length, _, _ = self.file_loader.parse_scales()

        dataframe_type = "ServerRequestStatisticsRecord"
        frames = []
        while True:
            _, df = self.file_loader.load_dataframe(dataframe_type)
            if df is None:
                break

            frames.append(self.count_offload_task_server_batch_statistics(df, start_at, period_length))
        stats = pd.concat(frames)

        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(24,8))

        stats["Batch Size"] = stats["Batch Size"].astype(int)
        max_batch_size = stats["Batch Size"].max()

        plt.rcParams.update({ 'font.size': 24 })
        plt.figure(figsize=(12,8))
        sns.boxplot(x="Batch Size", \
                    y="Task latency (ms)", \
                    data=stats, \
                    showfliers=False)

        plt.title(title)
        plt.xticks(np.arange(max_batch_size, step=10))
        plt.tight_layout()

        filepath = os.path.join(self.write_path, f"{dataframe_type}_batch_size_latency_boxplot.png")
        plt.savefig(filepath, dpi=400)
        print(f"Saved task latency boxplot to '{filepath}'")

if __name__ == "__main__":
    plotter = StatisticsGraphPlotter()
    operation = plotter.file_loader.select_from_options(["Print DataFrame", \
                                                         "Plot timeline", \
                                                         "Plot barplot", \
                                                         "Plot boxplot", \
                                                         "Plot batch size distribution", \
                                                         "Plot batch latency variance"], \
                                                        "Select operation:")
    use_legacy = bool(input("Legacy format y/N: "))
    if operation == "Print DataFrame":
        plotter.print_statistics()
    elif operation == "Plot timeline":
        plotter.plot_latency_timeline(use_legacy)
    elif operation == "Plot barplot":
        plotter.plot_benchmark_barplot()
    elif operation == "Plot boxplot":
        plotter.plot_offload_latency_boxplot()
    elif operation == "Plot batch size distribution":
        plotter.plot_batch_distribution()
    else:
        plotter.plot_batch_latency_boxplot()
