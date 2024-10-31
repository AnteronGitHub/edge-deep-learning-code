"""This module contains stream operator IO buffer implementation.
"""
import logging
import multiprocessing
import torch

__all__ = ["SparseIOBuffer"]

class SparseIOBuffer:
    """Memory buffer for Node's task executor.

    Buffer ensures that when a task is created for the executor, all of the needed data are available in the executor
    device memory. After the executor task has been processed, memory buffer transfers the data to the network device
    for result transmission.
    """

    def __init__(self, operator, qos_monitor = None):
        self.operator = operator
        self.qos_monitor = qos_monitor

        self.logger = logging.getLogger("SparseIOBuffer")
        m = multiprocessing.Manager()
        self.lock = m.Lock()
        self.input_buffer = []

    def buffer_input(self, input_data, source_stream, sequence_no, result_callback) -> int:
        """Appends an input tensor to the specified model's input buffer and returns its index.
        """
        with self.lock:
            index = len(self.input_buffer)
            self.input_buffer.append((self.load_to_device(input_data),
                                      source_stream,
                                      sequence_no,
                                      result_callback))

        self.logger.debug("%d samples buffered.", index+1)
        return index

    def pop_input(self):
        """Read and remove a single tuple from the buffer.
        """
        with self.lock:
            input_data, source_stream, sequence_no, result_callback = self.input_buffer.pop(0)

        if self.qos_monitor is not None:
            self.qos_monitor.operator_input_dispatched(self.operator, source_stream, sequence_no)
        self.logger.debug("Dispatched sample from buffer.")

        return input_data, [result_callback]

    def dispatch_batch(self):
        """Read and remove all of the tuples from the buffer.
        """
        with self.lock:
            task_data_batch = self.input_buffer
            self.input_buffer = []

        input_batch = []
        callbacks = []
        batch_size = 0
        for input_data, source_stream, sequence_no, result_callback in task_data_batch:
            input_batch.append(input_data)
            callbacks.append(result_callback)
            if self.qos_monitor is not None:
                self.qos_monitor.operator_input_dispatched(self.operator, source_stream, sequence_no)
            batch_size += 1
        self.logger.debug("Dispatched batch of %d samples from buffer.", batch_size)

        return input_batch, callbacks

    def result_received(self, result, callbacks, use_batching : bool):
        """Callback for when a task executor has completed processing a tuple.
        """
        transferred_result = self.load_to_host(result)

        if use_batching:
            for batch_index, callback in enumerate(callbacks):
                callback(transferred_result[batch_index])
                # callback(transferred_result[batch_index], batch_index)
        else:
            for callback in callbacks:
                callback(transferred_result)

    def load_to_device(self, data_tuple):
        """Transfer a tuple from the host memory to the accelerator (e.g. GPU) memory.

        This function will likely need to be implemented based on the semantics of the accelerator api.
        """
        return data_tuple

    def load_to_host(self, data_tuple):
        """Transfer a tuple from the accelerator (e.g. GPU) memory to the host memory.

        This function will likely need to be implemented based on the semantics of the accelerator api.
        """
        return data_tuple

class SparsePytorchIOBuffer(SparseIOBuffer):
    """Memory buffer for Node's task executor.

    Buffer ensures that when a task is created for the executor, all of the needed data are available in the executor
    device memory. After the executor task has been processed, memory buffer transfers the data to the network device
    for result transmission.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.device = "cuda" if torch.cuda.is_available() else "cpu"

    def load_to_device(self, data_tuple):
        return data_tuple.to(self.device)

    def load_to_host(self, data_tuple):
        return data_tuple.to("cpu")

    def dispatch_batch(self):
        input_batch, callbacks = super().dispatch_batch()

        return torch.cat(input_batch), callbacks
