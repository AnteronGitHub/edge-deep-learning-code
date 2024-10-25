import logging
import multiprocessing

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
            self.input_buffer.append((self.transferToDevice(input_data),
                                      source_stream,
                                      sequence_no,
                                      result_callback))

        self.logger.debug(f"{index+1} samples buffered.")
        return index

    def pop_input(self):
        with self.lock:
            input_data, source_stream, sequence_no, result_callback = self.input_buffer.pop(0)

        if self.qos_monitor is not None:
            self.qos_monitor.operator_input_dispatched(self.operator, source_stream, sequence_no)
        self.logger.debug(f"Dispatched sample from buffer.")

        return input_data, [result_callback]

    def dispatch_batch(self):
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
        self.logger.debug(f"Dispatched batch of {batch_size} samples from buffer.")

        return input_batch, callbacks

    def result_received(self, result, callbacks, use_batching : bool):
        transferred_result = self.transferToHost(result)

        if use_batching:
            for batch_index, callback in enumerate(callbacks):
                callback(transferred_result[batch_index])
                # callback(transferred_result[batch_index], batch_index)
        else:
            for callback in callbacks:
                callback(transferred_result)

    def transferToDevice(self, input_data):
        pass

    def transferToHost(self, output_data):
        pass

import torch

class SparsePytorchIOBuffer(SparseIOBuffer):
    """Memory buffer for Node's task executor.

    Buffer ensures that when a task is created for the executor, all of the needed data are available in the executor
    device memory. After the executor task has been processed, memory buffer transfers the data to the network device
    for result transmission.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.device = "cuda" if torch.cuda.is_available() else "cpu"

    def transferToDevice(self, tensor):
        return tensor.to(self.device)

    def transferToHost(self, tensor):
        return tensor.to("cpu")

    def dispatch_batch(self):
        input_batch, callbacks = super().dispatch_batch()

        return torch.cat(input_batch), callbacks
