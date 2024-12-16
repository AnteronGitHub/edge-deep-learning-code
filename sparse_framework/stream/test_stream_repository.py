import unittest

from ..node_config import SparseNodeConfig
from ..module import ModuleRepository
from ..runtime import SparseRuntime
from ..stats import QoSMonitor

from .stream_repository import StreamRepository

class TestStreamRepository(unittest.TestCase):
    def setUp(self):
        self.config = SparseNodeConfig()
        self.qos_monitor = QoSMonitor(self.config)
        self.module_repo = ModuleRepository(self.config)
        self.runtime = SparseRuntime(self.module_repo, self.qos_monitor, self.config)

        self.stream_repository = StreamRepository(self.runtime)

    def test_get_stream_creates_a_new_stream_if_one_is_not_found(self):
        self.assertEqual(len(self.stream_repository.streams), 0)

        stream = self.stream_repository.get_stream()

        self.assertEqual(len(self.stream_repository.streams), 1)
