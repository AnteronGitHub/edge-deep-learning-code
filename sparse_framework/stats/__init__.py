"""This module implements collecting system statistics for performance evaluation, and possibly automatic scaling of
applications.
"""
from .qos_monitor import QoSMonitor

__all__ = (
        "QoSMonitor",
        )
