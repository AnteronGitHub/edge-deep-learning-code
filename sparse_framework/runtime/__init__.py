"""This module contains the stream operator runtime implementation.
"""
from .operator import StreamOperator
from .runtime import SparseRuntime

__all__ = ("StreamOperator",
           "SparseRuntime")
