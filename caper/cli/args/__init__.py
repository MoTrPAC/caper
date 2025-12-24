"""Caper CLI argument dataclasses."""

from .analysis import CleanupArgs, GcpMonitorArgs, GcpResAnalysisArgs
from .base import get_abspath, namespace_to_dataclass, split_delimited
from .client import AbortArgs, ListArgs, MetadataArgs, TroubleshootArgs, UnholdArgs
from .hpc import HpcAbortArgs, HpcListArgs, HpcSubmitArgs
from .init import InitArgs
from .run import RunArgs
from .server import ServerArgs
from .submit import SubmitArgs

__all__ = [
    # Base helpers
    'get_abspath',
    'namespace_to_dataclass',
    'split_delimited',
    # Dataclasses
    'InitArgs',
    'RunArgs',
    'ServerArgs',
    'SubmitArgs',
    'AbortArgs',
    'UnholdArgs',
    'ListArgs',
    'MetadataArgs',
    'TroubleshootArgs',
    'GcpMonitorArgs',
    'GcpResAnalysisArgs',
    'CleanupArgs',
    'HpcSubmitArgs',
    'HpcListArgs',
    'HpcAbortArgs',
]
