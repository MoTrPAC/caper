"""Parser building functions for Caper CLI using dataclass-driven approach."""

from __future__ import annotations

from typing import TYPE_CHECKING

from caper.cli.args import (
    AbortArgs,
    CleanupArgs,
    GcpMonitorArgs,
    GcpResAnalysisArgs,
    HpcAbortArgs,
    HpcListArgs,
    HpcSubmitArgs,
    InitArgs,
    ListArgs,
    MetadataArgs,
    RunArgs,
    ServerArgs,
    SubmitArgs,
    TroubleshootArgs,
    UnholdArgs,
    add_dataclass_args,
)

if TYPE_CHECKING:
    import argparse
    from argparse import ArgumentParser


def build_init(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'init' subcommand."""
    p = subparsers.add_parser(
        'init',
        aliases=(),
        help="Initialize Caper's configuration file",
    )
    add_dataclass_args(p, InitArgs)
    return p


def build_run(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'run' subcommand."""
    p = subparsers.add_parser(
        'run',
        aliases=('local', 'exec'),
        help='Run a single workflow without server',
    )
    add_dataclass_args(p, RunArgs)
    return p


def build_server(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'server' subcommand."""
    p = subparsers.add_parser(
        'server',
        aliases=('srv',),
        help='Run a Cromwell server',
    )
    add_dataclass_args(p, ServerArgs)
    return p


def build_submit(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'submit' subcommand."""
    p = subparsers.add_parser(
        'submit',
        aliases=('sub',),
        help='Submit a workflow to a Cromwell server',
    )
    add_dataclass_args(p, SubmitArgs)
    return p


def build_abort(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'abort' subcommand."""
    p = subparsers.add_parser(
        'abort',
        aliases=(),
        help='Abort running/pending workflows on a Cromwell server',
    )
    add_dataclass_args(p, AbortArgs)
    return p


def build_unhold(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'unhold' subcommand."""
    p = subparsers.add_parser(
        'unhold',
        aliases=(),
        help='Release hold of workflows on a Cromwell server',
    )
    add_dataclass_args(p, UnholdArgs)
    return p


def build_list(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'list' subcommand."""
    p = subparsers.add_parser(
        'list',
        aliases=('ls',),
        help='List running/pending workflows on a Cromwell server',
    )
    add_dataclass_args(p, ListArgs)
    return p


def build_metadata(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'metadata' subcommand."""
    p = subparsers.add_parser(
        'metadata',
        aliases=('meta', 'md'),
        help='Retrieve metadata JSON for workflows from a Cromwell server',
    )
    add_dataclass_args(p, MetadataArgs)
    return p


def build_troubleshoot(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'troubleshoot' subcommand."""
    p = subparsers.add_parser(
        'troubleshoot',
        aliases=('debug', 'ts'),
        help='Troubleshoot workflow problems from metadata JSON file or workflow IDs',
    )
    add_dataclass_args(p, TroubleshootArgs)
    return p


def build_gcp_monitor(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'gcp_monitor' subcommand."""
    p = subparsers.add_parser(
        'gcp_monitor',
        aliases=('monitor',),
        help="Tabulate task's resource data collected on GCP instances",
    )
    add_dataclass_args(p, GcpMonitorArgs)
    return p


def build_gcp_res_analysis(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'gcp_res_analysis' subcommand."""
    p = subparsers.add_parser(
        'gcp_res_analysis',
        aliases=('gcp_res', 'res'),
        help='Linear resource analysis on GCP monitoring data',
    )
    add_dataclass_args(p, GcpResAnalysisArgs)
    return p


def build_cleanup(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'cleanup' subcommand."""
    p = subparsers.add_parser(
        'cleanup',
        aliases=('clean',),
        help='Cleanup outputs of workflows',
    )
    add_dataclass_args(p, CleanupArgs)
    return p


def build_hpc(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """
    Build parser for 'hpc' subcommand with nested subcommands.

    Note: This returns the parent HPC parser. Nested subparsers are built internally.
    """
    p = subparsers.add_parser(
        'hpc',
        aliases=('cluster',),
        help='HPC helper commands',
    )

    # Create nested subparsers for HPC actions
    hpc_subparsers = p.add_subparsers(dest='hpc_action', required=True)

    # hpc submit
    submit = hpc_subparsers.add_parser(
        'submit',
        aliases=('sbatch', 'qsub', 'bsub'),
        help='Submit a single workflow to HPC',
    )
    add_dataclass_args(submit, HpcSubmitArgs)

    # hpc list
    lst = hpc_subparsers.add_parser(
        'list',
        aliases=('ls',),
        help='List all workflows submitted to HPC',
    )
    add_dataclass_args(lst, HpcListArgs)

    # hpc abort
    abort = hpc_subparsers.add_parser(
        'abort',
        aliases=('cancel',),
        help='Abort a workflow submitted to HPC',
    )
    add_dataclass_args(abort, HpcAbortArgs)

    return p
