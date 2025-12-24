"""Parser building functions for Caper CLI."""

from __future__ import annotations

import argparse
from argparse import ArgumentParser

# Import existing _add_* functions from caper_args.py
from caper.caper_args import (
    _add_aws_runner_args,
    _add_backend_args,
    _add_client_args,
    _add_cleanup_args,
    _add_common_args,
    _add_cromwell_args,
    _add_db_args,
    _add_dependency_resolver_args,
    _add_gcp_monitor_args,
    _add_gcp_res_analysis_args,
    _add_gcp_runner_args,
    _add_gcp_zones_args,
    _add_hpc_abort_args,
    _add_hpc_submit_args,
    _add_list_args,
    _add_local_backend_args,
    _add_localization_args,
    _add_run_args,
    _add_scheduler_args,
    _add_search_args,
    _add_server_args,
    _add_server_client_args,
    _add_submit_io_args,
    _add_troubleshoot_args,
)
from caper.cromwell_backend import BackendProvider


def build_init(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'init' subcommand."""
    p = subparsers.add_parser(
        'init',
        aliases=(),
        help="Initialize Caper's configuration file",
    )
    _add_common_args(p)
    _add_localization_args(p)
    p.add_argument(
        'platform',
        help='Platform to initialize Caper for.',
        choices=[provider for provider in BackendProvider],
    )
    return p


def build_run(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'run' subcommand."""
    p = subparsers.add_parser(
        'run',
        aliases=('local', 'exec'),
        help='Run a single workflow without server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_submit_io_args(p)
    _add_dependency_resolver_args(p)
    _add_hpc_submit_args(p)
    _add_scheduler_args(p)
    _add_run_args(p)
    _add_db_args(p)
    _add_cromwell_args(p)
    _add_local_backend_args(p)
    _add_gcp_runner_args(p)
    _add_aws_runner_args(p)
    _add_backend_args(p)
    _add_gcp_zones_args(p)
    return p


def build_server(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'server' subcommand."""
    p = subparsers.add_parser(
        'server',
        aliases=('srv',),
        help='Run a Cromwell server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_server_args(p)
    _add_db_args(p)
    _add_cromwell_args(p)
    _add_local_backend_args(p)
    _add_gcp_runner_args(p)
    _add_aws_runner_args(p)
    _add_backend_args(p)
    _add_gcp_zones_args(p)
    _add_scheduler_args(p)
    return p


def build_submit(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'submit' subcommand."""
    p = subparsers.add_parser(
        'submit',
        aliases=('sub',),
        help='Submit a workflow to a Cromwell server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_submit_io_args(p)
    _add_dependency_resolver_args(p)
    _add_scheduler_args(p)
    _add_backend_args(p)
    _add_gcp_zones_args(p)
    return p


def build_abort(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'abort' subcommand."""
    p = subparsers.add_parser(
        'abort',
        aliases=(),
        help='Abort running/pending workflows on a Cromwell server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    return p


def build_unhold(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'unhold' subcommand."""
    p = subparsers.add_parser(
        'unhold',
        aliases=(),
        help='Release hold of workflows on a Cromwell server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    return p


def build_list(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'list' subcommand."""
    p = subparsers.add_parser(
        'list',
        aliases=('ls',),
        help='List running/pending workflows on a Cromwell server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    _add_list_args(p)
    return p


def build_metadata(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'metadata' subcommand."""
    p = subparsers.add_parser(
        'metadata',
        aliases=('meta', 'md'),
        help='Retrieve metadata JSON for workflows from a Cromwell server',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    return p


def build_troubleshoot(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'troubleshoot' subcommand."""
    p = subparsers.add_parser(
        'troubleshoot',
        aliases=('debug', 'ts'),
        help='Troubleshoot workflow problems from metadata JSON file or workflow IDs',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    _add_troubleshoot_args(p)
    return p


def build_gcp_monitor(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'gcp_monitor' subcommand."""
    p = subparsers.add_parser(
        'gcp_monitor',
        aliases=('monitor',),
        help="Tabulate task's resource data collected on GCP instances",
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    _add_gcp_monitor_args(p)
    return p


def build_gcp_res_analysis(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'gcp_res_analysis' subcommand."""
    p = subparsers.add_parser(
        'gcp_res_analysis',
        aliases=('gcp_res', 'res'),
        help='Linear resource analysis on GCP monitoring data',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    _add_gcp_res_analysis_args(p)
    return p


def build_cleanup(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'cleanup' subcommand."""
    p = subparsers.add_parser(
        'cleanup',
        aliases=('clean',),
        help='Cleanup outputs of workflows',
    )
    _add_common_args(p)
    _add_localization_args(p)
    _add_server_client_args(p)
    _add_client_args(p)
    _add_search_args(p)
    _add_cleanup_args(p)
    return p


def build_hpc(subparsers: argparse._SubParsersAction) -> ArgumentParser:
    """Build parser for 'hpc' subcommand with nested subcommands."""
    p = subparsers.add_parser(
        'hpc',
        aliases=('cluster',),
        help='HPC helper commands',
    )
    _add_common_args(p)
    _add_localization_args(p)

    hpc_subparsers = p.add_subparsers(dest='hpc_action', required=True)

    # hpc submit
    submit = hpc_subparsers.add_parser(
        'submit',
        aliases=('sbatch', 'qsub', 'bsub'),
        help='Submit a single workflow to HPC',
    )
    _add_common_args(submit)
    _add_localization_args(submit)
    _add_submit_io_args(submit)
    _add_dependency_resolver_args(submit)
    _add_hpc_submit_args(submit)
    _add_scheduler_args(submit)
    _add_run_args(submit)
    _add_db_args(submit)
    _add_cromwell_args(submit)
    _add_local_backend_args(submit)
    _add_gcp_runner_args(submit)
    _add_aws_runner_args(submit)
    _add_backend_args(submit)
    _add_gcp_zones_args(submit)
    submit.set_defaults(_hpc_spec='submit')

    # hpc list
    lst = hpc_subparsers.add_parser(
        'list',
        aliases=('ls',),
        help='List all workflows submitted to HPC',
    )
    _add_common_args(lst)
    _add_localization_args(lst)
    _add_backend_args(lst)
    lst.set_defaults(_hpc_spec='list')

    # hpc abort
    abort = hpc_subparsers.add_parser(
        'abort',
        aliases=('cancel',),
        help='Abort a workflow submitted to HPC',
    )
    _add_common_args(abort)
    _add_localization_args(abort)
    _add_backend_args(abort)
    _add_hpc_abort_args(abort)
    abort.set_defaults(_hpc_spec='abort')

    return p

