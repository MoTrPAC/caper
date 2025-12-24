"""Command registry for Caper CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

import argparse

if TYPE_CHECKING:
    from .args import (
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
    )

T = TypeVar("T")


@dataclass
class CommandSpec(Generic[T]):
    """
    Specification for a CLI subcommand.

    Attributes:
        name: Primary command name
        aliases: Alternative names for the command
        help: Help text shown in --help
        build: Function to build the argparse subparser
        model_cls: Dataclass type for parsed arguments
        handler: Function to handle the command
    """

    name: str
    aliases: tuple[str, ...]
    help: str
    build: Callable[[argparse._SubParsersAction], argparse.ArgumentParser]
    model_cls: type[T]
    handler: Callable[[T], None]

    def to_model(self, ns: argparse.Namespace) -> T:
        """Convert Namespace to typed dataclass."""
        from .args.base import namespace_to_dataclass

        return namespace_to_dataclass(ns, self.model_cls)


# Import dataclasses
from .args import (
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
)

# Import parser builders
from .parsers import (
    build_abort,
    build_cleanup,
    build_gcp_monitor,
    build_gcp_res_analysis,
    build_hpc,
    build_init,
    build_list,
    build_metadata,
    build_run,
    build_server,
    build_submit,
    build_troubleshoot,
    build_unhold,
)

# Import handlers
from .handlers import (
    handle_abort,
    handle_cleanup,
    handle_gcp_monitor,
    handle_gcp_res_analysis,
    handle_hpc_abort,
    handle_hpc_list,
    handle_hpc_submit,
    handle_init,
    handle_list,
    handle_metadata,
    handle_run,
    handle_server,
    handle_submit,
    handle_troubleshoot,
    handle_unhold,
)


# The command registry
COMMANDS: tuple[CommandSpec[Any], ...] = (
    CommandSpec(
        name='init',
        aliases=(),
        help="Initialize Caper's configuration file",
        build=build_init,
        model_cls=InitArgs,
        handler=handle_init,
    ),
    CommandSpec(
        name='run',
        aliases=('local', 'exec'),
        help='Run a single workflow without server',
        build=build_run,
        model_cls=RunArgs,
        handler=handle_run,
    ),
    CommandSpec(
        name='server',
        aliases=('srv',),
        help='Run a Cromwell server',
        build=build_server,
        model_cls=ServerArgs,
        handler=handle_server,
    ),
    CommandSpec(
        name='submit',
        aliases=('sub',),
        help='Submit a workflow to a Cromwell server',
        build=build_submit,
        model_cls=SubmitArgs,
        handler=handle_submit,
    ),
    CommandSpec(
        name='abort',
        aliases=(),
        help='Abort running/pending workflows on a Cromwell server',
        build=build_abort,
        model_cls=AbortArgs,
        handler=handle_abort,
    ),
    CommandSpec(
        name='unhold',
        aliases=(),
        help='Release hold of workflows on a Cromwell server',
        build=build_unhold,
        model_cls=UnholdArgs,
        handler=handle_unhold,
    ),
    CommandSpec(
        name='list',
        aliases=('ls',),
        help='List running/pending workflows on a Cromwell server',
        build=build_list,
        model_cls=ListArgs,
        handler=handle_list,
    ),
    CommandSpec(
        name='metadata',
        aliases=('meta', 'md'),
        help='Retrieve metadata JSON for workflows from a Cromwell server',
        build=build_metadata,
        model_cls=MetadataArgs,
        handler=handle_metadata,
    ),
    CommandSpec(
        name='troubleshoot',
        aliases=('debug', 'ts'),
        help='Troubleshoot workflow problems from metadata JSON file or workflow IDs',
        build=build_troubleshoot,
        model_cls=TroubleshootArgs,
        handler=handle_troubleshoot,
    ),
    CommandSpec(
        name='gcp_monitor',
        aliases=('monitor',),
        help="Tabulate task's resource data collected on GCP instances",
        build=build_gcp_monitor,
        model_cls=GcpMonitorArgs,
        handler=handle_gcp_monitor,
    ),
    CommandSpec(
        name='gcp_res_analysis',
        aliases=('gcp_res', 'res'),
        help='Linear resource analysis on GCP monitoring data',
        build=build_gcp_res_analysis,
        model_cls=GcpResAnalysisArgs,
        handler=handle_gcp_res_analysis,
    ),
    CommandSpec(
        name='cleanup',
        aliases=('clean',),
        help='Cleanup outputs of workflows',
        build=build_cleanup,
        model_cls=CleanupArgs,
        handler=handle_cleanup,
    ),
    # HPC is special - nested subparser
    CommandSpec(
        name='hpc',
        aliases=('cluster',),
        help='HPC helper commands',
        build=build_hpc,
        model_cls=HpcSubmitArgs,  # Placeholder, actual is per hpc_action
        handler=lambda x: None,  # Dispatch handled specially in dispatch.py
    ),
)

# Map for HPC subcommands
HPC_COMMANDS: dict[str, CommandSpec[Any]] = {
    'submit': CommandSpec(
        name='submit',
        aliases=('sbatch', 'qsub', 'bsub'),
        help='Submit a single workflow to HPC',
        build=lambda _: argparse.ArgumentParser(),  # Built inside build_hpc, dummy return
        model_cls=HpcSubmitArgs,
        handler=handle_hpc_submit,
    ),
    'list': CommandSpec(
        name='list',
        aliases=('ls',),
        help='List all workflows submitted to HPC',
        build=lambda _: argparse.ArgumentParser(),  # Built inside build_hpc, dummy return
        model_cls=HpcListArgs,
        handler=handle_hpc_list,
    ),
    'abort': CommandSpec(
        name='abort',
        aliases=('cancel',),
        help='Abort a workflow submitted to HPC',
        build=lambda _: argparse.ArgumentParser(),  # Built inside build_hpc, dummy return
        model_cls=HpcAbortArgs,
        handler=handle_hpc_abort,
    ),
}

