"""Command registry for Caper CLI.

This module defines the COMMANDS list, which is the single source of truth for
all available Caper commands. Each Command instance knows:
- Its name and aliases
- How to build its parser
- What typed dataclass to use
- What handler function to call
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

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
)
from caper.cli.handlers import (
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
from caper.cli.parsers import (
    build_abort,
    build_cleanup,
    build_gcp_monitor,
    build_gcp_res_analysis,
    build_init,
    build_list,
    build_metadata,
    build_run,
    build_server,
    build_submit,
    build_troubleshoot,
    build_unhold,
)

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar('T')


@dataclass
class Command:
    """
    Definition of a Caper CLI command.

    Attributes:
        name: Primary command name
        aliases: Alternative names (e.g., "ls" for "list")
        help: Brief help text
        build_parser: Function to build argparse parser
        args_class: Typed dataclass for arguments
        handler: Function that accepts typed args and executes command
    """

    name: str
    aliases: tuple[str, ...]
    help: str
    build_parser: Callable[[Any], Any]
    args_class: type
    handler: Callable[[Any], None]

# Command registry - single source of truth for all commands

COMMANDS: list[Command] = [
    Command(
        name='init',
        aliases=(),
        help="Initialize Caper's configuration file",
        build_parser=build_init,
        args_class=InitArgs,
        handler=handle_init,
    ),
    Command(
        name='run',
        aliases=('local', 'exec'),
        help='Run a single workflow without server',
        build_parser=build_run,
        args_class=RunArgs,
        handler=handle_run,
    ),
    Command(
        name='server',
        aliases=('srv',),
        help='Run a Cromwell server',
        build_parser=build_server,
        args_class=ServerArgs,
        handler=handle_server,
    ),
    Command(
        name='submit',
        aliases=('sub',),
        help='Submit a workflow to a Cromwell server',
        build_parser=build_submit,
        args_class=SubmitArgs,
        handler=handle_submit,
    ),
    Command(
        name='abort',
        aliases=(),
        help='Abort running/pending workflows on a Cromwell server',
        build_parser=build_abort,
        args_class=AbortArgs,
        handler=handle_abort,
    ),
    Command(
        name='unhold',
        aliases=(),
        help='Release hold of workflows on a Cromwell server',
        build_parser=build_unhold,
        args_class=UnholdArgs,
        handler=handle_unhold,
    ),
    Command(
        name='list',
        aliases=('ls',),
        help='List running/pending workflows on a Cromwell server',
        build_parser=build_list,
        args_class=ListArgs,
        handler=handle_list,
    ),
    Command(
        name='metadata',
        aliases=('meta', 'md'),
        help='Retrieve metadata JSON for workflows from a Cromwell server',
        build_parser=build_metadata,
        args_class=MetadataArgs,
        handler=handle_metadata,
    ),
    Command(
        name='troubleshoot',
        aliases=('debug', 'ts'),
        help='Troubleshoot workflow problems',
        build_parser=build_troubleshoot,
        args_class=TroubleshootArgs,
        handler=handle_troubleshoot,
    ),
    Command(
        name='gcp_monitor',
        aliases=('monitor',),
        help="Tabulate task's resource data collected on GCP instances",
        build_parser=build_gcp_monitor,
        args_class=GcpMonitorArgs,
        handler=handle_gcp_monitor,
    ),
    Command(
        name='gcp_res_analysis',
        aliases=('gcp_res', 'res'),
        help='Linear resource analysis on GCP monitoring data',
        build_parser=build_gcp_res_analysis,
        args_class=GcpResAnalysisArgs,
        handler=handle_gcp_res_analysis,
    ),
    Command(
        name='cleanup',
        aliases=('clean',),
        help='Cleanup outputs of workflows',
        build_parser=build_cleanup,
        args_class=CleanupArgs,
        handler=handle_cleanup,
    ),
]

# HPC commands are special - they're nested under "hpc" parent
# We handle these separately in dispatch
HPC_COMMANDS: dict[str, tuple[type, Callable]] = {
    'submit': (HpcSubmitArgs, handle_hpc_submit),
    'list': (HpcListArgs, handle_hpc_list),
    'abort': (HpcAbortArgs, handle_hpc_abort),
}
