"""Main dispatch logic for Caper CLI."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from caper import __version__
from caper.cli.args import namespace_to_dataclass
from caper.cli.commands import COMMANDS, HPC_COMMANDS
from caper.cli.config import apply_config_to_parser, load_conf_defaults


def main(argv: list[str] | None = None) -> None:
    """
    Main entry point for Caper CLI.

    Flow:
    1. Bootstrap parse for --conf
    2. Load config defaults
    3. Build all parsers from COMMANDS registry
    4. Apply config defaults to parsers
    5. Parse arguments
    6. Convert Namespace to typed dataclass
    7. Dispatch to handler

    Args:
        argv: Command line arguments (defaults to sys.argv)
    """
    # 1. bootstrap parse for --conf
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument('-c', '--conf', default='~/.caper/default.conf')
    conf_ns, _ = bootstrap.parse_known_args(argv)

    # 2. load config defaults
    config = load_conf_defaults(conf_ns.conf)

    # 3. build main parser and subparsers
    parser = argparse.ArgumentParser(
        prog='caper',
        description='Cromwell Assisted Pipeline ExecutoR',
    )
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version=f'%(prog)s {__version__}',
    )

    subparsers = parser.add_subparsers(
        dest='command',
        required=False,  # Will handle missing command manually
    )

    # Build all command parsers and create lookup map
    command_map: dict[str, Any] = {}

    for cmd in COMMANDS:
        # Build the parser using the command's build function
        subparser = cmd.build_parser(subparsers)

        # Apply config defaults to this subparser
        apply_config_to_parser(subparser, config)

        # Store command in map for lookup by name or alias
        command_map[cmd.name] = cmd
        for alias in cmd.aliases:
            command_map[alias] = cmd

    # Build HPC parser separately (it has nested subcommands)
    # Import here to avoid circular import
    from caper.cli.parsers import build_hpc  # noqa: PLC0415

    hpc_parser = build_hpc(subparsers)
    apply_config_to_parser(hpc_parser, config)
    command_map['hpc'] = None  # HPC handled specially below
    command_map['cluster'] = None  # HPC alias

    # 4. handle no arguments or --help
    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        parser.print_help()
        return

    # 5. parse arguments
    ns = parser.parse_args(argv)

    # 6. dispatch to handler
    # Special handling for HPC commands (nested subcommands)
    if ns.command in ('hpc', 'cluster'):
        if not hasattr(ns, 'hpc_action'):
            parser.error('HPC subcommand required (submit, list, or abort)')

        # Get HPC subcommand info
        hpc_action = ns.hpc_action
        if hpc_action not in HPC_COMMANDS:
            parser.error(f'Unknown HPC subcommand: {hpc_action}')

        args_class, handler = HPC_COMMANDS[hpc_action]

        # Convert to typed dataclass
        typed_args = namespace_to_dataclass(ns, args_class)

        # Dispatch
        handler(typed_args)
        return

    # Regular command dispatch
    if not ns.command:
        parser.error('No command specified')

    cmd = command_map.get(ns.command)
    if cmd is None:
        parser.error(f'Unknown command: {ns.command}')

    # Type narrowing: cmd is not None here
    assert cmd is not None  # noqa: S101

    # Convert Namespace to typed dataclass
    typed_args = namespace_to_dataclass(ns, cmd.args_class)

    # Dispatch to handler
    cmd.handler(typed_args)
