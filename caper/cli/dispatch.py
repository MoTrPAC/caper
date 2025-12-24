"""Main dispatch logic for Caper CLI."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any, cast

from autouri import GCSURI

from caper import __version__ as version
from caper import caper_args

from .args.base import namespace_to_dataclass
from .commands import COMMANDS, HPC_COMMANDS, CommandSpec
from .config import apply_conf_defaults_to_parsers, load_conf_defaults

DEFAULT_CAPER_CONF = caper_args.DEFAULT_CAPER_CONF

logger = logging.getLogger(__name__)


def init_logging(debug: bool) -> None:
    """Initialize logging configuration."""
    log_level = 'DEBUG' if debug else 'INFO'
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s|%(name)s|%(levelname)s| %(message)s',
    )
    logging.getLogger('filelock').setLevel('CRITICAL')


def init_autouri(use_gsutil_for_s3: bool = False) -> None:
    """Initialize autouri with GCS settings."""
    GCSURI.init_gcsuri(use_gsutil_for_s3=use_gsutil_for_s3)


def main(argv: list[str] | None = None, nonblocking_server: bool = False) -> None:
    """
    Main entry point for Caper CLI.

    Args:
        argv: Command line arguments (uses sys.argv if None)
        nonblocking_server: Return Thread instead of blocking for 'server' command
    """
    # Step 1: Bootstrap parse for --conf
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument('-c', '--conf', default=DEFAULT_CAPER_CONF)
    conf_ns, _ = bootstrap.parse_known_args(argv)

    # Step 2: Load config defaults
    conf_defaults = load_conf_defaults(conf_ns.conf)

    # Step 3: Build main parser
    parser = argparse.ArgumentParser(
        description='Caper (Cromwell-assisted Pipeline ExecutioneR)',
    )
    parser.add_argument('-v', '--version', action='store_true', help='Show version')

    subparsers = parser.add_subparsers(dest='command', required=False)

    # Step 4: Build all subparsers and apply defaults
    built_parsers: list[argparse.ArgumentParser] = []
    for spec in COMMANDS:
        sub = spec.build(subparsers)
        sub.set_defaults(_spec=spec)
        built_parsers.append(sub)

    apply_conf_defaults_to_parsers(built_parsers, conf_defaults)

    # Step 5: Handle no arguments
    if argv is None and len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()

    # Step 6: Parse
    ns = parser.parse_args(argv)

    # Step 7: Handle --version
    if ns.version:
        print(version)  # noqa: T201
        return

    # Step 8: Initialize logging
    init_logging(getattr(ns, 'debug', False))

    # Step 9: Initialize autouri
    if hasattr(ns, 'use_gsutil_for_s3'):
        init_autouri(ns.use_gsutil_for_s3)

    # Step 10: Dispatch
    if ns.command is None:
        parser.print_help()
        parser.exit()

    # Handle HPC specially due to nested subparsers
    if ns.command == 'hpc':
        hpc_action = getattr(ns, 'hpc_action', None)
        if hpc_action is None:
            parser.error('hpc requires a subcommand: submit, list, or abort')

        if not isinstance(hpc_action, str):
            parser.error('hpc action must be a string')

        # At this point, hpc_action is guaranteed to be a string
        hpc_action_str = cast('str', hpc_action)
        hpc_spec = HPC_COMMANDS.get(hpc_action_str)
        if hpc_spec is None:
            parser.error(f'Unknown hpc action: {hpc_action_str}')

        # Type narrowing: hpc_spec is not None after the check above
        assert hpc_spec is not None  # noqa: S101
        model = namespace_to_dataclass(ns, hpc_spec.model_cls)
        hpc_spec.handler(model)
        return None

    spec: CommandSpec[Any] = ns._spec  # type: ignore[attr-defined]
    model = spec.to_model(ns)

    # Special handling for server nonblocking mode
    if ns.command == 'server' and nonblocking_server:
        from .handlers import handle_server  # noqa: PLC0415

        return handle_server(model, nonblocking=True)

    spec.handler(model)
    return None


if __name__ == '__main__':
    main()

