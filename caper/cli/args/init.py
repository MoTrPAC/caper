"""InitArgs dataclass for 'caper init' command."""

from __future__ import annotations

from dataclasses import dataclass

from caper.cli.arg_field import apply_normalizers, arg
from caper.cli.args.mixins import CommonArgs, LocalizationArgs


@dataclass
class InitArgs(
    CommonArgs,
    LocalizationArgs,
):
    """Arguments for 'caper init' subcommand."""

    # Positional argument
    platform: str = arg(
        'platform',
        help_text='Platform to initialize Caper for',
        # choices will be set dynamically from BackendProvider
    )

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)
