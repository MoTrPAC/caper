"""Client command dataclasses (abort, unhold, list, metadata, troubleshoot)."""

from __future__ import annotations

from dataclasses import dataclass

from caper.cli.arg_field import apply_normalizers
from caper.cli.args.mixins import (
    ClientArgs,
    CommonArgs,
    ListDisplayArgs,
    LocalizationArgs,
    SearchArgs,
    ServerClientArgs,
    TroubleshootDisplayArgs,
)


@dataclass
class AbortArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
):
    """Arguments for 'caper abort' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)


@dataclass
class UnholdArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
):
    """Arguments for 'caper unhold' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)


@dataclass
class ListArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
    ListDisplayArgs,
):
    """Arguments for 'caper list' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)


@dataclass
class MetadataArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
):
    """Arguments for 'caper metadata' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)


@dataclass
class TroubleshootArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
    TroubleshootDisplayArgs,
):
    """Arguments for 'caper troubleshoot' and 'caper debug' subcommands."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)
