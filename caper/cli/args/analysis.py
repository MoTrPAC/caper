"""Analysis command dataclasses (gcp_monitor, gcp_res_analysis, cleanup)."""

from __future__ import annotations

from dataclasses import dataclass

from caper.cli.arg_field import abspath, apply_normalizers, arg
from caper.cli.args.mixins import (
    CleanupActionArgs,
    ClientArgs,
    CommonArgs,
    GcpMonitorDisplayArgs,
    LocalizationArgs,
    SearchArgs,
    ServerClientArgs,
)


@dataclass
class GcpMonitorArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
    GcpMonitorDisplayArgs,
):
    """Arguments for 'caper gcp_monitor' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)


@dataclass
class GcpResAnalysisArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
):
    """Arguments for 'caper gcp_res_analysis' subcommand."""

    in_file_vars_def_json: str | None = arg(
        '--in-file-vars-def-json',
        help_text='JSON file to define task name and input file vars for resource analysis',
        normalize=abspath,
    )
    reduce_in_file_vars: str = arg(
        '--reduce-in-file-vars',
        help_text='Reduce X matrix (resource data) into a vector',
        default='SUM',
        choices=['SUM', 'MAX', 'MIN', 'NONE'],
    )
    target_resources: list[str] = arg(
        '--target-resources',
        help_text='Keys for resources in JSON gcp_monitor outputs',
        nargs='+',
        default_factory=lambda: ['stats.max.mem', 'stats.max.disk'],
    )
    plot_pdf: str | None = arg(
        '--plot-pdf',
        help_text='Local path for a 2D scatter plot PDF file',
        normalize=abspath,
    )

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)


@dataclass
class CleanupArgs(
    CommonArgs,
    LocalizationArgs,
    ServerClientArgs,
    ClientArgs,
    SearchArgs,
    CleanupActionArgs,
):
    """Arguments for 'caper cleanup' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)
