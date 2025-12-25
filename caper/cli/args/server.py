"""ServerArgs dataclass for 'caper server' command."""

from __future__ import annotations

from dataclasses import dataclass, field

from caper.cli.arg_field import apply_normalizers, split_commas
from caper.cli.args.mixins import (
    AwsRunnerArgs,
    BackendArgs,
    CommonArgs,
    CromwellArgs,
    DatabaseArgs,
    GcpRunnerArgs,
    LocalBackendArgs,
    LocalizationArgs,
    SchedulerArgs,
    ServerClientArgs,
    ServerSpecificArgs,
)


@dataclass
class ServerArgs(
    CommonArgs,
    LocalizationArgs,
    BackendArgs,
    ServerClientArgs,
    ServerSpecificArgs,
    DatabaseArgs,
    CromwellArgs,
    LocalBackendArgs,
    GcpRunnerArgs,
    AwsRunnerArgs,
    SchedulerArgs,
):
    """Arguments for 'caper server' subcommand."""

    # Derived fields
    gcp_zones_list: list[str] = field(default_factory=list, repr=False)
    memory_retry_error_keys_list: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Normalize paths and validate arguments."""
        # Apply all field normalizers
        apply_normalizers(self)

        # Derive localization dirs from output dirs if not set
        if not self.local_loc_dir:
            self.local_loc_dir = f'{self.local_out_dir}/.caper_tmp'

        if self.gcp_out_dir and not self.gcp_loc_dir:
            self.gcp_loc_dir = f'{self.gcp_out_dir}/.caper_tmp'

        if self.aws_out_dir and not self.aws_loc_dir:
            self.aws_loc_dir = f'{self.aws_out_dir}/.caper_tmp'

        # Split comma-delimited strings
        self.gcp_zones_list = split_commas(self.gcp_zones)
        self.memory_retry_error_keys_list = split_commas(self.memory_retry_error_keys)
