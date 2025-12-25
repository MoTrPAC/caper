"""SubmitArgs dataclass for 'caper submit' command."""

from __future__ import annotations

from dataclasses import dataclass, field

from caper.cli.arg_field import abspath, apply_normalizers, arg, split_commas
from caper.cli.args.mixins import (
    BackendArgs,
    ClientArgs,
    CommonArgs,
    LocalizationArgs,
    SchedulerArgs,
    ServerClientArgs,
    SubmitIOArgs,
)


@dataclass
class SubmitArgs(
    CommonArgs,
    LocalizationArgs,
    BackendArgs,
    ServerClientArgs,
    ClientArgs,
    SubmitIOArgs,
    SchedulerArgs,
):
    """Arguments for 'caper submit' subcommand."""

    # Positional argument
    wdl: str = arg(
        'wdl',
        help_text='Path, URL or URI for WDL script',
        normalize=abspath,
    )

    # GCP-specific for submit
    gcp_compute_service_account: str | None = arg(
        '--gcp-compute-service-account',
        help_text='Service account email for Google Cloud Batch compute instances',
    )

    # Derived fields
    gcp_zones_list: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Normalize paths and validate arguments."""
        # Apply all field normalizers
        apply_normalizers(self)

        # Split comma-delimited strings
        self.gcp_zones_list = split_commas(self.gcp_zones)

        # Validate mutual exclusion of container options
        container_flags = [
            self.docker is not None,
            self.singularity is not None,
            self.conda is not None,
        ]
        if sum(container_flags) > 1:
            msg = '--docker, --singularity, and --conda are mutually exclusive'
            raise ValueError(msg)
