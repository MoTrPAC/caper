"""RunArgs dataclass for 'caper run' command."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from caper.cli.arg_field import abspath, apply_normalizers, arg, split_commas

from .mixins import (
    AwsRunnerArgs,
    BackendArgs,
    CommonArgs,
    CromwellArgs,
    DatabaseArgs,
    GcpRunnerArgs,
    LocalBackendArgs,
    LocalizationArgs,
    RunSpecificArgs,
    SchedulerArgs,
    SubmitIOArgs,
)


class ContainerFlagError(ValueError):
    """Error related to container flags."""


def _validate_container_exclusivity(
    docker: str | None,
    singularity: str | None,
    conda: str | None,
) -> None:
    """Validate that container flags are mutually exclusive."""
    flags = [docker is not None, singularity is not None, conda is not None]
    if sum(flags) > 1:
        msg = '--docker, --singularity, and --conda are mutually exclusive'
        raise ContainerFlagError(msg)


def _validate_container_value(name: str, value: str | None) -> None:
    """Validate container flag didn't eat positional arguments."""
    if value and value.endswith(('.wdl', '.cwl')):
        msg = (
            f'--{name} ate up positional arguments (e.g. WDL, CWL). '
            f'Define --{name} at the end of command line arguments. {name}={value}'
        )
        raise ContainerFlagError(msg)


def _validate_soft_glob_docker(docker: str | None, *, soft_glob_output: bool) -> None:
    """Validate both --soft-glob-output and --docker are not set."""
    if docker is not None and soft_glob_output:
        msg = (
            '--soft-glob-output and --docker are mutually exclusive. '
            'Delocalization from docker fails for soft-linked globbed outputs.'
        )
        raise ContainerFlagError(msg)


@dataclass
class RunArgs(
    CommonArgs,
    LocalizationArgs,
    BackendArgs,
    SubmitIOArgs,
    DatabaseArgs,
    CromwellArgs,
    LocalBackendArgs,
    GcpRunnerArgs,
    AwsRunnerArgs,
    SchedulerArgs,
    RunSpecificArgs,
):
    """Arguments for 'caper run' subcommand."""

    wdl: str = arg(
        'wdl',
        help_text='Path, URL or URI for WDL script',
        normalize=abspath,
    )

    gcp_zones_list: list[str] = field(default_factory=list, repr=False)
    memory_retry_error_keys_list: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Normalize paths and validate arguments."""
        apply_normalizers(self)
        self._derive_loc_dirs()
        self._derive_file_db()
        self._split_delimited_fields()
        self._validate_container_options()

    def _derive_loc_dirs(self) -> None:
        """Derive localization dirs from output dirs if not set."""
        if not self.local_loc_dir:
            self.local_loc_dir = f'{self.local_out_dir}/.caper_tmp'
        if self.gcp_out_dir and not self.gcp_loc_dir:
            self.gcp_loc_dir = f'{self.gcp_out_dir}/.caper_tmp'
        if self.aws_out_dir and not self.aws_loc_dir:
            self.aws_loc_dir = f'{self.aws_out_dir}/.caper_tmp'

    def _derive_file_db(self) -> None:
        """Derive DB path if using file DB and none specified."""
        if self.db != 'file' or self.file_db:
            return
        db_parts = ['caper-db']
        if self.wdl:
            db_parts.append(os.path.basename(self.wdl))
        if self.inputs:
            db_parts.append(os.path.basename(self.inputs))
        self.file_db = f'{self.local_out_dir}/_'.join(db_parts)

    def _split_delimited_fields(self) -> None:
        """Split comma-delimited string fields."""
        self.gcp_zones_list = split_commas(self.gcp_zones)
        self.memory_retry_error_keys_list = split_commas(self.memory_retry_error_keys)

    def _validate_container_options(self) -> None:
        """Validate container-related options."""
        _validate_container_exclusivity(self.docker, self.singularity, self.conda)
        for name, value in [
            ('docker', self.docker),
            ('singularity', self.singularity),
            ('conda', self.conda),
        ]:
            _validate_container_value(name, value)
        _validate_soft_glob_docker(self.docker, soft_glob_output=self.soft_glob_output)
