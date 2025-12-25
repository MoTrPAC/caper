"""HPC command dataclasses (hpc submit, hpc list, hpc abort)."""

from __future__ import annotations

from dataclasses import dataclass, fields

from caper.cli.arg_field import apply_normalizers, arg, dataclass_to_cli_args
from caper.cli.args.mixins import BackendArgs, CommonArgs, LocalizationArgs
from caper.cli.args.run import RunArgs

_SUPPORTED_HPC_BACKENDS = ('slurm', 'sge', 'pbs', 'lsf')


class HpcBackendError(ValueError):
    """Error for unsupported HPC backend."""


def _validate_hpc_backend(backend: str | None) -> None:
    """Validate that the backend is a supported HPC backend."""
    if backend not in _SUPPORTED_HPC_BACKENDS:
        msg = f'Unsupported backend {backend} for hpc. Must be slurm, sge, pbs, or lsf'
        raise HpcBackendError(msg)


@dataclass
class HpcSubmitArgs(RunArgs):
    """Arguments for 'caper hpc submit' subcommand.

    Extends RunArgs with HPC-specific fields for submitting a Caper run job
    to an HPC scheduler.
    """

    leader_job_name: str | None = arg(
        '--leader-job-name',
        help_text='Leader job name for a submitted workflow',
    )
    slurm_leader_job_resource_param: str = arg(
        '--slurm-leader-job-resource-param',
        help_text='Resource parameters to submit a Caper leader job to SLURM',
        default='--mem 2G --time 4-0 --cpus-per-task 1',
    )
    sge_leader_job_resource_param: str = arg(
        '--sge-leader-job-resource-param',
        help_text='Resource parameters to submit a Caper leader job to SGE',
        default='-l h_rt=96:00:00 -l h_vmem=2G -pe shm 1',
    )
    pbs_leader_job_resource_param: str = arg(
        '--pbs-leader-job-resource-param',
        help_text='Resource parameters to submit a Caper leader job to PBS',
        default='-l walltime=96:00:00 -l mem=2gb -l nodes=1:ppn=1',
    )
    lsf_leader_job_resource_param: str = arg(
        '--lsf-leader-job-resource-param',
        help_text='Resource parameters to submit a Caper leader job to LSF',
        default='-W 96:00 -M 2G -n 1',
    )

    def __post_init__(self) -> None:
        """Validate HPC-specific requirements."""
        super().__post_init__()

        if not self.leader_job_name:
            msg = 'Define --leader-job-name [LEADER_JOB_NAME] in command line arguments'
            raise ValueError(msg)

        _validate_hpc_backend(self.backend)

    def to_caper_run_command(self) -> list[str]:
        """Build the 'caper run' command for HPC job submission.

        Reconstructs the command line arguments from RunArgs fields.
        """
        # Identify fields belonging to RunArgs (base class)
        run_arg_names = {f.name for f in fields(RunArgs)}

        # Use shared utility to generate CLI arguments
        args = dataclass_to_cli_args(self, include_fields=run_arg_names)

        return ['caper', 'run', *args]


@dataclass
class HpcListArgs(
    CommonArgs,
    LocalizationArgs,
    BackendArgs,
):
    """Arguments for 'caper hpc list' subcommand."""

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)
        _validate_hpc_backend(self.backend)


@dataclass
class HpcAbortArgs(
    CommonArgs,
    LocalizationArgs,
    BackendArgs,
):
    """Arguments for 'caper hpc abort' subcommand."""

    job_ids: list[str] = arg(
        'job_ids',
        help_text='Job ID or list of job IDs to abort matching Caper leader jobs',
        nargs='+',
    )

    def __post_init__(self) -> None:
        """Apply post-init argparse processing (defined per field)."""
        apply_normalizers(self)
        _validate_hpc_backend(self.backend)
