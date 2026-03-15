"""CLI for HPC commands."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

from .hpc import LsfWrapper, PbsWrapper, SgeWrapper, SlurmWrapper

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)


def make_caper_run_command_for_hpc_submit() -> list[str]:
    """
    Make `caper run ...` command from `caper hpc submit` command.

    Makes `caper run ...` command from `caper hpc submit` command by simply
    replacing `caper hpc submit` with `caper run`.
    This also escapes double quotes in caper run command.
    """
    if sys.argv[1] == 'hpc' and sys.argv[2] == 'submit':
        # Replace "caper hpc submit" with "caper run"
        new_argv = list(sys.argv)
        new_argv.pop(2)
        new_argv[1] = 'run'
        return new_argv

    msg = 'Wrong HPC command'
    raise ValueError(msg)


def subcmd_hpc(args: argparse.Namespace) -> None:
    """Handle 'caper hpc' subcommand."""
    if args.hpc_action == 'submit':
        if args.leader_job_name is None:
            msg = 'Define --leader-job-name [LEADER_JOB_NAME] in the command line arguments.'
            raise ValueError(msg)
        caper_run_command = make_caper_run_command_for_hpc_submit()

        if args.backend == 'slurm':
            stdout = SlurmWrapper(
                args.slurm_leader_job_resource_param.split(),
                args.slurm_partition,
                args.slurm_account,
            ).submit(args.leader_job_name, caper_run_command)

        elif args.backend == 'sge':
            stdout = SgeWrapper(args.sge_leader_job_resource_param.split(), args.sge_queue).submit(
                args.leader_job_name, caper_run_command
            )

        elif args.backend == 'pbs':
            stdout = PbsWrapper(args.pbs_leader_job_resource_param.split(), args.pbs_queue).submit(
                args.leader_job_name, caper_run_command
            )

        elif args.backend == 'lsf':
            stdout = LsfWrapper(args.lsf_leader_job_resource_param.split(), args.lsf_queue).submit(
                args.leader_job_name, caper_run_command
            )

        else:
            msg = f'Unsupported backend {args.backend} for hpc'
            raise ValueError(msg)
    else:
        if args.backend == 'slurm':
            hpc_wrapper = SlurmWrapper()
        elif args.backend == 'sge':
            hpc_wrapper = SgeWrapper()
        elif args.backend == 'pbs':
            hpc_wrapper = PbsWrapper()
        elif args.backend == 'lsf':
            hpc_wrapper = LsfWrapper()
        else:
            msg = f'Unsupported backend {args.backend} for hpc'
            raise ValueError(msg)

        if args.hpc_action == 'list':
            stdout = hpc_wrapper.list()

        elif args.hpc_action == 'abort':
            stdout = hpc_wrapper.abort(args.job_ids)

        else:
            msg = f'Unsupported hpc action {args.hpc_action}'
            raise ValueError(msg)

    print(stdout)  # noqa: T201
