"""
Caper's HPC Wrapper based on job engine's CLI (shell command).

Supports/wraps sbatch, squeue, qsub, qstat
"""

from __future__ import annotations

import logging
import os
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import builtins
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

CAPER_LEADER_JOB_NAME_PREFIX = 'CAPER_'
ILLEGAL_CHARS_IN_JOB_NAME = [',', ' ', '\t']


def get_user_from_os_environ() -> str:
    """Returns username from OS environment."""
    return os.environ['USER']


def make_bash_script_contents(contents: str) -> str:
    """Wraps a shell command in a bash shebang."""
    return f'#!/bin/bash\n{contents}\n'


def make_caper_leader_job_name(job_name: str) -> str:
    """
    Check if job name contains Comma, TAB or whitespace.

    These characters are not allowed since they can be used as separators.
    """
    for illegal_char in ILLEGAL_CHARS_IN_JOB_NAME:
        if illegal_char in job_name:
            msg = f'Illegal character {illegal_char} in job name {job_name}'
            raise ValueError(msg)
    return CAPER_LEADER_JOB_NAME_PREFIX + job_name


class HpcWrapper(ABC):
    """Base class for HPC job engine wrapper."""

    def __init__(self, leader_job_resource_param: Iterable[str] = ()) -> None:
        """Base class for HPC job engine wrapper."""
        self._leader_job_resource_param = list(leader_job_resource_param)

    def submit(self, job_name: str, caper_run_command: Iterable[str]) -> str:
        """
        Submits a caper leader job to HPC (e.g. sbatch, qsub).

        Such leader job will be prefixed with CAPER_LEADER_JOB_NAME_PREFIX.

        Returns output STDOUT from submission command.
        """
        home_dir = f'{Path.home()!s}{os.sep}'
        with NamedTemporaryFile(prefix=home_dir, suffix='.sh') as shell_script:
            contents = make_bash_script_contents(' '.join(caper_run_command))
            shell_script.write(contents.encode())
            shell_script.flush()

            return self._submit(job_name, shell_script.name)

    def list(self) -> str:
        """
        Print out non-caper jobs from the job list keeping the first line (header).

        And then returns output STDOUT.
        """
        lines = self._list().split('\n')

        # keep header
        result = [lines[0]]

        # filter out non-caper lines
        logger.info('Filtering out non-caper leader jobs...')

        result.extend([line for line in lines[1:] if CAPER_LEADER_JOB_NAME_PREFIX in line])
        return '\n'.join(result)

    def abort(self, job_ids: Iterable[str]) -> str:
        """Returns output STDOUT from job engine's abort command (e.g. scancel, qdel)."""
        return self._abort(job_ids)

    @abstractmethod
    def _submit(self, job_name: str, shell_script: str) -> str:
        """Submits a caper leader job to HPC (e.g. sbatch, qsub)."""
        msg = f'{self.__class__.__name__} does not support submit command.'
        raise NotImplementedError(msg)

    @abstractmethod
    def _list(self) -> str:
        """Returns output STDOUT from job engine's list command."""
        msg = f'{self.__class__.__name__} does not support list command.'
        raise NotImplementedError(msg)

    @abstractmethod
    def _abort(self, job_ids: Iterable[str]) -> str:
        """Sends SIGINT (or SIGTERM) to Caper for a graceful shutdown."""
        msg = f'{self.__class__.__name__} does not support abort command.'
        raise NotImplementedError(msg)

    def _run_command(self, command: builtins.list[str]) -> str:
        """Runs a shell command line and returns STDOUT."""
        logger.info('Running shell command: %s', ' '.join(command))
        return (
            subprocess.run(  # noqa: S603
                command,
                stdout=subprocess.PIPE,
                env=os.environ,
                check=True,
            )
            .stdout.decode()
            .strip()
        )


class SlurmWrapper(HpcWrapper):
    """Wrapper for SLURM job engine."""

    DEFAULT_LEADER_JOB_RESOURCE_PARAM = ('-t', '48:00:00', '--mem', '4G')

    def __init__(
        self,
        leader_job_resource_param: Iterable[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        slurm_partition: str | None = None,
        slurm_account: str | None = None,
    ) -> None:
        """Initialize the shared parameters for SLURM job engine."""
        super().__init__(leader_job_resource_param=leader_job_resource_param)
        slurm_partition_param = ['-p', slurm_partition] if slurm_partition else []
        slurm_account_param = ['-A', slurm_account] if slurm_account else []
        self._slurm_extra_param = slurm_partition_param + slurm_account_param

    def _submit(self, job_name: str, shell_script: str) -> str:
        command = [
            'sbatch',
            *self._leader_job_resource_param,
            *self._slurm_extra_param,
            '--export=ALL',
            '-J',
            make_caper_leader_job_name(job_name),
            shell_script,
        ]
        return self._run_command(command)

    def _list(self) -> str:
        """List SLURM jobs."""
        return self._run_command(
            [
                'squeue',
                '-u',
                get_user_from_os_environ(),
                '--Format=JobID,Name,State,SubmitTime',
            ]
        )

    def _abort(self, job_ids: Iterable[str]) -> str:
        """
        Abort a SLURM job.

        Notes: --full is necessary to correctly send SIGINT to the leader job (Cromwell
        process). Sending SIGTERM may result in an immediate shutdown of the leaderjob on some
        clusters. SIGINT is much better to trigger a graceful shutdown.
        """
        return self._run_command(['scancel', '--full', '--signal=SIGINT', *job_ids])


class SgeWrapper(HpcWrapper):
    """Wrapper for SGE job engine."""

    DEFAULT_LEADER_JOB_RESOURCE_PARAM = ('-l', 'h_rt=48:00:00,h_vmem=4G')

    def __init__(
        self,
        leader_job_resource_param: Iterable[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        sge_queue: str | None = None,
    ) -> None:
        """Initialize the shared parameters for SGE job engine."""
        super().__init__(leader_job_resource_param=leader_job_resource_param)
        self._sge_queue_param = ['-q', sge_queue] if sge_queue else []

    def _submit(self, job_name: str, shell_script: str) -> str:
        """Submit a SGE job."""
        command = [
            'qsub',
            *self._leader_job_resource_param,
            *self._sge_queue_param,
            '-V',
            '-terse',
            '-N',
            make_caper_leader_job_name(job_name),
            shell_script,
        ]
        return self._run_command(command)

    def _list(self) -> str:
        """List SGE jobs."""
        return self._run_command(['qstat', '-u', get_user_from_os_environ()])

    def _abort(self, job_ids: Iterable[str]) -> str:
        """Abort a SGE job."""
        return self._run_command(['qdel', *job_ids])


class PbsWrapper(HpcWrapper):
    """Wrapper for PBS job engine."""

    DEFAULT_LEADER_JOB_RESOURCE_PARAM = ('-l', 'walltime=48:00:00,mem=4gb')

    def __init__(
        self,
        leader_job_resource_param: Iterable[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        pbs_queue: str | None = None,
    ) -> None:
        """Initialize the shared parameters for PBS job engine."""
        super().__init__(leader_job_resource_param=leader_job_resource_param)
        self._pbs_queue_param = ['-q', pbs_queue] if pbs_queue else []

    def _submit(self, job_name: str, shell_script: str) -> str:
        """Submit a PBS job."""
        command = [
            'qsub',
            *self._leader_job_resource_param,
            *self._pbs_queue_param,
            '-V',
            '-N',
            make_caper_leader_job_name(job_name),
            shell_script,
        ]
        return self._run_command(command)

    def _list(self) -> str:
        """List PBS jobs."""
        return self._run_command(['qstat', '-u', get_user_from_os_environ()])

    def _abort(self, job_ids: Iterable[str]) -> str:
        """Abort a PBS job."""
        return self._run_command(['qdel', '-W', '30', *job_ids])


class LsfWrapper(HpcWrapper):
    """Wrapper for LSF job engine."""

    DEFAULT_LEADER_JOB_RESOURCE_PARAM = ('-W', '2880', '-M', '4g')

    def __init__(
        self,
        leader_job_resource_param: Iterable[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        lsf_queue: str | None = None,
    ) -> None:
        """Initialize the shared parameters for LSF job engine."""
        super().__init__(leader_job_resource_param=leader_job_resource_param)
        self._lsf_queue_param = ['-q', lsf_queue] if lsf_queue else []

    def _submit(self, job_name: str, shell_script: str) -> str:
        """Submit a LSF job."""
        command = [
            'bsub',
            *self._leader_job_resource_param,
            *self._lsf_queue_param,
            '-env',
            'all',
            '-J',
            make_caper_leader_job_name(job_name),
            shell_script,
        ]
        return self._run_command(command)

    def _list(self) -> str:
        """List LSF jobs."""
        return self._run_command(['bjobs', '-u', get_user_from_os_environ()])

    def _abort(self, job_ids: Iterable[str]) -> str:
        """Abort a LSF job."""
        return self._run_command(['bkill', *job_ids])
