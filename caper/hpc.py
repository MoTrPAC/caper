"""Caper's HPC Wrapper based on job engine's CLI (shell command).
e.g. sbatch, squeue, qsub, qstat
"""

from __future__ import annotations

import logging
import os
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    import builtins

logger = logging.getLogger(__name__)

CAPER_LEADER_JOB_NAME_PREFIX = 'CAPER_'
ILLEGAL_CHARS_IN_JOB_NAME = [',', ' ', '\t']


def get_user_from_os_environ() -> str:
    return os.environ['USER']


def make_bash_script_contents(contents: str) -> str:
    return f'#!/bin/bash\n{contents}\n'


def make_caper_leader_job_name(job_name: str) -> str:
    """Check if job name contains Comma, TAB or whitespace.
    They are not allowed since they can be used as separators.
    """
    for illegal_char in ILLEGAL_CHARS_IN_JOB_NAME:
        if illegal_char in job_name:
            msg = f'Illegal character {illegal_char} in job name {job_name}'
            raise ValueError(
                msg,
            )
    return CAPER_LEADER_JOB_NAME_PREFIX + job_name


class HpcWrapper(ABC):
    def __init__(self, leader_job_resource_param: list[str] | None = None) -> None:
        """Base class for HPC job engine wrapper."""
        if leader_job_resource_param is None:
            leader_job_resource_param = []
        self._leader_job_resource_param = leader_job_resource_param

    def submit(self, job_name: str, caper_run_command: list[str]) -> str:
        """Submits a caper leader job to HPC (e.g. sbatch, qsub).
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
        """Filters out non-caper jobs from the job list keeping the first line (header).
        And then returns output STDOUT.
        """
        result = []
        lines = self._list().split('\n')

        # keep header
        result.append(lines[0])

        # filter out non-caper lines
        logger.info('Filtering out non-caper leader jobs...')
        result.extend(line for line in lines[1:] if CAPER_LEADER_JOB_NAME_PREFIX in line)

        return '\n'.join(result)

    def abort(self, job_ids: builtins.list[str]) -> str:
        """Returns output STDOUT from job engine's abort command (e.g. scancel, qdel)."""
        return self._abort(job_ids)

    @abstractmethod
    def _submit(self, job_name: str, shell_script: str) -> str:
        pass

    @abstractmethod
    def _list(self) -> str:
        pass

    @abstractmethod
    def _abort(self, job_ids: builtins.list[str]) -> str:
        """Sends SIGINT (or SIGTERM) to Caper for a graceful shutdown."""

    def _run_command(self, command: builtins.list[str]) -> str:
        """Runs a shell command line and returns STDOUT."""
        logger.info('Running shell command: %s', ' '.join(command))
        return (
            subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                env=os.environ,
            )
            .stdout.decode()
            .strip()
        )


class SlurmWrapper(HpcWrapper):
    DEFAULT_LEADER_JOB_RESOURCE_PARAM: ClassVar[list[str]] = ['-t', '48:00:00', '--mem', '4G']

    def __init__(
        self,
        leader_job_resource_param: list[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        slurm_partition: str | None = None,
        slurm_account: str | None = None,
    ) -> None:
        super().__init__(
            leader_job_resource_param=leader_job_resource_param,
        )
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
        return self._run_command(
            [
                'squeue',
                '-u',
                get_user_from_os_environ(),
                '--Format=JobID,Name,State,SubmitTime',
            ],
        )

    def _abort(self, job_ids: list[str]) -> str:
        """Notes: --full is necessary to correctly send SIGINT to the leader job (Cromwell process).
        Sending SIGTERM may result in an immediate shutdown of the leaderjob on some clusters.
        SIGINT is much better to trigger a graceful shutdown.
        """
        return self._run_command(['scancel', '--full', '--signal=SIGINT', *job_ids])


class SgeWrapper(HpcWrapper):
    DEFAULT_LEADER_JOB_RESOURCE_PARAM: ClassVar[list[str]] = ['-l', 'h_rt=48:00:00,h_vmem=4G']

    def __init__(
        self,
        leader_job_resource_param: list[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        sge_queue: str | None = None,
    ) -> None:
        super().__init__(
            leader_job_resource_param=leader_job_resource_param,
        )
        self._sge_queue_param = ['-q', sge_queue] if sge_queue else []

    def _submit(self, job_name: str, shell_script: str) -> str:
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
        return self._run_command(['qstat', '-u', get_user_from_os_environ()])

    def _abort(self, job_ids: list[str]) -> str:
        return self._run_command(['qdel', *job_ids])


class PbsWrapper(HpcWrapper):
    DEFAULT_LEADER_JOB_RESOURCE_PARAM: ClassVar[list[str]] = ['-l', 'walltime=48:00:00,mem=4gb']

    def __init__(
        self,
        leader_job_resource_param: list[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        pbs_queue: str | None = None,
    ) -> None:
        super().__init__(
            leader_job_resource_param=leader_job_resource_param,
        )
        self._pbs_queue_param = ['-q', pbs_queue] if pbs_queue else []

    def _submit(self, job_name: str, shell_script: str) -> str:
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
        return self._run_command(['qstat', '-u', get_user_from_os_environ()])

    def _abort(self, job_ids: list[str]) -> str:
        return self._run_command(['qdel', '-W', '30', *job_ids])


class LsfWrapper(HpcWrapper):
    DEFAULT_LEADER_JOB_RESOURCE_PARAM: ClassVar[list[str]] = ['-W', '2880', '-M', '4g']

    def __init__(
        self,
        leader_job_resource_param: list[str] = DEFAULT_LEADER_JOB_RESOURCE_PARAM,
        lsf_queue: str | None = None,
    ) -> None:
        super().__init__(
            leader_job_resource_param=leader_job_resource_param,
        )
        self._lsf_queue_param = ['-q', lsf_queue] if lsf_queue else []

    def _submit(self, job_name: str, shell_script: str) -> str:
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
        return self._run_command(['bjobs', '-u', get_user_from_os_environ()])

    def _abort(self, job_ids: list[str]) -> str:
        return self._run_command(['bkill', *job_ids])
