"""Arguments for HPC subcommands."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .base import get_abspath
from caper import caper_args
from caper.caper_workflow_opts import CaperWorkflowOpts
from caper.cromwell import Cromwell
from caper.hpc import LsfWrapper, PbsWrapper, SgeWrapper, SlurmWrapper


@dataclass
class HpcSubmitArgs:
    """Arguments for 'caper hpc submit' subcommand."""

    # Positional (must come first)
    wdl: str

    # Action identifiers
    action: Literal['hpc'] = 'hpc'
    hpc_action: Literal['submit'] = 'submit'

    # All RunArgs fields needed to build the caper run command
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None

    inputs: str | None = None
    options: str | None = None
    labels: str | None = None
    imports: str | None = None
    str_label: str | None = None
    hold: bool = False
    use_gsutil_for_s3: bool = False
    no_deepcopy: bool = False
    ignore_womtool: bool = False
    womtool: str = Cromwell.DEFAULT_WOMTOOL
    java_heap_womtool: str = Cromwell.DEFAULT_JAVA_HEAP_WOMTOOL
    max_retries: int = CaperWorkflowOpts.DEFAULT_MAX_RETRIES
    memory_retry_multiplier: float = CaperWorkflowOpts.DEFAULT_MEMORY_RETRY_MULTIPLIER
    gcp_monitoring_script: str = CaperWorkflowOpts.DEFAULT_GCP_MONITORING_SCRIPT
    docker: str | None = None
    singularity: str | None = None
    conda: str | None = None

    # Backend args
    backend: str | None = None  # Required: slurm, sge, pbs, lsf
    dry_run: bool = False
    gcp_zones: str | None = None

    # Runner args (subset needed for run command)
    db: str | None = None
    db_timeout: int | None = None
    file_db: str | None = None
    mysql_db_ip: str | None = None
    mysql_db_port: int | None = None
    mysql_db_user: str | None = None
    mysql_db_password: str | None = None
    mysql_db_name: str | None = None
    postgresql_db_ip: str | None = None
    postgresql_db_port: int | None = None
    postgresql_db_user: str | None = None
    postgresql_db_password: str | None = None
    postgresql_db_name: str | None = None
    cromwell: str | None = None
    max_concurrent_tasks: int | None = None
    max_concurrent_workflows: int | None = None
    memory_retry_error_keys: str | None = None
    disable_call_caching: bool = False
    backend_file: str | None = None
    soft_glob_output: bool = False
    local_hash_strat: str | None = None
    cromwell_stdout: str | None = None
    local_out_dir: str | None = None
    slurm_resource_param: str | None = None
    gcp_prj: str | None = None
    gcp_region: str | None = None
    gcp_compute_service_account: str | None = None
    gcp_call_caching_dup_strat: str | None = None
    gcp_out_dir: str | None = None
    aws_batch_arn: str | None = None
    aws_region: str | None = None
    aws_out_dir: str | None = None
    aws_call_caching_dup_strat: str | None = None
    slurm_partition: str | None = None
    slurm_account: str | None = None
    slurm_extra_param: str | None = None
    sge_pe: str | None = None
    sge_queue: str | None = None
    sge_extra_param: str | None = None
    pbs_queue: str | None = None
    pbs_extra_param: str | None = None
    lsf_queue: str | None = None
    lsf_extra_param: str | None = None

    # Run-specific args
    metadata_output: str | None = None
    java_heap_run: str | None = None

    # HPC-specific
    leader_job_name: str | None = None
    slurm_leader_job_resource_param: str = ' '.join(SlurmWrapper.DEFAULT_LEADER_JOB_RESOURCE_PARAM)
    sge_leader_job_resource_param: str = ' '.join(SgeWrapper.DEFAULT_LEADER_JOB_RESOURCE_PARAM)
    pbs_leader_job_resource_param: str = ' '.join(PbsWrapper.DEFAULT_LEADER_JOB_RESOURCE_PARAM)
    lsf_leader_job_resource_param: str = ' '.join(LsfWrapper.DEFAULT_LEADER_JOB_RESOURCE_PARAM)

    def __post_init__(self) -> None:
        """Validate and normalize."""
        if self.leader_job_name is None:
            raise ValueError(
                'Define --leader-job-name [LEADER_JOB_NAME] in the command line arguments.'
            )
        if self.backend not in ('slurm', 'sge', 'pbs', 'lsf'):
            raise ValueError(f'Unsupported backend {self.backend} for hpc')

        self.wdl = get_abspath(self.wdl) or self.wdl
        self.inputs = get_abspath(self.inputs) if self.inputs else None
        self.options = get_abspath(self.options) if self.options else None
        self.labels = get_abspath(self.labels) if self.labels else None
        self.imports = get_abspath(self.imports) if self.imports else None

    def to_caper_run_command(self) -> list[str]:
        """Build the 'caper run' command for HPC job submission."""
        cmd = ['caper', 'run', self.wdl]

        if self.conf != caper_args.DEFAULT_CAPER_CONF:
            cmd.extend(['-c', self.conf])
        if self.debug:
            cmd.append('-D')
        if self.inputs:
            cmd.extend(['-i', self.inputs])
        if self.options:
            cmd.extend(['-o', self.options])
        if self.labels:
            cmd.extend(['-l', self.labels])
        if self.imports:
            cmd.extend(['-p', self.imports])
        if self.str_label:
            cmd.extend(['-s', self.str_label])
        if self.backend:
            cmd.extend(['-b', self.backend])
        if self.dry_run:
            cmd.append('--dry-run')
        if self.hold:
            cmd.append('--hold')
        if self.use_gsutil_for_s3:
            cmd.append('--use-gsutil-for-s3')
        if self.no_deepcopy:
            cmd.append('--no-deepcopy')
        if self.ignore_womtool:
            cmd.append('--ignore-womtool')
        if self.docker is not None:
            cmd.append('--docker' if self.docker == '' else f'--docker={self.docker}')
        if self.singularity is not None:
            cmd.append('--singularity' if self.singularity == '' else f'--singularity={self.singularity}')
        if self.conda is not None:
            cmd.append('--conda' if self.conda == '' else f'--conda={self.conda}')
        if self.metadata_output:
            cmd.extend(['-m', self.metadata_output])
        if self.java_heap_run:
            cmd.extend(['--java-heap-run', self.java_heap_run])
        if self.womtool:
            cmd.extend(['--womtool', self.womtool])
        if self.java_heap_womtool:
            cmd.extend(['--java-heap-womtool', self.java_heap_womtool])
        if self.max_retries:
            cmd.extend(['--max-retries', str(self.max_retries)])
        if self.memory_retry_multiplier:
            cmd.extend(['--memory-retry-multiplier', str(self.memory_retry_multiplier)])
        if self.gcp_monitoring_script:
            cmd.extend(['--gcp-monitoring-script', self.gcp_monitoring_script])
        if self.db:
            cmd.extend(['--db', self.db])
        if self.file_db:
            cmd.extend(['-d', self.file_db])
        if self.cromwell:
            cmd.extend(['--cromwell', self.cromwell])
        if self.backend_file:
            cmd.extend(['--backend-file', self.backend_file])
        if self.local_out_dir:
            cmd.extend(['--local-out-dir', self.local_out_dir])
        if self.gcp_out_dir:
            cmd.extend(['--gcp-out-dir', self.gcp_out_dir])
        if self.aws_out_dir:
            cmd.extend(['--aws-out-dir', self.aws_out_dir])
        if self.gcp_zones:
            cmd.extend(['--gcp-zones', self.gcp_zones])
        if self.slurm_partition:
            cmd.extend(['--slurm-partition', self.slurm_partition])
        if self.slurm_account:
            cmd.extend(['--slurm-account', self.slurm_account])
        if self.sge_queue:
            cmd.extend(['--sge-queue', self.sge_queue])
        if self.sge_pe:
            cmd.extend(['--sge-pe', self.sge_pe])
        if self.pbs_queue:
            cmd.extend(['--pbs-queue', self.pbs_queue])
        if self.lsf_queue:
            cmd.extend(['--lsf-queue', self.lsf_queue])

        return cmd


@dataclass
class HpcListArgs:
    """Arguments for 'caper hpc list' subcommand."""

    action: Literal['hpc'] = 'hpc'
    hpc_action: Literal['list'] = 'list'

    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None
    backend: str | None = None

    def __post_init__(self) -> None:
        """Validate backend."""
        if self.backend not in ('slurm', 'sge', 'pbs', 'lsf', None):
            raise ValueError(f'Unsupported backend {self.backend} for hpc')


@dataclass
class HpcAbortArgs:
    """Arguments for 'caper hpc abort' subcommand."""

    action: Literal['hpc'] = 'hpc'
    hpc_action: Literal['abort'] = 'abort'

    job_ids: list[str] = field(default_factory=list)

    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None
    backend: str | None = None

    def __post_init__(self) -> None:
        """Validate backend."""
        if self.backend not in ('slurm', 'sge', 'pbs', 'lsf', None):
            raise ValueError(f'Unsupported backend {self.backend} for hpc')

