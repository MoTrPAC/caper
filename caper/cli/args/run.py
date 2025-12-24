"""Arguments for 'caper run' subcommand."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

from caper import caper_args
from caper.caper_base import CaperBase
from caper.caper_workflow_opts import CaperWorkflowOpts
from caper.cromwell import Cromwell
from caper.cromwell_backend import (
    CromwellBackendAws,
    CromwellBackendBase,
    CromwellBackendCommon,
    CromwellBackendDatabase,
    CromwellBackendGcp,
    CromwellBackendLocal,
    CromwellBackendSlurm,
)

from .base import get_abspath, split_delimited

DEFAULT_DB_FILE_PREFIX = 'caper-db'


@dataclass
class RunArgs:
    """Arguments for 'caper run' subcommand."""

    # Positional
    wdl: str

    # Action identifier (set by argparse)
    action: Literal['run'] = 'run'

    # Common args (parent_all)
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None

    # Submit IO args (parent_submit)
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

    # Dependency resolver args
    docker: str | None = None
    singularity: str | None = None
    conda: str | None = None

    # Run-specific args (parent_run)
    metadata_output: str | None = None
    java_heap_run: str = Cromwell.DEFAULT_JAVA_HEAP_CROMWELL_RUN

    # Backend args (parent_backend)
    backend: str | None = None
    dry_run: bool = False
    gcp_zones: str | None = None  # Will be split in __post_init__

    # Runner/Cromwell args (parent_runner)
    db: str = CromwellBackendDatabase.DB_FILE
    db_timeout: int = CromwellBackendDatabase.DEFAULT_DB_TIMEOUT_MS
    file_db: str | None = None
    mysql_db_ip: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_IP
    mysql_db_port: int = CromwellBackendDatabase.DEFAULT_MYSQL_DB_PORT
    mysql_db_user: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_USER
    mysql_db_password: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_PASSWORD
    mysql_db_name: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_NAME
    postgresql_db_ip: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_IP
    postgresql_db_port: int = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_PORT
    postgresql_db_user: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_USER
    postgresql_db_password: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_PASSWORD
    postgresql_db_name: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_NAME

    cromwell: str = Cromwell.DEFAULT_CROMWELL
    max_concurrent_tasks: int = CromwellBackendBase.DEFAULT_CONCURRENT_JOB_LIMIT
    max_concurrent_workflows: int = CromwellBackendCommon.DEFAULT_MAX_CONCURRENT_WORKFLOWS
    memory_retry_error_keys: str = ','.join(CromwellBackendCommon.DEFAULT_MEMORY_RETRY_ERROR_KEYS)
    disable_call_caching: bool = False
    backend_file: str | None = None
    soft_glob_output: bool = False
    local_hash_strat: str = CromwellBackendLocal.DEFAULT_LOCAL_HASH_STRAT
    cromwell_stdout: str = caper_args.DEFAULT_CROMWELL_STDOUT

    # Local backend args
    local_out_dir: str = caper_args.DEFAULT_OUT_DIR
    slurm_resource_param: str = CromwellBackendSlurm.DEFAULT_SLURM_RESOURCE_PARAM

    # GCP args
    gcp_prj: str | None = None
    gcp_region: str = CromwellBackendGcp.DEFAULT_REGION
    gcp_compute_service_account: str | None = None
    gcp_call_caching_dup_strat: str = 'reference'  # CromwellBackendGcp.DEFAULT_CALL_CACHING_DUP_STRAT[0]
    gcp_out_dir: str | None = None

    # AWS args
    aws_batch_arn: str | None = None
    aws_region: str | None = None
    aws_out_dir: str | None = None
    aws_call_caching_dup_strat: str = 'reference'  # CromwellBackendAws.DEFAULT_CALL_CACHING_DUP_STRAT

    # Scheduler args
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

    # Processed fields (set in __post_init__)
    gcp_zones_list: list[str] | None = field(default=None, init=False)
    memory_retry_error_keys_list: list[str] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Normalize paths and split delimited strings."""
        # Path normalization
        self.wdl = get_abspath(self.wdl) or self.wdl
        self.inputs = get_abspath(self.inputs) if self.inputs else None
        self.options = get_abspath(self.options) if self.options else None
        self.labels = get_abspath(self.labels) if self.labels else None
        self.imports = get_abspath(self.imports) if self.imports else None
        self.metadata_output = get_abspath(self.metadata_output) if self.metadata_output else None
        self.backend_file = get_abspath(self.backend_file) if self.backend_file else None
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        self.cromwell = get_abspath(self.cromwell) or self.cromwell
        self.womtool = get_abspath(self.womtool) or self.womtool
        self.file_db = get_abspath(self.file_db) if self.file_db else None

        # Local directories - expand and make absolute
        self.local_out_dir = os.path.abspath(os.path.expanduser(self.local_out_dir))
        if self.local_loc_dir:
            self.local_loc_dir = os.path.abspath(os.path.expanduser(self.local_loc_dir))
        else:
            self.local_loc_dir = os.path.join(self.local_out_dir, CaperBase.DEFAULT_LOC_DIR_NAME)

        # Cloud loc dirs - derive from out dirs if not set
        if self.gcp_out_dir and not self.gcp_loc_dir:
            self.gcp_loc_dir = os.path.join(self.gcp_out_dir, CaperBase.DEFAULT_LOC_DIR_NAME)
        if self.aws_out_dir and not self.aws_loc_dir:
            self.aws_loc_dir = os.path.join(self.aws_out_dir, CaperBase.DEFAULT_LOC_DIR_NAME)

        # DB path
        if self.db == CromwellBackendDatabase.DB_FILE and not self.file_db:
            db_parts = [DEFAULT_DB_FILE_PREFIX]
            if self.wdl:
                db_parts.append(os.path.basename(self.wdl))
            if self.inputs:
                db_parts.append(os.path.basename(self.inputs))
            self.file_db = os.path.join(self.local_out_dir, '_'.join(db_parts))

        # Split delimited strings
        self.gcp_zones_list = split_delimited(self.gcp_zones)
        self.memory_retry_error_keys_list = split_delimited(self.memory_retry_error_keys)

        # Validate mutually exclusive flags
        self._validate_dependency_flags()

    def _validate_dependency_flags(self) -> None:
        """Validate --docker, --singularity, --conda are mutually exclusive."""
        flags = [
            self.docker is not None,
            self.singularity is not None,
            self.conda is not None,
        ]
        if sum(flags) > 1:
            raise ValueError('--docker, --singularity and --conda are mutually exclusive.')

        # Check for WDL file being eaten by nargs='?'
        for name, value in [
            ('docker', self.docker),
            ('singularity', self.singularity),
            ('conda', self.conda),
        ]:
            if value and value.endswith(('.wdl', '.cwl')):
                raise ValueError(
                    f'--{name} ate up positional arguments (e.g. WDL, CWL). '
                    f'Define --{name} at the end of command line arguments. {name}={value}'
                )

        if self.docker is not None and self.soft_glob_output:
            raise ValueError(
                '--soft-glob-output and --docker are mutually exclusive. '
                'Delocalization from docker container will fail for soft-linked globbed outputs.'
            )

