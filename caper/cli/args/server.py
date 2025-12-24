"""Arguments for 'caper server' subcommand."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

from caper import caper_args
from caper.caper_base import CaperBase
from caper.cli.args.base import get_abspath, split_delimited
from caper.cromwell import Cromwell
from caper.cromwell_backend import (
    CromwellBackendBase,
    CromwellBackendCommon,
    CromwellBackendDatabase,
    CromwellBackendGcp,
    CromwellBackendLocal,
    CromwellBackendSlurm,
)
from caper.cromwell_rest_api import CromwellRestAPI
from caper.server_heartbeat import ServerHeartbeat


@dataclass
class ServerArgs:
    """Arguments for 'caper server' subcommand."""

    action: Literal['server'] = 'server'

    # Common args
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None

    # Server-specific args
    java_heap_server: str = Cromwell.DEFAULT_JAVA_HEAP_CROMWELL_SERVER
    disable_auto_write_metadata: bool = False

    # Server/client args
    port: int = CromwellRestAPI.DEFAULT_PORT
    no_server_heartbeat: bool = False
    server_heartbeat_file: str = ServerHeartbeat.DEFAULT_SERVER_HEARTBEAT_FILE
    server_heartbeat_timeout: int = ServerHeartbeat.DEFAULT_HEARTBEAT_TIMEOUT_MS

    # Backend args
    backend: str | None = None
    dry_run: bool = False
    gcp_zones: str | None = None

    # Runner args (all the same as RunArgs)
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
    local_out_dir: str = caper_args.DEFAULT_OUT_DIR
    slurm_resource_param: str = CromwellBackendSlurm.DEFAULT_SLURM_RESOURCE_PARAM
    gcp_prj: str | None = None
    gcp_region: str = CromwellBackendGcp.DEFAULT_REGION
    gcp_compute_service_account: str | None = None
    gcp_call_caching_dup_strat: str = 'reference'  # CromwellBackendGcp.DEFAULT_CALL_CACHING_DUP_STRAT[0]
    gcp_out_dir: str | None = None
    aws_batch_arn: str | None = None
    aws_region: str | None = None
    aws_out_dir: str | None = None
    aws_call_caching_dup_strat: str = 'reference'  # CromwellBackendAws.DEFAULT_CALL_CACHING_DUP_STRAT
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

    # Processed fields
    gcp_zones_list: list[str] | None = field(default=None, init=False)
    memory_retry_error_keys_list: list[str] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Normalize paths and split delimited strings."""
        # Similar normalization as RunArgs
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        self.cromwell = get_abspath(self.cromwell) or self.cromwell
        self.backend_file = get_abspath(self.backend_file) if self.backend_file else None
        self.file_db = get_abspath(self.file_db) if self.file_db else None

        self.local_out_dir = os.path.abspath(os.path.expanduser(self.local_out_dir))
        if self.local_loc_dir:
            self.local_loc_dir = os.path.abspath(os.path.expanduser(self.local_loc_dir))
        else:
            self.local_loc_dir = os.path.join(self.local_out_dir, CaperBase.DEFAULT_LOC_DIR_NAME)

        if self.gcp_out_dir and not self.gcp_loc_dir:
            self.gcp_loc_dir = os.path.join(self.gcp_out_dir, CaperBase.DEFAULT_LOC_DIR_NAME)
        if self.aws_out_dir and not self.aws_loc_dir:
            self.aws_loc_dir = os.path.join(self.aws_out_dir, CaperBase.DEFAULT_LOC_DIR_NAME)

        self.gcp_zones_list = split_delimited(self.gcp_zones)
        self.memory_retry_error_keys_list = split_delimited(self.memory_retry_error_keys)

