"""Arguments for 'caper submit' subcommand."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .base import get_abspath, split_delimited
from caper import caper_args
from caper.caper_workflow_opts import CaperWorkflowOpts
from caper.cromwell import Cromwell
from caper.cromwell_rest_api import CromwellRestAPI
from caper.server_heartbeat import ServerHeartbeat


@dataclass
class SubmitArgs:
    """Arguments for 'caper submit' subcommand."""

    wdl: str
    action: Literal['submit'] = 'submit'

    # Common args
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None

    # Submit IO args
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
    backend: str | None = None
    dry_run: bool = False
    gcp_zones: str | None = None

    # Server/client args
    port: int = CromwellRestAPI.DEFAULT_PORT
    no_server_heartbeat: bool = False
    server_heartbeat_file: str = ServerHeartbeat.DEFAULT_SERVER_HEARTBEAT_FILE
    server_heartbeat_timeout: int = ServerHeartbeat.DEFAULT_HEARTBEAT_TIMEOUT_MS
    hostname: str = CromwellRestAPI.DEFAULT_HOSTNAME

    # Scheduler args (for workflow options)
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

    # GCP args
    gcp_compute_service_account: str | None = None

    # Processed
    gcp_zones_list: list[str] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Normalize paths and validate."""
        self.wdl = get_abspath(self.wdl) or self.wdl
        self.inputs = get_abspath(self.inputs) if self.inputs else None
        self.options = get_abspath(self.options) if self.options else None
        self.labels = get_abspath(self.labels) if self.labels else None
        self.imports = get_abspath(self.imports) if self.imports else None
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        self.womtool = get_abspath(self.womtool) or self.womtool

        if self.local_loc_dir:
            self.local_loc_dir = get_abspath(self.local_loc_dir) or self.local_loc_dir

        self.gcp_zones_list = split_delimited(self.gcp_zones)

        # Validate dependency flags
        flags = [self.docker is not None, self.singularity is not None, self.conda is not None]
        if sum(flags) > 1:
            msg = '--docker, --singularity and --conda are mutually exclusive.'
            raise ValueError(msg)

