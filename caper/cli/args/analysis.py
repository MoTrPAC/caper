"""Arguments for analysis subcommands (gcp_monitor, gcp_res_analysis, cleanup)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .base import get_abspath
from caper import caper_args
from caper.cromwell_rest_api import CromwellRestAPI
from caper.resource_analysis import ResourceAnalysis
from caper.server_heartbeat import ServerHeartbeat
from autouri import URIBase


@dataclass
class GcpMonitorArgs:
    """Arguments for 'caper gcp_monitor' subcommand."""

    action: Literal['gcp_monitor'] = 'gcp_monitor'
    wf_id_or_label: list[str] = field(default_factory=list)

    # Common + server/client args
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None
    port: int = CromwellRestAPI.DEFAULT_PORT
    no_server_heartbeat: bool = False
    server_heartbeat_file: str = ServerHeartbeat.DEFAULT_SERVER_HEARTBEAT_FILE
    server_heartbeat_timeout: int = ServerHeartbeat.DEFAULT_HEARTBEAT_TIMEOUT_MS
    hostname: str = CromwellRestAPI.DEFAULT_HOSTNAME

    # GCP monitor-specific
    json_format: bool = False

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )


@dataclass
class GcpResAnalysisArgs:
    """Arguments for 'caper gcp_res_analysis' subcommand."""

    action: Literal['gcp_res_analysis'] = 'gcp_res_analysis'
    wf_id_or_label: list[str] = field(default_factory=list)

    # Common + server/client args
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None
    port: int = CromwellRestAPI.DEFAULT_PORT
    no_server_heartbeat: bool = False
    server_heartbeat_file: str = ServerHeartbeat.DEFAULT_SERVER_HEARTBEAT_FILE
    server_heartbeat_timeout: int = ServerHeartbeat.DEFAULT_HEARTBEAT_TIMEOUT_MS
    hostname: str = CromwellRestAPI.DEFAULT_HOSTNAME

    # Analysis-specific
    in_file_vars_def_json: str | None = None
    reduce_in_file_vars: str = 'SUM'  # ResourceAnalysisReductionMethod.SUM.name
    target_resources: list[str] = field(
        default_factory=lambda: list(ResourceAnalysis.DEFAULT_TARGET_RESOURCES)
    )
    plot_pdf: str | None = None

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        self.in_file_vars_def_json = (
            get_abspath(self.in_file_vars_def_json) if self.in_file_vars_def_json else None
        )
        self.plot_pdf = get_abspath(self.plot_pdf) if self.plot_pdf else None


@dataclass
class CleanupArgs:
    """Arguments for 'caper cleanup' subcommand."""

    action: Literal['cleanup'] = 'cleanup'
    wf_id_or_label: list[str] = field(default_factory=list)

    # Common + server/client args
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None
    port: int = CromwellRestAPI.DEFAULT_PORT
    no_server_heartbeat: bool = False
    server_heartbeat_file: str = ServerHeartbeat.DEFAULT_SERVER_HEARTBEAT_FILE
    server_heartbeat_timeout: int = ServerHeartbeat.DEFAULT_HEARTBEAT_TIMEOUT_MS
    hostname: str = CromwellRestAPI.DEFAULT_HOSTNAME

    # Cleanup-specific
    delete: bool = False
    num_threads: int = URIBase.DEFAULT_NUM_THREADS

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )

