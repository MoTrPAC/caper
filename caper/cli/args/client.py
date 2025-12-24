"""Arguments for client subcommands (abort, unhold, list, metadata, troubleshoot)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .base import get_abspath
from caper import caper_args
from caper.cromwell_rest_api import CromwellRestAPI
from caper.server_heartbeat import ServerHeartbeat

DEFAULT_LIST_FORMAT = 'id,status,name,str_label,user,parent,submission'


@dataclass
class AbortArgs:
    """Arguments for 'caper abort' subcommand."""

    action: Literal['abort'] = 'abort'
    wf_id_or_label: list[str] = field(default_factory=list)

    # Common
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None

    # Server/client
    port: int = CromwellRestAPI.DEFAULT_PORT
    no_server_heartbeat: bool = False
    server_heartbeat_file: str = ServerHeartbeat.DEFAULT_SERVER_HEARTBEAT_FILE
    server_heartbeat_timeout: int = ServerHeartbeat.DEFAULT_HEARTBEAT_TIMEOUT_MS
    hostname: str = CromwellRestAPI.DEFAULT_HOSTNAME

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        if self.local_loc_dir:
            self.local_loc_dir = get_abspath(self.local_loc_dir) or self.local_loc_dir


@dataclass
class UnholdArgs:
    """Arguments for 'caper unhold' subcommand."""

    action: Literal['unhold'] = 'unhold'
    wf_id_or_label: list[str] = field(default_factory=list)

    # Common + server/client args (same as AbortArgs)
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

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        if self.local_loc_dir:
            self.local_loc_dir = get_abspath(self.local_loc_dir) or self.local_loc_dir


@dataclass
class ListArgs:
    """Arguments for 'caper list' subcommand."""

    action: Literal['list'] = 'list'
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

    # List-specific
    format: str = DEFAULT_LIST_FORMAT
    hide_result_before: str | None = None
    show_subworkflow: bool = False

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        if self.local_loc_dir:
            self.local_loc_dir = get_abspath(self.local_loc_dir) or self.local_loc_dir


@dataclass
class MetadataArgs:
    """Arguments for 'caper metadata' subcommand."""

    action: Literal['metadata'] = 'metadata'
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

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )


@dataclass
class TroubleshootArgs:
    """Arguments for 'caper troubleshoot' and 'caper debug' subcommands."""

    action: Literal['troubleshoot', 'debug'] = 'troubleshoot'
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

    # Troubleshoot-specific
    show_completed_task: bool = False
    show_stdout: bool = False

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )

