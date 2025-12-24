"""Arguments for 'caper init' subcommand."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .base import get_abspath
from caper import caper_args


@dataclass
class InitArgs:
    """Arguments for 'caper init' subcommand."""

    # Required positional (must come first)
    platform: str

    # Action identifier
    action: Literal['init'] = 'init'

    # Common args
    conf: str = caper_args.DEFAULT_CAPER_CONF
    debug: bool = False
    gcp_service_account_key_json: str | None = None
    local_loc_dir: str | None = None
    gcp_loc_dir: str | None = None
    aws_loc_dir: str | None = None

    def __post_init__(self) -> None:
        """Normalize paths."""
        self.gcp_service_account_key_json = (
            get_abspath(self.gcp_service_account_key_json)
            if self.gcp_service_account_key_json
            else None
        )
        if self.local_loc_dir:
            self.local_loc_dir = get_abspath(self.local_loc_dir) or self.local_loc_dir

