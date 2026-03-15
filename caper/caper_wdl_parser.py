"""WDL parser with Caper-specific functionality."""

from __future__ import annotations

import logging
import re

from caper.wdl_parser import WDLParser

logger = logging.getLogger(__name__)


class CaperWDLParser(WDLParser):
    """WDL parser for Caper."""

    RE_WDL_COMMENT_DOCKER = re.compile(r'^\s*#\s*CAPER\s+docker\s(.+)')
    RE_WDL_COMMENT_SINGULARITY = re.compile(r'^\s*#\s*CAPER\s+singularity\s(.+)')
    WDL_WORKFLOW_META_DOCKER_KEYS = ('default_docker', 'caper_docker')
    WDL_WORKFLOW_META_SINGULARITY_KEYS = ('default_singularity', 'caper_singularity')
    WDL_WORKFLOW_META_CONDA_KEYS = (
        'default_conda',
        'default_conda_env',
        'caper_conda',
        'caper_conda_env',
    )

    def __init__(self, wdl: str) -> None:  # noqa: D107
        super().__init__(wdl)

    @property
    def default_docker(self) -> str | None:
        """
        Find a default Docker image in WDL for Caper.

        Backward compatibililty:
            Keep using old regex method
            if WDL_WORKFLOW_META_DOCKER doesn't exist in workflow's meta
        """
        if self.workflow_meta:
            for docker_key in CaperWDLParser.WDL_WORKFLOW_META_DOCKER_KEYS:
                if docker_key in self.workflow_meta:
                    return self.workflow_meta[docker_key]

        ret = self._find_val_of_matched_lines(self.RE_WDL_COMMENT_DOCKER)
        if ret:
            return ret[0].strip('"\'')
        return None

    @property
    def default_singularity(self) -> str | None:
        """
        Find a default Singularity image in WDL for Caper.

        Backward compatibililty:
            Keep using old regex method
            if WDL_WORKFLOW_META_SINGULARITY doesn't exist in workflow's meta
        """
        if self.workflow_meta:
            for singularity_key in CaperWDLParser.WDL_WORKFLOW_META_SINGULARITY_KEYS:
                if singularity_key in self.workflow_meta:
                    return self.workflow_meta[singularity_key]

        ret = self._find_val_of_matched_lines(self.RE_WDL_COMMENT_SINGULARITY)
        if ret:
            return ret[0].strip('"\'')
        return None

    @property
    def default_conda(self) -> None:
        """Find a default Conda environment name in WDL for Caper."""
        if self.workflow_meta:
            for conda_key in CaperWDLParser.WDL_WORKFLOW_META_CONDA_KEYS:
                if conda_key in self.workflow_meta:
                    return self.workflow_meta[conda_key]
        return None
