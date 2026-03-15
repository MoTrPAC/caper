"""WDL parsing module."""

from __future__ import annotations

import logging
import os
import re
import shutil
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from autouri import HTTPURL, AbsPath, AutoURI
from WDL import parse_document

if TYPE_CHECKING:
    from WDL.Tree import Document

logger = logging.getLogger(__name__)


class WDLParser:
    """Wrapper for miniwdl's WDL parser."""

    RE_WDL_IMPORT = re.compile(r'^\s*import\s+[\"\'](.+)[\"\']\s*')
    RECURSION_DEPTH_LIMIT = 20
    BASENAME_IMPORTS = 'imports.zip'
    _wdl_doc: Document | None

    def __init__(self, wdl: str) -> None:
        """Wraps miniwdl's parse_document()."""
        u = AutoURI(wdl)
        if not u.exists:
            msg = f'WDL does not exist: wdl={wdl}'
            raise FileNotFoundError(msg)
        self._wdl = wdl
        self._wdl_contents = AutoURI(wdl).read()
        try:
            self._wdl_doc = parse_document(self._wdl_contents)
        except Exception:
            logger.exception('Failed to parse WDL with miniwdl.')
            self._wdl_doc = None

    @property
    def contents(self) -> str:
        """String contents of the WDL file."""
        return self._wdl_contents

    @property
    def workflow_meta(self) -> dict[str, Any] | None:
        """Retrieve the optional `meta` section from the workflow declaration."""
        if self._wdl_doc and self._wdl_doc.workflow:
            return self._wdl_doc.workflow.meta
        return None

    @property
    def workflow_parameter_meta(self) -> dict[str, Any | None] | None:
        """Retrieve the optional `parameter_meta` section from the workflow declaration."""
        if self._wdl_doc and self._wdl_doc.workflow:
            return self._wdl_doc.workflow.parameter_meta
        return None

    @property
    def imports(self) -> list[str]:
        """
        Miniwdl (0.3.7) has a bug for URL imports.

        Keep using reg-ex to find imports until it's fixed.

        Returns:
            List of URIs of imported subworkflows.
        """
        try:
            return [i.uri for i in self._wdl_doc.imports] if self._wdl_doc else []
        except Exception:
            logger.exception('Failed to get imports from WDL with miniwdl.')
        return self._find_val_of_matched_lines(self.RE_WDL_IMPORT)

    def zip_subworkflows(self, zip_file: str) -> str | None:
        """
        Recursively find/zip imported subworkflow WDLs.

        This will zip sub-WDLs with relative paths only, i.e. URIs are ignored. For this (main)
        workflow, any URI is allowed. However, only subworkflows with relative paths will be
        zipped since there is no way to make directory structure to zip them.

        Returns:
            Zipped imports file.
            None if no subworkflows recursively found in WDL.
        """
        with TemporaryDirectory() as tmp_d:
            # localize WDL first. If it's already local
            # then will use its original path without loc.
            wdl = AutoURI(self._wdl).localize_on(tmp_d)
            # keep directory structure as they imported
            num_sub_wf_packed = self.recurse_zip_subworkflows(
                root_zip_dir=tmp_d, root_wdl_dir=AutoURI(wdl).dirname
            )
            if num_sub_wf_packed:
                shutil.make_archive(AutoURI(zip_file).uri_wo_ext, 'zip', tmp_d)
                return zip_file
        return None

    def create_imports_file(self, directory: str, basename: str = BASENAME_IMPORTS) -> str | None:
        """
        Wrapper for zip_subworkflows.

        This creates an imports zip file with basename on directory.
        """
        zip_file = os.path.join(directory, basename)
        if self.zip_subworkflows(zip_file):
            return zip_file
        return None

    def _find_val_of_matched_lines(
        self, regex: re.Pattern[str], *, no_strip: bool = False
    ) -> list[str]:
        """
        Find value of the first line matching regex.

        Args:
            regex:
                Regular expression. This should have only one ().
            no_strip:
                Do not strip result strings.

        Returns:
            Value of the first line matching regex.
        """
        res = []
        for line in self.contents.split('\n'):
            r = regex.findall(line)
            if len(r) > 0:
                res.append(r[0] if no_strip else r[0].strip())
        return res

    def recurse_zip_subworkflows(
        self,
        root_zip_dir: str,
        root_wdl_dir: str,
        imported_as_url: bool = False,
        depth: int = 0,
    ) -> int:
        """
        Recurse imported sub-WDLs in main-WDL.

        Unlike Cromwell, Womtool does not take imports.zip while validating WDLs.
        All sub-WDLs should be in a correct directory structure relative to the
        root WDL.
        For Womtool, we should make a temporary directory and unpack imports.zip there and
        need to make a copy of root WDL on it. Then run Womtool to validate them.
        This function is to make such imports.zip.
        Sub-WDLs imported as relative path simply inherit parent's directory.
        Sub-WDLs imported as URL does not inherit parent's directory but root
        WDL's directory.
        Sub-WDLs imported as absolute path are not allowed. This can work with "caper run"
        but not with "caper submit" (or Cromwell submit).

        Args:
            root_zip_dir:
                Root directory for the imports zip file.
            root_wdl_dir:
                Root directory containing the main WDL file.
            imported_as_url:
                Whether the current WDL was imported as a URL.
            depth:
                Recursion depth.

        Returns:
            Total number of subworkflows:
                Sub WDL files "recursively" localized on "root_zip_dir".

        """
        if depth > WDLParser.RECURSION_DEPTH_LIMIT:
            msg = (
                'Reached recursion depth limit while zipping subworkflows recursively. '
                f'Possible cyclic import or self-refencing in WDLs? wdl={self._wdl}'
            )
            raise ValueError(msg)

        main_wdl_dir = root_wdl_dir if imported_as_url else AbsPath(self._wdl).dirname

        num_sub_wf_packed = 0
        for sub_rel_to_parent in self.imports:
            sub_wdl_file = AutoURI(sub_rel_to_parent)

            if isinstance(sub_wdl_file, HTTPURL):
                sub_abs = sub_wdl_file.uri
                imported_as_url_sub = True
            elif isinstance(sub_wdl_file, AbsPath):
                msg = (
                    'For sub WDL zipping, absolute path is not allowed for sub WDL. '
                    f'main={self._wdl}, sub={sub_rel_to_parent}'
                )
                raise TypeError(msg)
            else:
                sub_abs = os.path.realpath(os.path.join(main_wdl_dir, sub_rel_to_parent))
                if not AbsPath(sub_abs).exists:
                    msg = (
                        'Sub WDL does not exist. Did you import main WDL as a URL but sub WDL references a local file? '
                        f'main={self._wdl}, sub={sub_rel_to_parent}, imported_as_url={imported_as_url}'
                    )
                    raise FileNotFoundError(msg)
                if not sub_abs.startswith(root_wdl_dir):
                    msg = (
                        'Sub WDL exists but it is out of root WDL directory. Too many "../" in your sub WDL? '
                        'Or main WDL is imported as an URL but sub WDL has "../"? '
                        f'main={self._wdl}, sub={sub_rel_to_parent}, imported_as_url={imported_as_url}'
                    )
                    raise ValueError(msg)

                # make a copy on zip_dir
                rel_path = os.path.relpath(sub_abs, root_wdl_dir)
                cp_dest = os.path.join(root_zip_dir, rel_path)

                AbsPath(sub_abs).cp(cp_dest)
                num_sub_wf_packed += 1
                imported_as_url_sub = False

            num_sub_wf_packed += WDLParser(sub_abs).recurse_zip_subworkflows(
                root_zip_dir=root_zip_dir,
                root_wdl_dir=root_wdl_dir,
                imported_as_url=imported_as_url_sub,
                depth=depth + 1,
            )
        return num_sub_wf_packed
