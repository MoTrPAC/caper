"""Helpers for reading and analyzing Cromwell metadata."""

import io
import json
import logging
import os
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Self, TypeVar, cast

import humanfriendly
import numpy as np
import pandas as pd
from autouri import GCSURI, AbsPath, AutoURI, URIBase
from pandas.errors import EmptyDataError

from .dict_tool import recurse_dict_value

logger = logging.getLogger(__name__)


def get_workflow_root_from_call(call: dict) -> str | None:
    """Returns workflow's root directory from a call."""
    call_root = call.get('callRoot')
    if call_root:
        return '/'.join(call_root.split('/')[:-1])
    return None


def get_workflow_id_from_workflow_root(workflow_root: str | None) -> str | None:
    """Returns workflow's ID from a workflow's root directory."""
    if workflow_root:
        return workflow_root.split('/')[-1]
    return None


def parse_cromwell_disks(s: str | None) -> int | None:
    """Parses Cromwell's disks in runtime attribute."""
    if s:
        matches = re.findall(r'(\d+)', s)
        if matches:
            return int(matches[0]) * 1024 * 1024 * 1024
    return None


def parse_cromwell_memory(s: str | None) -> int | None:
    """Parses Cromwell's memory runtime attribute."""
    if s:
        return humanfriendly.parse_size(s)
    return None


T = TypeVar('T')


def convert_type_np_to_py(o: np.generic) -> T:
    """Convert numpy type to Python type."""
    if isinstance(o, np.generic):
        return o.item()
    raise TypeError


@dataclass(frozen=True, slots=True)
class _CallContext:
    """Internal context for a single Cromwell call (task)."""

    call_name: str
    call: dict[str, Any]
    parent_call_names: tuple[str, ...] = ()


class CromwellMetadata:
    """Metadata helper for Cromwell workflows."""

    DEFAULT_METADATA_BASENAME = 'metadata.json'
    DEFAULT_GCP_MONITOR_STAT_METHODS = ('mean', 'std', 'max', 'min', 'last')

    def __init__(self, metadata: dict | Self | str) -> None:
        """Load metadata from a dict, another instance, or a JSON file path/URI."""
        if isinstance(metadata, dict):
            self._metadata = metadata
        elif isinstance(metadata, CromwellMetadata):
            self._metadata = metadata._metadata  # noqa: SLF001
        else:
            s = cast('str', AutoURI(metadata).read())
            self._metadata = json.loads(s)

    @property
    def data(self) -> dict:
        """Raw metadata dictionary."""
        return self._metadata

    @property
    def metadata(self) -> dict:
        """Alias for raw metadata dictionary."""
        return self._metadata

    @property
    def workflow_id(self) -> str | None:
        """Workflow ID string or None if absent."""
        return self._metadata.get('id')

    @property
    def workflow_status(self) -> str | None:
        """Workflow status (e.g., Succeeded, Failed) or None if absent."""
        return self._metadata.get('status')

    @property
    def workflow_root(self) -> str | None:
        """Best-effort workflow root directory (explicit or inferred)."""
        if 'workflowRoot' in self._metadata:
            return self._metadata['workflowRoot']
        workflow_roots = [get_workflow_root_from_call(call) for _, call, _ in self.recursed_calls]
        common_root = os.path.commonprefix([r for r in workflow_roots if r])
        if common_root:
            guessed_workflow_id = get_workflow_id_from_workflow_root(common_root)
            if guessed_workflow_id == self.workflow_id:
                return common_root
            logger.error(
                'workflowRoot not found in metadata JSON. '
                'Tried to guess from callRoot of each call but failed.'
            )
        return None

    @property
    def failures(self) -> Any:
        """Failures object from metadata, if present."""
        return self._metadata.get('failures')

    @property
    def calls(self) -> Any:
        """Calls object from metadata (tasks and subworkflows)."""
        return self._metadata.get('calls')

    def iter_call_contexts(
        self,
        *,
        parent_call_names: tuple[str, ...] = (),
    ) -> Iterator[_CallContext]:
        """Yield leaf task calls, descending into subWorkflowMetadata."""
        calls = self.calls or {}
        for call_name, call_list in calls.items():
            for call in call_list:
                sub = call.get('subWorkflowMetadata')
                if sub is not None:
                    yield from CromwellMetadata(sub).iter_call_contexts(
                        parent_call_names=(*parent_call_names, call_name)
                    )
                else:
                    yield _CallContext(
                        call_name=call_name,
                        call=call,
                        parent_call_names=parent_call_names,
                    )

    @property
    def recursed_calls(self) -> Iterator[tuple[str, dict[str, Any], tuple[str, ...]]]:
        """Generator of (call_name, call, parent_call_names) for all leaf tasks."""
        for ctx in self.iter_call_contexts():
            yield (ctx.call_name, ctx.call, ctx.parent_call_names)

    def recurse_calls(
        self,
        fn_call: Callable[[str, dict[str, Any], tuple[str, ...]], T],
        parent_call_names: tuple[str, ...] = (),
    ) -> Iterator[T]:
        """
        Recurse on tasks in metadata.

        Args:
            fn_call:
                Function to be called recursively for each call (task).
                This function should take the following three arguments.
                    call_name:
                        Call's name. i.e. key in the original metadata JSON's `calls` dict.
                    call:
                        Call object. i.e. value in the original metadata JSON's `calls` dict.
                    parent_call_names:
                        Tuple of Parent call's names.
                        e.g. (..., great grand parent, grand parent, parent, ...)
            parent_call_names:
                Tuple of Parent call's names.
                e.g. (..., great grand parent, grand parent, parent, ...)

        Returns:
            Generator object for all calls.
        """
        yield from (
            fn_call(ctx.call_name, ctx.call, ctx.parent_call_names)
            for ctx in self.iter_call_contexts(parent_call_names=parent_call_names)
        )

    def write_on_workflow_root(self, basename: str = DEFAULT_METADATA_BASENAME) -> str | None:
        """Update metadata JSON file on metadata's output root directory."""
        root = self.workflow_root

        if root:
            metadata_file = os.path.join(root, basename)

            AutoURI(metadata_file).write(json.dumps(self._metadata, indent=4) + '\n')
            logger.info('Wrote metadata file. %s', metadata_file)

            return metadata_file
        return None

    @staticmethod
    def _get_running_window(call: dict[str, Any]) -> tuple[str | None, str | None]:
        """Extract start/end times from Running execution event."""
        for event in call.get('executionEvents', ()):
            desc = event.get('description', '')
            if desc.startswith('Running'):
                return event.get('startTime'), event.get('endTime')
        return None, None

    def _format_troubleshoot_call(
        self,
        call_name: str,
        call: dict[str, Any],
        parent_call_names: tuple[str, ...],
        *,
        show_completed_task: bool,
        show_stdout: bool,
    ) -> str:
        """Format a single call's troubleshooting info."""
        status = call.get('executionStatus')
        if not (show_completed_task or status not in ('Done', 'Succeeded')):
            return ''

        shard_index = call.get('shardIndex')
        rc = call.get('returnCode')
        job_id = call.get('jobId')
        stdout = call.get('stdout')
        stderr = call.get('stderr')
        stderr_background = f'{stderr}.background' if stderr else None
        run_start, run_end = self._get_running_window(call)

        parts: list[str] = [
            f'\n==== NAME={call_name}, STATUS={status}, PARENT={",".join(parent_call_names)}\n',
            f'SHARD_IDX={shard_index}, RC={rc}, JOB_ID={job_id}\n',
            f'START={run_start}, END={run_end}\n',
            f'STDOUT={stdout}\nSTDERR={stderr}\n',
        ]

        if stderr:
            stderr_uri = AutoURI(stderr)
            if stderr_uri.exists:
                parts.append(f'STDERR_CONTENTS=\n{stderr_uri.read()}\n')

        if show_stdout and stdout:
            stdout_uri = AutoURI(stdout)
            if stdout_uri.exists:
                parts.append(f'STDOUT_CONTENTS=\n{stdout_uri.read()}\n')

        if stderr_background:
            bg_uri = AutoURI(stderr_background)
            if bg_uri.exists:
                parts.append(f'STDERR_BACKGROUND_CONTENTS=\n{bg_uri.read()}\n')

        return ''.join(parts)

    def troubleshoot(self, *, show_completed_task: bool = False, show_stdout: bool = False) -> str:
        """
        Troubleshoot a workflow by finding failed calls and printing out STDERR and STDOUT.

        Args:
            show_completed_task:
                Show STDERR/STDOUT of completed tasks.
            show_stdout:
                Show failed task's STDOUT along with STDERR.

        Return:
            result:
                Troubleshooting report as a plain string.
        """
        header = (
            f'* Started troubleshooting workflow: id={self.workflow_id}, '
            f'status={self.workflow_status}\n'
        )
        lines: list[str] = [header]

        if self.workflow_status == 'Succeeded':
            lines.append('* Workflow ran Successfully.\n')
            return ''.join(lines)

        if self.failures:
            lines.append(f'* Found failures JSON object.\n{json.dumps(self.failures, indent=4)}\n')

        lines.append('* Recursively finding failures in calls (tasks)...\n')
        for ctx in self.iter_call_contexts():
            msg = self._format_troubleshoot_call(
                ctx.call_name,
                ctx.call,
                ctx.parent_call_names,
                show_completed_task=show_completed_task,
                show_stdout=show_stdout,
            )
            if msg:
                lines.append(msg)

        return ''.join(lines)

    @staticmethod
    def _py_scalar(val: Any) -> Any:
        """Convert numpy scalar to Python native type."""
        return val.item() if isinstance(val, np.generic) else val

    @staticmethod
    def _read_tsv_dataframe(text: str) -> pd.DataFrame | None:
        """Parse TSV text into DataFrame; return None if empty or unparseable."""
        try:
            return pd.read_csv(io.StringIO(text), delimiter='\t')
        except EmptyDataError:
            return None

    def _gcs_file_size_cached(self, uri_str: str, cache: dict[str, int]) -> int:
        """Get GCS file size with memoization."""
        if uri_str not in cache:
            cache[uri_str] = GCSURI(uri_str).size
        return cache[uri_str]

    def _collect_input_file_sizes(
        self,
        inputs: dict[str, Any],
        *,
        cache: dict[str, int],
    ) -> dict[str, list[int]]:
        """Collect file sizes for all GCS input files."""
        out: dict[str, list[int]] = defaultdict(list)

        for input_name, input_value in sorted(inputs.items()):

            def visit(v: Any, *, _name: str = input_name) -> None:
                if isinstance(v, str) and GCSURI(v).is_valid:
                    out[_name].append(self._gcs_file_size_cached(v, cache))

            recurse_dict_value(input_value, visit)

        return dict(out)

    def _compute_stats(
        self,
        df: pd.DataFrame,
        *,
        excluded_cols: set[int],
        stat_methods: tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        """Compute statistics for each column in the monitoring DataFrame."""
        stats: dict[str, dict[str, Any]] = {m: {} for m in stat_methods}

        for i, col_name in enumerate(df.columns):
            if i in excluded_cols:
                continue

            series = df[col_name]
            for method in stat_methods:
                if df.empty:
                    val = None
                elif method == 'last':
                    val = series.iloc[-1]
                else:
                    val = getattr(series, method)()
                stats[method][col_name] = self._py_scalar(val) if val is not None else None

        return stats

    def _gcp_monitor_one_call(
        self,
        call_name: str,
        call: dict[str, Any],
        *,
        workflow_id: str | None,
        excluded_cols: set[int],
        stat_methods: tuple[str, ...],
        file_size_cache: dict[str, int],
    ) -> dict[str, Any] | None:
        """Process a single call's monitoring log. Returns None if not applicable."""
        monitoring_log = call.get('monitoringLog')
        if not monitoring_log:
            return None

        log_uri = GCSURI(monitoring_log)
        if not log_uri.is_valid or not log_uri.exists:
            return None

        df = self._read_tsv_dataframe(log_uri.read())
        if df is None:
            return None

        rt_attrs = call.get('runtimeAttributes') or {}

        return {
            'workflow_id': workflow_id,
            'task_name': call_name,
            'shard_idx': call.get('shardIndex'),
            'status': call.get('executionStatus'),
            'attempt': call.get('attempt'),
            'instance': {
                'cpu': int(rt_attrs.get('cpu')) if rt_attrs.get('cpu') is not None else None,
                'disk': parse_cromwell_disks(rt_attrs.get('disks')),
                'mem': parse_cromwell_memory(rt_attrs.get('memory')),
            },
            'stats': self._compute_stats(
                df,
                excluded_cols=excluded_cols,
                stat_methods=stat_methods,
            ),
            'input_file_sizes': self._collect_input_file_sizes(
                call.get('inputs', {}),
                cache=file_size_cache,
            ),
        }

    def gcp_monitor(
        self,
        task_name: str | None = None,
        excluded_cols: Iterable[int] = (0,),
        stat_methods: Iterable[str] = DEFAULT_GCP_MONITOR_STAT_METHODS,
    ) -> list[dict[str, Any]]:
        """
        Recursively parse task(call)'s `monitoringLog`.

        (`monitoring.log` in task's execution directory)
        generated by `monitoring_script` defined in workflow options.
        This feature is gcp backend only.
        Check the following for details.
        https://cromwell.readthedocs.io/en/stable/wf_options/Google/

        This functions calculates mean/max/min/last of each column in `monitoring.log` and
        return them with task's input file sizes.

        Args:
            task_name:
                If defined, limit analysis to this task only.
            excluded_cols:
                List of 0-based indices of excluded columns. There will be no mean/max/min
                calculation for these excluded columns.
                col-0 (1st column) is excluded by default since it's usually a column
                for timestamps.
            stat_methods:
                List/tuple of stat method strings.
                Except for `last`, any method of pandas.DataFrame can be used for stat_methods.
                e.g. `mean`, `max`, `min`, ...
                `last` is to get the last element in data, which usually means the latest data.
                Some methods in pandas.DataFrame will return `nan` if the number of data row is
                too small (e.g. `std` requires more than one data row).

        Returns:
            List of mean/std/max/min/last of columns along with size of input files.
            Note that
                - None will be returned if there are no data in the file.
                    - This means that the log file exists but there is no data in it.
                - `shard_idx` is -1 for non-scattered tasks.
                - Dot notation (.) will be used for task_name of subworkflow's task.

            Result format:
            [
                {
                    'workflow_id': WORKFLOW_ID,
                    'task_name': TASK_NAME,
                    'status': TASK_STATUS,
                    'shard_idx': SHARD_INDEX,
                    'attempt': ATTEMPT_RETRIAL,
                    'mean': {
                        COL1_NAME: MEAN_OF_COL1,
                        COL2_NAME: MEAN_OF_COL2,
                        ...
                    },
                    'gcp_instance': {
                        'cpu': NUM_CPU,
                        'disk': DISK_SIZE_USED,
                        'mem': TOTAL_MEMORY_IN_BYTES,
                    },
                    'stats': {
                        'std': {
                            COL1_NAME: STD_OF_COL1,
                            COL2_NAME: STD_OF_COL2,
                            ...
                        },
                        'max': {
                            COL1_NAME: MAX_OF_COL1,
                            COL2_NAME: MAX_OF_COL2,
                            ...
                        },
                        'min': {
                            COL1_NAME: MIN_OF_COL1,
                            COL2_NAME: MIN_OF_COL2,
                            ...
                        },
                        'last': {
                            COL1_NAME: LAST_ENTRY_OF_COL1,
                            COL2_NAME: LAST_ENTRY_OF_COL2,
                            ...
                        },
                    },
                    'input_file_sizes': {
                        INPUT1: [
                            SIZE_OF_FILE1_IN_INPUT1,
                            SIZE_OF_FILE2_IN_INPUT1,
                            ...
                        ],
                        INPUT2: [
                            SIZE_OF_FILE1_IN_INPUT2,
                            ...
                        ],
                        ...
                    },
                },
                ...
            ]
        """
        file_size_cache: dict[str, int] = {}
        excluded_set = set(excluded_cols)
        stat_tuple = tuple(stat_methods)
        workflow_id = self.workflow_id

        results: list[dict[str, Any]] = []
        for ctx in self.iter_call_contexts():
            if task_name and task_name != ctx.call_name:
                continue
            data = self._gcp_monitor_one_call(
                ctx.call_name,
                ctx.call,
                workflow_id=workflow_id,
                excluded_cols=excluded_set,
                stat_methods=stat_tuple,
                file_size_cache=file_size_cache,
            )
            if data is not None:
                results.append(data)

        return results

    def cleanup(
        self,
        *,
        dry_run: bool = False,
        num_threads: int = URIBase.DEFAULT_NUM_THREADS,
        no_lock: bool = False,
    ) -> None:
        """
        Cleans up workflow's root output directory.

        Args:
            dry_run:
                Dry-run mode.
            num_threads:
                For outputs on cloud buckets only.
                Number of threads for deleting individual outputs on cloud buckets in parallel.
                Generates one client per thread. This works like `gsutil -m rm -rf`.
            no_lock:
                No file locking.
        """
        root = self.workflow_root
        if not root:
            logger.error(
                "workflow's root directory cannot be found in metadata JSON. "
                'Cannot proceed to cleanup outputs.'
            )
            return

        if AbsPath(root).is_valid:
            # num_threads is not available for AbsPath().rmdir()
            AbsPath(root).rmdir(dry_run=dry_run, no_lock=no_lock)
        else:
            AutoURI(root).rmdir(dry_run=dry_run, no_lock=no_lock, num_threads=num_threads)
