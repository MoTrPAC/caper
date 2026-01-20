#!/usr/bin/env python3
"""Caper command line interface module."""

from __future__ import annotations

import copy
import csv
import json
import logging
import os
import re
import sys
from typing import TYPE_CHECKING, Any, overload

from autouri import GCSURI, AutoURI

from . import __version__ as version
from .caper_args import ResourceAnalysisReductionMethod, get_parser_and_defaults
from .caper_client import CaperClient, CaperClientSubmit
from .caper_init import init_caper_conf
from .caper_labels import CaperLabels
from .caper_runner import CaperRunner
from .cli_hpc import subcmd_hpc
from .cromwell_backend import CromwellBackendDatabase
from .cromwell_metadata import CromwellMetadata
from .dict_tool import flatten_dict
from .resource_analysis import LinearResourceAnalysis
from .server_heartbeat import ServerHeartbeat

if TYPE_CHECKING:
    from argparse import ArgumentParser, Namespace

    from .nb_subproc_thread import NBSubprocThread


logger = logging.getLogger(__name__)


DEFAULT_DB_FILE_PREFIX = 'caper-db'
DEFAULT_SERVER_HEARTBEAT_FILE = '~/.caper/default_server_heartbeat'
USER_INTERRUPT_WARNING = (
    '\n\n'
    '*** DO NOT CTRL+C MULTIPLE TIMES! ***\n'
    '*** OR CAPER WILL NOT BE ABLE TO STOP CROMWELL AND RUNNING WORKFLOWS/TASKS ***'
    '\n\n'
)
REGEX_DELIMITER_PARAMS = r',| '
PRINT_ROW_DELIMITER = '\t'


def get_abspath(path: str) -> str:
    """
    Get abspath from a string.

    This function is mainly used to make a command line argument an abspath
    since AutoURI module only works with abspath and full URIs
    (e.g. /home/there, gs://here/there).
    For example, "caper run toy.wdl --docker ubuntu:latest".
    AutoURI cannot recognize toy.wdl on CWD as a file path.
    It should be converted to an abspath first.
    To do so, use this function for local file path strings only (e.g. toy.wdl).
    Do not use this function for other non-local-path strings (e.g. --docker).
    """
    if path and not AutoURI(path).is_valid:
        return os.path.abspath(os.path.expanduser(path))
    return path


def check_local_file_and_rename_if_exists(path: str, index: int = 0) -> str:
    """Return a unique path by appending index if file exists."""
    org_path = path
    if index:
        path = '.'.join([path, str(index)])
    if os.path.exists(path):
        return check_local_file_and_rename_if_exists(org_path, index + 1)
    return path


def print_version(parser: ArgumentParser, args: Namespace) -> None:
    """Print version and exit if --version flag is set."""
    if args.version:
        print(version)  # noqa: T201
        parser.exit()


def init_logging(args: Namespace) -> None:
    """Initialize logging configuration."""
    log_level = 'DEBUG' if args.debug else 'INFO'
    logging.basicConfig(level=log_level, format='%(asctime)s|%(name)s|%(levelname)s| %(message)s')
    # suppress filelock logging
    logging.getLogger('filelock').setLevel('CRITICAL')


def init_autouri(args: Namespace) -> None:
    """Initialize AutoURI settings from args."""
    if hasattr(args, 'use_gsutil_for_s3'):
        GCSURI.init_gcsuri(use_gsutil_for_s3=args.use_gsutil_for_s3)


def check_flags(args: Namespace) -> None:
    """Validate container and environment flags."""
    singularity_flag = False
    docker_flag = False
    conda_flag = False

    if hasattr(args, 'singularity') and args.singularity is not None:
        singularity_flag = True
        if args.singularity.endswith(('.wdl', '.cwl')):
            msg = (
                '--singularity ate up positional arguments (e.g. WDL, CWL). '
                'Define --singularity at the end of command line arguments. '
                f'singularity={args.singularity}'
            )
            raise ValueError(msg)

    if hasattr(args, 'docker') and args.docker is not None:
        docker_flag = True
        if args.docker.endswith(('.wdl', '.cwl')):
            msg = (
                '--docker ate up positional arguments (e.g. WDL, CWL). '
                'Define --docker at the end of command line arguments. '
                f'docker={args.docker}'
            )
            raise ValueError(msg)
        if hasattr(args, 'soft_glob_output') and args.soft_glob_output:
            msg = (
                '--soft-glob-output and --docker are mutually exclusive. '
                'Delocalization from docker container will fail '
                'for soft-linked globbed outputs.'
            )
            raise ValueError(msg)

    if hasattr(args, 'conda') and args.conda is not None:
        conda_flag = True
        if args.conda.endswith(('.wdl', '.cwl')):
            msg = (
                '--conda ate up positional arguments (e.g. WDL, CWL). '
                'Define --conda at the end of command line arguments. '
                f'conda={args.conda}'
            )
            raise ValueError(msg)

    all_flags = (docker_flag, singularity_flag, conda_flag)
    if len([flag for flag in all_flags if flag]) > 1:
        msg = '--docker, --singularity and --conda are mutually exclusive.'
        raise ValueError(msg)


def check_dirs(args: Namespace) -> None:
    """
    Convert local directories (local_out_dir, local_loc_dir) to absolute ones.

    Also, if temporary/cache directory is not defined for each storage,
    then append ".caper_tmp" on output directory and use it.
    """
    if hasattr(args, 'local_out_dir'):
        args.local_out_dir = get_abspath(args.local_out_dir)
        if not args.local_loc_dir:
            args.local_loc_dir = os.path.join(args.local_out_dir, CaperRunner.DEFAULT_LOC_DIR_NAME)
    elif not args.local_loc_dir:
        args.local_loc_dir = os.path.join(os.getcwd(), CaperRunner.DEFAULT_LOC_DIR_NAME)

    args.local_loc_dir = get_abspath(args.local_loc_dir)

    if hasattr(args, 'gcp_out_dir') and args.gcp_out_dir and not args.gcp_loc_dir:
        args.gcp_loc_dir = os.path.join(args.gcp_out_dir, CaperRunner.DEFAULT_LOC_DIR_NAME)

    if hasattr(args, 'aws_out_dir') and args.aws_out_dir and not args.aws_loc_dir:
        args.aws_loc_dir = os.path.join(args.aws_out_dir, CaperRunner.DEFAULT_LOC_DIR_NAME)


def check_db_path(args: Namespace) -> None:
    """Set up file database path from args."""
    if hasattr(args, 'db') and args.db == CromwellBackendDatabase.DB_FILE:
        args.file_db = get_abspath(args.file_db)

        if not args.file_db:
            db_filename_list = [DEFAULT_DB_FILE_PREFIX]
            if hasattr(args, 'wdl') and args.wdl:
                db_filename_list.append(os.path.basename(args.wdl))
            if hasattr(args, 'inputs') and args.inputs:
                db_filename_list.append(os.path.basename(args.inputs))
            db_filename = '_'.join(db_filename_list)
            args.file_db = os.path.join(args.local_out_dir, db_filename)


def runner(args: Namespace, nonblocking_server: bool = False) -> NBSubprocThread | None:
    """Execute runner subcommand (run or server)."""
    if args.gcp_zones:
        args.gcp_zones = re.split(REGEX_DELIMITER_PARAMS, args.gcp_zones)
    if args.memory_retry_error_keys:
        args.memory_retry_error_keys = re.split(
            REGEX_DELIMITER_PARAMS, args.memory_retry_error_keys
        )

    c = CaperRunner(
        local_loc_dir=args.local_loc_dir,
        local_out_dir=args.local_out_dir,
        default_backend=args.backend,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=get_abspath(args.gcp_service_account_key_json),
        gcp_compute_service_account=args.gcp_compute_service_account,
        cromwell=get_abspath(args.cromwell),
        womtool=get_abspath(getattr(args, 'womtool', None)),
        disable_call_caching=args.disable_call_caching,
        max_concurrent_workflows=args.max_concurrent_workflows,
        memory_retry_error_keys=args.memory_retry_error_keys,
        max_concurrent_tasks=args.max_concurrent_tasks,
        soft_glob_output=args.soft_glob_output,
        local_hash_strat=args.local_hash_strat,
        db=args.db,
        db_timeout=args.db_timeout,
        file_db=args.file_db,
        mysql_db_ip=args.mysql_db_ip,
        mysql_db_port=args.mysql_db_port,
        mysql_db_user=args.mysql_db_user,
        mysql_db_password=args.mysql_db_password,
        mysql_db_name=args.mysql_db_name,
        postgresql_db_ip=args.postgresql_db_ip,
        postgresql_db_port=args.postgresql_db_port,
        postgresql_db_user=args.postgresql_db_user,
        postgresql_db_password=args.postgresql_db_password,
        postgresql_db_name=args.postgresql_db_name,
        gcp_prj=args.gcp_prj,
        gcp_region=args.gcp_region,
        gcp_zones=args.gcp_zones,
        gcp_call_caching_dup_strat=args.gcp_call_caching_dup_strat,
        gcp_out_dir=args.gcp_out_dir,
        aws_batch_arn=args.aws_batch_arn,
        aws_region=args.aws_region,
        aws_out_dir=args.aws_out_dir,
        aws_call_caching_dup_strat=args.aws_call_caching_dup_strat,
        slurm_partition=getattr(args, 'slurm_partition', None),
        slurm_account=getattr(args, 'slurm_account', None),
        slurm_resource_param=getattr(args, 'slurm_resource_param', None),
        slurm_extra_param=getattr(args, 'slurm_extra_param', None),
        sge_pe=getattr(args, 'sge_pe', None),
        sge_queue=getattr(args, 'sge_queue', None),
        sge_resource_param=getattr(args, 'sge_resource_param', None),
        sge_extra_param=getattr(args, 'sge_extra_param', None),
        pbs_queue=getattr(args, 'pbs_queue', None),
        pbs_resource_param=getattr(args, 'pbs_resource_param', None),
        pbs_extra_param=getattr(args, 'pbs_extra_param', None),
        lsf_queue=getattr(args, 'lsf_queue', None),
        lsf_resource_param=getattr(args, 'lsf_resource_param', None),
        lsf_extra_param=getattr(args, 'lsf_extra_param', None),
    )

    if args.action == 'run':
        return subcmd_run(c, args)
    if args.action == 'server':
        return subcmd_server(c, args, nonblocking=nonblocking_server)
    msg = f'Unsupported runner action {args.action}'
    raise ValueError(msg)


def client(args: Namespace) -> None:
    """Execute client subcommand."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )
    if args.action == 'submit':
        if args.gcp_zones:
            args.gcp_zones = re.split(REGEX_DELIMITER_PARAMS, args.gcp_zones)

        c = CaperClientSubmit(
            local_loc_dir=args.local_loc_dir,
            gcp_loc_dir=args.gcp_loc_dir,
            aws_loc_dir=args.aws_loc_dir,
            gcp_service_account_key_json=get_abspath(args.gcp_service_account_key_json),
            gcp_compute_service_account=args.gcp_compute_service_account,
            server_hostname=args.hostname,
            server_port=args.port,
            server_heartbeat=sh,
            womtool=get_abspath(args.womtool),
            gcp_zones=args.gcp_zones,
            slurm_partition=args.slurm_partition,
            slurm_account=args.slurm_account,
            slurm_extra_param=args.slurm_extra_param,
            sge_pe=args.sge_pe,
            sge_queue=args.sge_queue,
            sge_extra_param=args.sge_extra_param,
            pbs_queue=args.pbs_queue,
            pbs_extra_param=args.pbs_extra_param,
            lsf_queue=args.lsf_queue,
            lsf_extra_param=args.lsf_extra_param,
        )
        subcmd_submit(c, args)

    else:
        c = CaperClient(
            local_loc_dir=args.local_loc_dir,
            gcp_loc_dir=args.gcp_loc_dir,
            aws_loc_dir=args.aws_loc_dir,
            gcp_service_account_key_json=get_abspath(args.gcp_service_account_key_json),
            server_hostname=args.hostname,
            server_port=args.port,
            server_heartbeat=sh,
        )
        if args.action == 'abort':
            subcmd_abort(c, args)
        elif args.action == 'unhold':
            subcmd_unhold(c, args)
        elif args.action == 'list':
            subcmd_list(c, args)
        elif args.action == 'metadata':
            subcmd_metadata(c, args)
        elif args.action in ('troubleshoot', 'debug'):
            subcmd_troubleshoot(c, args)
        elif args.action == 'gcp_monitor':
            subcmd_gcp_monitor(c, args)
        elif args.action == 'gcp_res_analysis':
            subcmd_gcp_res_analysis(c, args)
        elif args.action == 'cleanup':
            subcmd_cleanup(c, args)
        else:
            msg = f'Unsupported client action {args.action}'
            raise ValueError(msg)


@overload
def subcmd_server(
    caper_runner: CaperRunner, args: Namespace, nonblocking: bool = True
) -> NBSubprocThread: ...
@overload
def subcmd_server(
    caper_runner: CaperRunner, args: Namespace, nonblocking: bool = False
) -> None: ...
def subcmd_server(
    caper_runner: CaperRunner, args: Namespace, nonblocking: bool = False
) -> NBSubprocThread | None:
    """
    Run a Cromwell server.

    Args:
        caper_runner:
            CaperRunner instance for running workflows.
        args:
            Parsed command-line arguments.
        nonblocking:
            Make this function return a Thread object
            instead of blocking (Thread.join()).
            Also writes Cromwell's STDOUT to sys.stdout
            instead of a file (args.cromwell_stdout).
    """
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    args_from_cli = {
        'default_backend': args.backend,
        'server_port': args.port,
        'server_heartbeat': sh,
        'custom_backend_conf': get_abspath(args.backend_file),
        'embed_subworkflow': True,
        'auto_write_metadata': not args.disable_auto_write_metadata,
        'java_heap_server': args.java_heap_server,
        'dry_run': args.dry_run,
    }

    if nonblocking:
        return caper_runner.server(fileobj_stdout=sys.stdout, **args_from_cli)

    cromwell_stdout = check_local_file_and_rename_if_exists(get_abspath(args.cromwell_stdout))
    logger.info('Cromwell stdout: %s', cromwell_stdout)

    with open(cromwell_stdout, 'w') as f:
        try:
            thread = caper_runner.server(fileobj_stdout=f, **args_from_cli)
            if thread:
                thread.join()
                thread.stop(wait=True)
                if thread.returncode:
                    logger.error('Check stdout in %s', cromwell_stdout)

        except KeyboardInterrupt:
            logger.exception(USER_INTERRUPT_WARNING)


def subcmd_run(caper_runner: CaperRunner, args: Namespace) -> None:
    """Execute the run subcommand."""
    cromwell_stdout = check_local_file_and_rename_if_exists(get_abspath(args.cromwell_stdout))
    logger.info('Cromwell stdout: %s', cromwell_stdout)

    with open(cromwell_stdout, 'w') as f:
        try:
            thread = caper_runner.run(
                backend=args.backend,
                wdl=get_abspath(args.wdl),
                inputs=get_abspath(args.inputs),
                options=get_abspath(args.options),
                labels=get_abspath(args.labels),
                imports=get_abspath(args.imports),
                metadata_output=get_abspath(args.metadata_output),
                str_label=args.str_label,
                docker=args.docker,
                singularity=args.singularity,
                conda=args.conda,
                custom_backend_conf=get_abspath(args.backend_file),
                max_retries=args.max_retries,
                memory_retry_multiplier=args.memory_retry_multiplier,
                gcp_monitoring_script=args.gcp_monitoring_script,
                ignore_womtool=args.ignore_womtool,
                no_deepcopy=args.no_deepcopy,
                fileobj_stdout=f,
                fileobj_troubleshoot=sys.stdout,
                java_heap_run=args.java_heap_run,
                java_heap_womtool=args.java_heap_womtool,
                dry_run=args.dry_run,
            )
            if thread:
                thread.join()
                thread.stop(wait=True)
                if thread.returncode:
                    logger.error('Check stdout in %s', cromwell_stdout)

        except KeyboardInterrupt:
            logger.exception(USER_INTERRUPT_WARNING)


def subcmd_submit(caper_client: CaperClientSubmit, args: Namespace) -> None:
    """Execute the submit subcommand."""
    caper_client.submit(
        wdl=get_abspath(args.wdl),
        backend=args.backend,
        inputs=get_abspath(args.inputs),
        options=get_abspath(args.options),
        labels=get_abspath(args.labels),
        imports=get_abspath(args.imports),
        str_label=args.str_label,
        docker=args.docker,
        singularity=args.singularity,
        conda=args.conda,
        max_retries=args.max_retries,
        memory_retry_multiplier=args.memory_retry_multiplier,
        gcp_monitoring_script=args.gcp_monitoring_script,
        ignore_womtool=args.ignore_womtool,
        no_deepcopy=args.no_deepcopy,
        hold=args.hold,
        java_heap_womtool=args.java_heap_womtool,
        dry_run=args.dry_run,
    )


def subcmd_abort(caper_client: CaperClient, args: Namespace) -> None:
    """Execute the abort subcommand."""
    caper_client.abort(args.wf_id_or_label)


def subcmd_unhold(caper_client: CaperClient, args: Namespace) -> None:
    """Execute the unhold subcommand."""
    caper_client.unhold(args.wf_id_or_label)


def subcmd_list(caper_client: CaperClient, args: Namespace) -> None:
    """Execute the list subcommand."""
    workflows = caper_client.list(
        args.wf_id_or_label, exclude_subworkflow=not args.show_subworkflow
    )

    try:
        writer = csv.writer(sys.stdout, delimiter=PRINT_ROW_DELIMITER)

        formats = args.format.split(',')
        writer.writerow(formats)

        if workflows is None:
            return
        for w in workflows:
            row = []
            workflow_id = w.get('id')
            parent_workflow_id = w.get('parentWorkflowId')

            if args.hide_result_before is not None:
                if w.get('submission') and w.get('submission') <= args.hide_result_before:
                    continue
            for f in formats:
                if f == 'workflow_id':
                    row.append(str(workflow_id))
                elif f == 'str_label':
                    if 'labels' in w and CaperLabels.KEY_CAPER_STR_LABEL in w['labels']:
                        lbl = w['labels'][CaperLabels.KEY_CAPER_STR_LABEL]
                    else:
                        lbl = None
                    row.append(str(lbl))
                elif f == 'user':
                    if 'labels' in w and CaperLabels.KEY_CAPER_USER in w['labels']:
                        lbl = w['labels'][CaperLabels.KEY_CAPER_USER]
                    else:
                        lbl = None
                    row.append(str(lbl))
                elif f == 'parent':
                    row.append(str(parent_workflow_id))
                else:
                    row.append(str(w.get(f)))
            writer.writerow(row)

    except BrokenPipeError:
        logger.debug('Ignored BrokenPipeError.')


def subcmd_metadata(caper_client: CaperClient, args: Namespace) -> None:
    """Execute the metadata subcommand."""
    m = caper_client.metadata(wf_ids_or_labels=args.wf_id_or_label, embed_subworkflow=True)
    if not m:
        msg = 'Found no workflow matching with search query.'
        raise ValueError(msg)
    if len(m) > 1:
        msg = 'Found multiple workflow matching with search query.'
        raise ValueError(msg)


def get_single_cromwell_metadata_obj(
    caper_client: CaperClient, args: Namespace, subcmd: str
) -> CromwellMetadata:
    """Get a single CromwellMetadata object from file or server query."""
    if not args.wf_id_or_label:
        msg = (
            'Define at least one metadata JSON file or '
            'a search query for workflow ID/string label '
            'if there is a running Caper server.'
        )
        raise ValueError(msg)
    if len(args.wf_id_or_label) > 1:
        msg = (
            f'Multiple files/queries are not allowed for {subcmd}. '
            'Define one metadata JSON file or a search query '
            'for workflow ID/string label.'
        )
        raise ValueError(msg)

    metadata_file = AutoURI(get_abspath(args.wf_id_or_label[0]))

    if metadata_file.exists:
        metadata = json.loads(metadata_file.read())
    else:
        metadata_objs = caper_client.metadata(
            wf_ids_or_labels=args.wf_id_or_label, embed_subworkflow=True
        )
        if len(metadata_objs) > 1:
            msg = 'Found multiple workflows matching with search query.'
            raise ValueError(msg)
        if len(metadata_objs) == 0:
            msg = 'Found no workflow matching with search query.'
            raise ValueError(msg)
        metadata = metadata_objs[0]

    return CromwellMetadata(metadata)


def split_list_into_file_and_non_file(lst: list[str]) -> tuple[list[str], list[str]]:
    """Returns tuple of (list of existing files, list of non-file strings)."""
    files = []
    non_files = []

    for maybe_file in lst:
        if AutoURI(get_abspath(maybe_file)).exists:
            files.append(maybe_file)
        else:
            non_files.append(maybe_file)

    return files, non_files


def get_multi_cromwell_metadata_objs(
    caper_client: CaperClient, args: Namespace
) -> list[CromwellMetadata]:
    """Get multiple CromwellMetadata objects from files or server queries."""
    if not args.wf_id_or_label:
        msg = (
            'Define at least one metadata JSON file or '
            'a search query for workflow ID/string label '
            'if there is a running Caper server.'
        )
        raise ValueError(msg)

    files, non_files = split_list_into_file_and_non_file(args.wf_id_or_label)

    all_metadata = []
    for file in files:
        metadata = json.loads(AutoURI(get_abspath(file)).read())
        all_metadata.append(metadata)

    if non_files:
        all_metadata.extend(
            caper_client.metadata(wf_ids_or_labels=non_files, embed_subworkflow=True)
        )

    if not all_metadata:
        msg = 'Found no metadata/workflow matching with search query.'
        raise ValueError(msg)
    return [CromwellMetadata(m) for m in all_metadata]


def subcmd_troubleshoot(caper_client: CaperClient, args: Namespace) -> None:
    """Execute the troubleshoot subcommand."""
    cm = get_single_cromwell_metadata_obj(caper_client, args, 'troubleshoot/debug')
    sys.stdout.write(
        cm.troubleshoot(show_completed_task=args.show_completed_task, show_stdout=args.show_stdout)
    )


def subcmd_gcp_monitor(caper_client: CaperClient, args: Namespace) -> None:
    """
    Prints out monitoring result either in a TSV format or in a JSON one.

    TSV format:
        TSV will be a flattened JSON with dot notation.
        The last column in the header is `input_file_name_size_pairs`.
        As its name implies, contents for this last columns are dynamic in length.

    JSON format:
        See description at CromwellMetadata.gcp_monitor.__doc__.
        Use a JSON format instead to get more detailed information.
    """
    all_metadata = get_multi_cromwell_metadata_objs(caper_client, args)
    writer = csv.writer(sys.stdout, delimiter=PRINT_ROW_DELIMITER)

    result = []
    for metadata in all_metadata:
        result.extend(metadata.gcp_monitor())

    if args.json_format:
        pass
    else:
        # input_file_sizes is dynamic in length so exclude and then put it back
        first_data = copy.deepcopy(result[0])
        first_data.pop('input_file_sizes')
        header = list(flatten_dict(first_data, reducer='.').keys())
        header += ['input_file_var_size_pairs']
        writer.writerow(header)

        for task_data in result:
            input_file_sizes = task_data.pop('input_file_sizes')
            row = [str(v) for v in flatten_dict(task_data).values()]

            # append `input_file_sizes` data which couldn't be cleanly
            # flattened by flatten_dict()
            for key, file_sizes in input_file_sizes.items():
                for i, file_size in enumerate(file_sizes):
                    if len(file_sizes) == 1:
                        row.append(key)
                    else:
                        row.append(key + f'[{i}]')
                    row.append(str(file_size))

            writer.writerow(row)


def read_json(json_file: str | None) -> dict[str, Any] | None:
    """Read JSON file and return a dictionary."""
    if json_file:
        json_contents = AutoURI(get_abspath(json_file)).read()
        return json.loads(json_contents)
    return None


def subcmd_gcp_res_analysis(caper_client: CaperClient, args: Namespace) -> None:
    """
    Solves linear regression problem to optimize resources for a task based on input file sizes.

    Prints out found coeffs and intercept along with raw dataset (x, y).
        - x: input file sizes for a task
        - y: resources (max_mem, max_disk) taken for a task
    """
    all_metadata = get_multi_cromwell_metadata_objs(caper_client, args)

    res_analysis = LinearResourceAnalysis()
    res_analysis.collect_resource_data(all_metadata)

    result = res_analysis.analyze(
        in_file_vars=read_json(args.in_file_vars_def_json),
        reduce_in_file_vars=getattr(
            ResourceAnalysisReductionMethod, args.reduce_in_file_vars
        ).value,
        target_resources=args.target_resources,
        plot_pdf=get_abspath(args.plot_pdf),
    )
    print(json.dumps(result, indent=4))  # noqa: T201


def subcmd_cleanup(caper_client: CaperClient, args: Namespace) -> None:
    """Cleanup outputs of a workflow."""
    cm = get_single_cromwell_metadata_obj(caper_client, args, 'cleanup')
    cm.cleanup(dry_run=not args.delete, num_threads=args.num_threads, no_lock=True)
    if not args.delete:
        logger.warning(
            'Use --delete to DELETE ALL OUTPUTS of this workflow. '
            'This action is NOT REVERSIBLE. Use this at your own risk.'
        )


@overload
def main(args: list[str] | None = None, nonblocking_server: bool = False) -> None: ...
@overload
def main(args: list[str] | None = None, nonblocking_server: bool = True) -> NBSubprocThread: ...
def main(args: list[str] | None = None, nonblocking_server: bool = False) -> NBSubprocThread | None:
    """
    Main function for the Caper command line interface.

    Args:
        args:
            List of command line arguments.
            If defined use it instead of sys.argv.
        nonblocking_server:
            "server" subcommand will return a Thread object
            instead of waiting (Thread.join()).
    """
    parser, _ = get_parser_and_defaults()

    if args is None and len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()

    known_args, _ = parser.parse_known_args(args)
    check_flags(known_args)
    print_version(parser, known_args)

    parsed_args = parser.parse_args(args)
    init_logging(parsed_args)
    init_autouri(parsed_args)

    check_dirs(parsed_args)
    check_db_path(parsed_args)

    if parsed_args.action == 'init':
        return init_caper_conf(parsed_args.conf, parsed_args.platform)
    if parsed_args.action == 'hpc':
        return subcmd_hpc(parsed_args)
    if parsed_args.action in ('run', 'server'):
        return runner(parsed_args, nonblocking_server=nonblocking_server)
    return client(parsed_args)


if __name__ == '__main__':
    main()
