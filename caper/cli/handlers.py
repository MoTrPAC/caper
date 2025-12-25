"""Handler functions for Caper CLI commands."""

from __future__ import annotations

import copy
import csv
import json
import logging
import os
import sys
from typing import TYPE_CHECKING, Any

from autouri import AutoURI

from caper.caper_args import ResourceAnalysisReductionMethod
from caper.caper_client import CaperClient, CaperClientSubmit
from caper.caper_init import init_caper_conf
from caper.caper_labels import CaperLabels
from caper.caper_runner import CaperRunner
from caper.cli.arg_field import abspath as get_abspath
from caper.cromwell_metadata import CromwellMetadata
from caper.dict_tool import flatten_dict
from caper.hpc import LsfWrapper, PbsWrapper, SgeWrapper, SlurmWrapper
from caper.resource_analysis import LinearResourceAnalysis
from caper.server_heartbeat import ServerHeartbeat

if TYPE_CHECKING:
    from .args import (
        AbortArgs,
        CleanupArgs,
        GcpMonitorArgs,
        GcpResAnalysisArgs,
        HpcAbortArgs,
        HpcListArgs,
        HpcSubmitArgs,
        InitArgs,
        ListArgs,
        MetadataArgs,
        RunArgs,
        ServerArgs,
        SubmitArgs,
        TroubleshootArgs,
        UnholdArgs,
    )

logger = logging.getLogger(__name__)

DEFAULT_DB_FILE_PREFIX = 'caper-db'
PRINT_ROW_DELIMITER = '\t'
USER_INTERRUPT_WARNING = (
    '\n\n'
    '*** DO NOT CTRL+C MULTIPLE TIMES! ***\n'
    '*** OR CAPER WILL NOT BE ABLE TO STOP CROMWELL AND RUNNING WORKFLOWS/TASKS ***'
    '\n\n'
)


def check_local_file_and_rename_if_exists(path: str, index: int = 0) -> str:
    """Check if file exists and rename with index if needed."""
    org_path = path
    if index:
        path = '.'.join([path, str(index)])
    if os.path.exists(path):
        return check_local_file_and_rename_if_exists(org_path, index + 1)
    return path


def handle_init(args: InitArgs) -> None:
    """Handle 'caper init' command."""
    init_caper_conf(args.conf, args.platform)


def handle_run(args: RunArgs) -> None:
    """Handle 'caper run' command."""
    c = CaperRunner(
        local_loc_dir=args.local_loc_dir,
        local_out_dir=args.local_out_dir,
        default_backend=args.backend,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        gcp_compute_service_account=args.gcp_compute_service_account,
        cromwell=args.cromwell,
        womtool=args.womtool,
        disable_call_caching=args.disable_call_caching,
        max_concurrent_workflows=args.max_concurrent_workflows,
        memory_retry_error_keys=args.memory_retry_error_keys_list,
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
        gcp_zones=args.gcp_zones_list,
        gcp_call_caching_dup_strat=args.gcp_call_caching_dup_strat,
        gcp_out_dir=args.gcp_out_dir,
        aws_batch_arn=args.aws_batch_arn,
        aws_region=args.aws_region,
        aws_out_dir=args.aws_out_dir,
        aws_call_caching_dup_strat=args.aws_call_caching_dup_strat,
        slurm_partition=args.slurm_partition,
        slurm_account=args.slurm_account,
        slurm_resource_param=args.slurm_resource_param,
        slurm_extra_param=args.slurm_extra_param,
        sge_pe=args.sge_pe,
        sge_queue=args.sge_queue,
        sge_extra_param=args.sge_extra_param,
        pbs_queue=args.pbs_queue,
        pbs_extra_param=args.pbs_extra_param,
        lsf_queue=args.lsf_queue,
        lsf_extra_param=args.lsf_extra_param,
    )

    cromwell_stdout = check_local_file_and_rename_if_exists(args.cromwell_stdout)
    logger.info('Cromwell stdout: %s', cromwell_stdout)

    with open(cromwell_stdout, 'w') as f:
        try:
            thread = c.run(
                backend=args.backend,
                wdl=args.wdl,
                inputs=args.inputs,
                options=args.options,
                labels=args.labels,
                imports=args.imports,
                metadata_output=args.metadata_output,
                str_label=args.str_label,
                docker=args.docker,
                singularity=args.singularity,
                conda=args.conda,
                custom_backend_conf=args.backend_file,
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


def handle_server(args: ServerArgs, nonblocking: bool = False) -> None:
    """Handle 'caper server' command."""
    c = CaperRunner(
        local_loc_dir=args.local_loc_dir,
        local_out_dir=args.local_out_dir,
        default_backend=args.backend,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        gcp_compute_service_account=args.gcp_compute_service_account,
        cromwell=args.cromwell,
        disable_call_caching=args.disable_call_caching,
        max_concurrent_workflows=args.max_concurrent_workflows,
        memory_retry_error_keys=args.memory_retry_error_keys_list,
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
        gcp_zones=args.gcp_zones_list,
        gcp_call_caching_dup_strat=args.gcp_call_caching_dup_strat,
        gcp_out_dir=args.gcp_out_dir,
        aws_batch_arn=args.aws_batch_arn,
        aws_region=args.aws_region,
        aws_out_dir=args.aws_out_dir,
        aws_call_caching_dup_strat=args.aws_call_caching_dup_strat,
        slurm_partition=args.slurm_partition,
        slurm_account=args.slurm_account,
        slurm_resource_param=args.slurm_resource_param,
        slurm_extra_param=args.slurm_extra_param,
        sge_pe=args.sge_pe,
        sge_queue=args.sge_queue,
        sge_extra_param=args.sge_extra_param,
        pbs_queue=args.pbs_queue,
        pbs_extra_param=args.pbs_extra_param,
        lsf_queue=args.lsf_queue,
        lsf_extra_param=args.lsf_extra_param,
    )

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
        'custom_backend_conf': args.backend_file,
        'embed_subworkflow': True,
        'auto_write_metadata': not args.disable_auto_write_metadata,
        'java_heap_server': args.java_heap_server,
        'dry_run': args.dry_run,
    }

    if nonblocking:
        c.server(fileobj_stdout=sys.stdout, **args_from_cli)
        return

    cromwell_stdout = check_local_file_and_rename_if_exists(args.cromwell_stdout)
    logger.info('Cromwell stdout: %s', cromwell_stdout)

    with open(cromwell_stdout, 'w') as f:
        try:
            thread = c.server(fileobj_stdout=f, **args_from_cli)
            if thread:
                thread.join()
                thread.stop(wait=True)
                if thread.returncode:
                    logger.error('Check stdout in %s', cromwell_stdout)

        except KeyboardInterrupt:
            logger.exception(USER_INTERRUPT_WARNING)


def handle_submit(args: SubmitArgs) -> None:
    """Handle 'caper submit' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClientSubmit(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        gcp_compute_service_account=args.gcp_compute_service_account,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
        womtool=args.womtool,
        gcp_zones=args.gcp_zones_list,
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
    c.submit(
        wdl=args.wdl,
        backend=args.backend,
        inputs=args.inputs,
        options=args.options,
        labels=args.labels,
        imports=args.imports,
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


def handle_abort(args: AbortArgs) -> None:
    """Handle 'caper abort' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    c.abort(args.wf_id_or_label)


def handle_unhold(args: UnholdArgs) -> None:
    """Handle 'caper unhold' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    c.unhold(args.wf_id_or_label)


def handle_list(args: ListArgs) -> None:  # noqa: C901, PLR0912
    """Handle 'caper list' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    workflows = c.list(args.wf_id_or_label, exclude_subworkflow=not args.show_subworkflow)

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

            if (
                args.hide_result_before is not None
                and w.get('submission')
                and w.get('submission') <= args.hide_result_before
            ):
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


def handle_metadata(args: MetadataArgs) -> None:
    """Handle 'caper metadata' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    m = c.metadata(wf_ids_or_labels=args.wf_id_or_label, embed_subworkflow=True)
    if not m:
        msg = 'Found no workflow matching with search query.'
        raise ValueError(msg)
    if len(m) > 1:
        msg = 'Found multiple workflow matching with search query.'
        raise ValueError(msg)

    print(json.dumps(m[0], indent=4))  # noqa: T201


def get_single_cromwell_metadata_obj(
    caper_client: CaperClient, wf_id_or_label: list[str], subcmd: str
) -> CromwellMetadata:
    """Get a single Cromwell metadata object from file or server."""
    if not wf_id_or_label:
        msg = (
            'Define at least one metadata JSON file or '
            'a search query for workflow ID/string label '
            'if there is a running Caper server.'
        )
        raise ValueError(msg)
    if len(wf_id_or_label) > 1:
        msg = (
            f'Multiple files/queries are not allowed for {subcmd}. '
            'Define one metadata JSON file or a search query '
            'for workflow ID/string label.'
        )
        raise ValueError(msg)

    metadata_file = AutoURI(get_abspath(wf_id_or_label[0]))

    if metadata_file.exists:
        metadata = json.loads(metadata_file.read())
    else:
        metadata_objs = (
            caper_client.metadata(wf_ids_or_labels=wf_id_or_label, embed_subworkflow=True) or []
        )
        if len(metadata_objs) > 1:
            msg = 'Found multiple workflows matching with search query.'
            raise ValueError(msg)
        if len(metadata_objs) == 0:
            msg = 'Found no workflow matching with search query.'
            raise ValueError(msg)
        metadata = metadata_objs[0]

    return CromwellMetadata(metadata)


def split_list_into_file_and_non_file(
    lst: list[str],
) -> tuple[list[str], list[str]]:
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
    caper_client: CaperClient, wf_id_or_label: list[str]
) -> list[CromwellMetadata]:
    """Get multiple Cromwell metadata objects from files or server."""
    if not wf_id_or_label:
        msg = (
            'Define at least one metadata JSON file or a search query for workflow ID/string '
            'label if there is a running Caper server.'
        )
        raise ValueError(msg)

    files, non_files = split_list_into_file_and_non_file(wf_id_or_label)

    all_metadata = []
    for file in files:
        metadata = json.loads(AutoURI(get_abspath(file)).read())
        all_metadata.append(metadata)

    if non_files:
        all_metadata.extend(
            caper_client.metadata(wf_ids_or_labels=non_files, embed_subworkflow=True) or []
        )

    if not all_metadata:
        msg = 'Found no metadata/workflow matching with search query.'
        raise ValueError(msg)
    return [CromwellMetadata(m) for m in all_metadata]


def handle_troubleshoot(args: TroubleshootArgs) -> None:
    """Handle 'caper troubleshoot' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    cm = get_single_cromwell_metadata_obj(c, args.wf_id_or_label, 'troubleshoot/debug')
    sys.stdout.write(
        cm.troubleshoot(
            show_completed_task=args.show_completed_task, show_stdout=args.show_stdout
        )
    )


def handle_gcp_monitor(args: GcpMonitorArgs) -> None:
    """Handle 'caper gcp_monitor' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    all_metadata = get_multi_cromwell_metadata_objs(c, args.wf_id_or_label)
    writer = csv.writer(sys.stdout, delimiter=PRINT_ROW_DELIMITER)

    result = []
    for metadata in all_metadata:
        result.extend(metadata.gcp_monitor())

    if args.json_format:
        print(json.dumps(result, indent=4))  # noqa: T201
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


def handle_gcp_res_analysis(args: GcpResAnalysisArgs) -> None:
    """Handle 'caper gcp_res_analysis' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    all_metadata = get_multi_cromwell_metadata_objs(c, args.wf_id_or_label)

    res_analysis = LinearResourceAnalysis()
    res_analysis.collect_resource_data(all_metadata)

    reduce_method = getattr(ResourceAnalysisReductionMethod, args.reduce_in_file_vars).value
    result = res_analysis.analyze(
        in_file_vars=read_json(args.in_file_vars_def_json),
        reduce_in_file_vars=reduce_method,
        target_resources=args.target_resources,
        plot_pdf=args.plot_pdf,
    )
    print(json.dumps(result, indent=4))  # noqa: T201


def handle_cleanup(args: CleanupArgs) -> None:
    """Handle 'caper cleanup' command."""
    sh = None
    if not args.no_server_heartbeat:
        sh = ServerHeartbeat(
            heartbeat_file=args.server_heartbeat_file,
            heartbeat_timeout=args.server_heartbeat_timeout,
        )

    c = CaperClient(
        local_loc_dir=args.local_loc_dir,
        gcp_loc_dir=args.gcp_loc_dir,
        aws_loc_dir=args.aws_loc_dir,
        gcp_service_account_key_json=args.gcp_service_account_key_json,
        server_hostname=args.hostname,
        server_port=args.port,
        server_heartbeat=sh,
    )
    cm = get_single_cromwell_metadata_obj(c, args.wf_id_or_label, 'cleanup')
    cm.cleanup(dry_run=not args.delete, num_threads=args.num_threads, no_lock=True)
    if not args.delete:
        logger.warning(
            'Use --delete to DELETE ALL OUTPUTS of this workflow. '
            'This action is NOT REVERSIBLE. Use this at your own risk.'
        )


def handle_hpc_submit(args: HpcSubmitArgs) -> None:
    """Handle 'caper hpc submit' command."""
    caper_run_cmd = args.to_caper_run_command()

    if args.backend == 'slurm':
        wrapper = SlurmWrapper(
            args.slurm_leader_job_resource_param.split(),
            args.slurm_partition,
            args.slurm_account,
        )
    elif args.backend == 'sge':
        wrapper = SgeWrapper(
            args.sge_leader_job_resource_param.split(),
            args.sge_queue,
        )
    elif args.backend == 'pbs':
        wrapper = PbsWrapper(
            args.pbs_leader_job_resource_param.split(),
            args.pbs_queue,
        )
    elif args.backend == 'lsf':
        wrapper = LsfWrapper(
            args.lsf_leader_job_resource_param.split(),
            args.lsf_queue,
        )
    else:
        msg = f'Unsupported backend: {args.backend}'
        raise ValueError(msg)

    stdout = wrapper.submit(args.leader_job_name, caper_run_cmd)
    print(stdout)  # noqa: T201


def handle_hpc_list(args: HpcListArgs) -> None:
    """Handle 'caper hpc list' command."""
    if args.backend == 'slurm':
        wrapper = SlurmWrapper()
    elif args.backend == 'sge':
        wrapper = SgeWrapper()
    elif args.backend == 'pbs':
        wrapper = PbsWrapper()
    elif args.backend == 'lsf':
        wrapper = LsfWrapper()
    else:
        msg = f'Unsupported backend: {args.backend}'
        raise ValueError(msg)

    stdout = wrapper.list()
    print(stdout)  # noqa: T201


def handle_hpc_abort(args: HpcAbortArgs) -> None:
    """Handle 'caper hpc abort' command."""
    if args.backend == 'slurm':
        wrapper = SlurmWrapper()
    elif args.backend == 'sge':
        wrapper = SgeWrapper()
    elif args.backend == 'pbs':
        wrapper = PbsWrapper()
    elif args.backend == 'lsf':
        wrapper = LsfWrapper()
    else:
        msg = f'Unsupported backend: {args.backend}'
        raise ValueError(msg)

    stdout = wrapper.abort(args.job_ids)
    print(stdout)  # noqa: T201
