"""Runner for executing workflows with Cromwell."""

import logging
import os
from collections.abc import Callable
from typing import TextIO

from autouri import AbsPath, AutoURI

from .caper_backend_conf import CaperBackendConf
from .caper_base import CaperBase
from .caper_labels import CaperLabels
from .caper_workflow_opts import CaperWorkflowOpts
from .cromwell import Cromwell
from .cromwell_backend import (
    BackendProvider,
    CromwellBackendAws,
    CromwellBackendBase,
    CromwellBackendCommon,
    CromwellBackendDatabase,
    CromwellBackendGcp,
    CromwellBackendLocal,
    CromwellBackendLsf,
    CromwellBackendPbs,
    CromwellBackendSge,
    CromwellBackendSlurm,
)
from .cromwell_metadata import CromwellMetadata
from .cromwell_rest_api import CromwellRestAPI
from .nb_subproc_thread import NBSubprocThread
from .server_heartbeat import ServerHeartbeat
from .wdl_parser import WDLParser

logger = logging.getLogger(__name__)


class WomtoolValidationFailedException(Exception):  # noqa: N818
    """Exception raised when WDL validation fails."""


class CaperRunner(CaperBase):
    """Runner for executing WDL workflows with Cromwell."""

    ENV_GOOGLE_CLOUD_PROJECT = 'GOOGLE_CLOUD_PROJECT'
    DEFAULT_FILE_DB_PREFIX = 'default_caper_file_db'
    SERVER_TMP_DIR_PREFIX = '.caper_server'

    def __init__(
        self,
        default_backend: BackendProvider,
        local_loc_dir: str | None = None,
        local_out_dir: str | None = None,
        gcp_loc_dir: str | None = None,
        aws_loc_dir: str | None = None,
        cromwell: str = Cromwell.DEFAULT_CROMWELL,
        womtool: str = Cromwell.DEFAULT_WOMTOOL,
        disable_call_caching: bool = False,
        max_concurrent_workflows: int = CromwellBackendCommon.DEFAULT_MAX_CONCURRENT_WORKFLOWS,
        memory_retry_error_keys: tuple[
            str, ...
        ] = CromwellBackendCommon.DEFAULT_MEMORY_RETRY_ERROR_KEYS,
        max_concurrent_tasks: int = CromwellBackendBase.DEFAULT_CONCURRENT_JOB_LIMIT,
        soft_glob_output: bool = False,
        local_hash_strat: str = CromwellBackendLocal.DEFAULT_LOCAL_HASH_STRAT,
        db: str = CromwellBackendDatabase.DEFAULT_DB,
        db_timeout: int = CromwellBackendDatabase.DEFAULT_DB_TIMEOUT_MS,
        mysql_db_ip: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_IP,
        mysql_db_port: int = CromwellBackendDatabase.DEFAULT_MYSQL_DB_PORT,
        mysql_db_user: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_USER,
        mysql_db_password: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_PASSWORD,
        mysql_db_name: str = CromwellBackendDatabase.DEFAULT_MYSQL_DB_NAME,
        postgresql_db_ip: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_IP,
        postgresql_db_port: int = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_PORT,
        postgresql_db_user: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_USER,
        postgresql_db_password: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_PASSWORD,
        postgresql_db_name: str = CromwellBackendDatabase.DEFAULT_POSTGRESQL_DB_NAME,
        file_db: str | None = None,
        gcp_prj: str | None = None,
        gcp_out_dir: str | None = None,
        gcp_call_caching_dup_strat: str = CromwellBackendGcp.DEFAULT_CALL_CACHING_DUP_STRAT,
        gcp_service_account_key_json: str | None = None,
        gcp_compute_service_account: str | None = None,
        gcp_region: str = CromwellBackendGcp.DEFAULT_REGION,
        aws_batch_arn: str | None = None,
        aws_region: str | None = None,
        aws_out_dir: str | None = None,
        aws_call_caching_dup_strat: str = CromwellBackendAws.DEFAULT_CALL_CACHING_DUP_STRAT,
        gcp_zones: list[str] | None = None,
        slurm_partition: str | None = None,
        slurm_account: str | None = None,
        slurm_extra_param: str | None = None,
        slurm_resource_param: str = CromwellBackendSlurm.DEFAULT_SLURM_RESOURCE_PARAM,
        sge_pe: str | None = None,
        sge_queue: str | None = None,
        sge_extra_param: str | None = None,
        sge_resource_param: str = CromwellBackendSge.DEFAULT_SGE_RESOURCE_PARAM,
        pbs_queue: str | None = None,
        pbs_extra_param: str | None = None,
        pbs_resource_param: str = CromwellBackendPbs.DEFAULT_PBS_RESOURCE_PARAM,
        lsf_queue: str | None = None,
        lsf_extra_param: str | None = None,
        lsf_resource_param: str = CromwellBackendLsf.DEFAULT_LSF_RESOURCE_PARAM,
    ) -> None:
        """
        See docstring of base class for other arguments.

        Args:
            default_backend:
                Default backend.
            local_loc_dir:
                Local cache directory for localization.
            local_out_dir:
                Output directory for local backends.
            gcp_loc_dir:
                GCP cache directory (gs://) for localization.
            aws_loc_dir:
                AWS cache directory (s3://) for localization.
            cromwell:
                Cromwell JAR URI.
            womtool:
                Womtool JAR URI.
            disable_call_caching:
                Disable call-caching (re-using outputs from previous workflows/tasks).
            max_concurrent_workflows:
                Limit for concurrent number of workflows.
            memory_retry_error_keys:
                List of error messages to catch failures due to OOM.
            max_concurrent_tasks:
                Limit for concurrent number of tasks for each workflow.
            soft_glob_output:
                Glob with ln -s instead of hard-linking.
            local_hash_strat:
                Local file hashing strategy for call-caching.
            db:
                Metadata DB type.
            db_timeout:
                DB connection timeout in milliseconds.
            mysql_db_ip:
                MySQL DB hostname.
            mysql_db_port:
                MySQL DB port.
            mysql_db_user:
                MySQL DB username.
            mysql_db_password:
                MySQL DB password.
            mysql_db_name:
                MySQL DB name.
            postgresql_db_ip:
                PostgreSQL DB hostname.
            postgresql_db_port:
                PostgreSQL DB port.
            postgresql_db_user:
                PostgreSQL DB username.
            postgresql_db_password:
                PostgreSQL DB password.
            postgresql_db_name:
                PostgreSQL DB name.
            file_db:
                File DB path prefix for db == "file".
            gcp_prj:
                Google project name.
            gcp_out_dir:
                Output bucket path for GCP backend (gs://).
            gcp_call_caching_dup_strat:
                Call-caching duplication strategy for GCP backend.
            gcp_service_account_key_json:
                This will be added to environment variable
                GOOGLE_APPLICATION_CREDENTIALS
                If not match with existing key then error out.
            gcp_compute_service_account:
                Service account email to use for Google Cloud Batch compute instances.
                If not provided, the default Compute Engine service account will be used.
                Ensure that this service account has the `roles/batch.agentReporter` role, so that
                VM instances can report their status to Batch.
            gcp_region:
                Region for Google Cloud Batch API.
            aws_batch_arn:
                ARN for AWS Batch.
            aws_region:
                AWS region.
            aws_out_dir:
                Output bucket path for AWS backend (s3://).
            aws_call_caching_dup_strat:
                Call-caching duplication strategy for AWS backend.
            gcp_zones:
                For this and all below arguments,
                see details in CaperWorkflowOpts.__init__.
            slurm_partition:
                SLURM partition if required to sbatch jobs.
            slurm_account:
                SLURM account if required to sbatch jobs.
            slurm_extra_param:
                SLURM extra parameter to be appended to sbatch command line.
            slurm_resource_param:
                SLURM resource parameters to be passed to sbatch.
            sge_pe:
                SGE parallel environment (required to run with multiple cpus).
            sge_queue:
                SGE queue.
            sge_extra_param:
                SGE extra parameter to be appended to qsub command line.
            sge_resource_param:
                SGE resource parameters to be passed to qsub.
            pbs_queue:
                PBS queue.
            pbs_extra_param:
                PBS extra parameter to be appended to qsub command line.
            pbs_resource_param:
                PBS resource parameters to be passed to qsub.
            lsf_queue:
                LSF queue.
            lsf_extra_param:
                LSF extra parameter to be appended to bsub command line.
            lsf_resource_param:
                LSF resource parameters to be passed to bsub.
        """
        super().__init__(
            local_loc_dir=local_loc_dir,
            gcp_loc_dir=gcp_loc_dir,
            aws_loc_dir=aws_loc_dir,
            gcp_service_account_key_json=gcp_service_account_key_json,
        )
        self._set_env_gcp_prj(gcp_prj)

        self._cromwell = Cromwell(cromwell=cromwell, womtool=womtool)

        if local_out_dir is None:
            local_out_dir = os.getcwd()

        self._caper_backend_conf = CaperBackendConf(
            default_backend=default_backend,
            local_out_dir=local_out_dir,
            disable_call_caching=disable_call_caching,
            max_concurrent_workflows=max_concurrent_workflows,
            max_concurrent_tasks=max_concurrent_tasks,
            soft_glob_output=soft_glob_output,
            local_hash_strat=local_hash_strat,
            db=db,
            db_timeout=db_timeout,
            file_db=file_db,
            mysql_db_ip=mysql_db_ip,
            mysql_db_port=mysql_db_port,
            mysql_db_user=mysql_db_user,
            mysql_db_password=mysql_db_password,
            mysql_db_name=mysql_db_name,
            postgresql_db_ip=postgresql_db_ip,
            postgresql_db_port=postgresql_db_port,
            postgresql_db_user=postgresql_db_user,
            postgresql_db_password=postgresql_db_password,
            postgresql_db_name=postgresql_db_name,
            gcp_prj=gcp_prj,
            gcp_out_dir=gcp_out_dir,
            memory_retry_error_keys=memory_retry_error_keys,
            gcp_call_caching_dup_strat=gcp_call_caching_dup_strat,
            gcp_service_account_key_json=gcp_service_account_key_json,
            gcp_compute_service_account=gcp_compute_service_account,
            gcp_region=gcp_region,
            aws_batch_arn=aws_batch_arn,
            aws_region=aws_region,
            aws_out_dir=aws_out_dir,
            aws_call_caching_dup_strat=aws_call_caching_dup_strat,
            slurm_partition=slurm_partition,
            slurm_account=slurm_account,
            slurm_extra_param=slurm_extra_param,
            slurm_resource_param=slurm_resource_param,
            sge_pe=sge_pe,
            sge_queue=sge_queue,
            sge_extra_param=sge_extra_param,
            sge_resource_param=sge_resource_param,
            pbs_queue=pbs_queue,
            pbs_extra_param=pbs_extra_param,
            pbs_resource_param=pbs_resource_param,
            lsf_queue=lsf_queue,
            lsf_extra_param=lsf_extra_param,
            lsf_resource_param=lsf_resource_param,
        )

        self._caper_workflow_opts = CaperWorkflowOpts(
            gcp_zones=gcp_zones,
            slurm_partition=slurm_partition,
            slurm_account=slurm_account,
            slurm_extra_param=slurm_extra_param,
            sge_pe=sge_pe,
            sge_queue=sge_queue,
            sge_extra_param=sge_extra_param,
            pbs_queue=pbs_queue,
            pbs_extra_param=pbs_extra_param,
            lsf_queue=lsf_queue,
            lsf_extra_param=lsf_extra_param,
        )

        self._caper_labels = CaperLabels()

    def _set_env_gcp_prj(
        self, gcp_prj: str | None = None, env_name: str = ENV_GOOGLE_CLOUD_PROJECT
    ) -> None:
        """
        Initalizes environment for authentication (storage).

        Args:
            gcp_prj:
                Environment variable GOOGLE_CLOUD_PROJECT will be updated with
                this.
            env_name:
                Environment variable name to set for GCP project.
        """
        if gcp_prj:
            if env_name in os.environ:
                prj = os.environ[env_name]
                if prj != gcp_prj:
                    logger.warning('Env var %s does not match with gcp_prj %s.', env_name, gcp_prj)
            logger.debug('Adding %s to env var %s', gcp_prj, env_name)
            os.environ[env_name] = gcp_prj

    def run(
        self,
        backend: str,
        wdl: str,
        inputs: str | None = None,
        options: str | None = None,
        labels: str | None = None,
        imports: str | None = None,
        metadata_output: str | None = None,
        str_label: str | None = None,
        user: str | None = None,
        docker: str | None = None,
        singularity: str | None = None,
        conda: str | None = None,
        custom_backend_conf: str | None = None,
        max_retries: int | None = CaperWorkflowOpts.DEFAULT_MAX_RETRIES,
        memory_retry_multiplier: float | None = CaperWorkflowOpts.DEFAULT_MEMORY_RETRY_MULTIPLIER,
        gcp_monitoring_script: str | None = CaperWorkflowOpts.DEFAULT_GCP_MONITORING_SCRIPT,
        ignore_womtool: bool = False,
        no_deepcopy: bool = False,
        fileobj_stdout: TextIO | None = None,
        fileobj_troubleshoot: TextIO | None = None,
        work_dir: str | None = None,
        java_heap_run: str = Cromwell.DEFAULT_JAVA_HEAP_CROMWELL_RUN,
        java_heap_womtool: str = Cromwell.DEFAULT_JAVA_HEAP_WOMTOOL,
        dry_run: bool = False,
    ) -> NBSubprocThread | None:
        """
        Run a workflow using Cromwell run mode.

        Args:
            backend:
                Choose among Caper's built-in backends.
                (aws, gcp, Local, slurm, sge, pbs, lsf).
                Or use a backend defined in your custom backend config file
                (above "backend_conf" file).
            wdl:
                WDL file.
            inputs:
                Input JSON file. Cromwell's parameter -i.
            options:
                Workflow options JSON file. Cromwell's parameter -o.
            labels:
                Labels JSON file. Cromwell's parameter -l.
            imports:
                imports ZIP file. Cromwell's parameter -p.
            metadata_output:
                Output metadata file path. Metadata JSON file will be written to
                this path. Caper also automatiacally generates it on each workflow's
                root directory.  Cromwell's parameter -m.
            str_label:
                Caper's string label, which will be written
                to labels JSON object.
            user:
                Username. If not defined, find username from system.
            docker:
                Default Docker image to run a workflow on.
                This will be overriden by "docker" attr defined in
                WDL's task's "runtime {} section.

                If this is None:
                    Docker will not be used for this workflow.
                If this is an emtpy string (working like a flag):
                    Docker will be used. Caper will try to find docker image
                    in WDL (from a comment "#CAPER docker" or
                    from workflow.meta.default_docker).
            singularity:
                Default Singularity image to run a workflow on.
                This will be overriden by "singularity" attr defined in
                WDL's task's "runtime {} section.

                If this is None:
                    Singularity will not be used for this workflow.
                If this is an emtpy string:
                    Singularity will be used. Caper will try to find Singularity image
                    in WDL (from a comment "#CAPER singularity" or
                    from workflow.meta.default_singularlity).
            conda:
                Default Conda environment name to run a workflow in.
                This will be overriden by "conda" attr defined in
                WDL's task's "runtime {} section.

                If this is None:
                    Conda (conda run -n ENV_NAME script.sh) will not be used for this workflow.
                If this is an emtpy string:
                    Conda will be used. Caper will try to find Conda environment image
                    in WDL (from workflow.meta.default_conda).
            custom_backend_conf:
                Backend config file (HOCON) to override Caper's
                auto-generated backend config.
            max_retries:
                Number of retrial for a failed task in a workflow.
                This applies to every task in a workflow.
                0 means no retrial. "attemps" attribute in a task's metadata
                increments from 1 as it is retried. attempts==2 means first retrial.
            memory_retry_multiplier:
                Multiplier for the memory retry feature.
                See https://cromwell.readthedocs.io/en/develop/cromwell_features/RetryWithMoreMemory/
                for details.
            gcp_monitoring_script:
                GCP monitoring script for resource monitoring in workflow options.
            ignore_womtool:
                Disable Womtool validation for WDL/input JSON/imports.
            no_deepcopy:
                Disable recursive localization of files defined in input JSON.
                Input JSON file itself will still be localized.
            fileobj_stdout:
                File-like object to write Cromwell's STDOUT.
            fileobj_troubleshoot:
                File-like object to write auto-troubleshooting after workflow is done.
            work_dir:
                Local temporary directory to store all temporary files.
                Temporary files mean intermediate files used for running Cromwell.
                For example, backend config file, workflow options file.
                Localized (recursively) data files defined in input JSON
                will NOT be stored here.
                They will be localized on self._local_loc_dir instead.
                If this is not defined, then cache directory self._local_loc_dir will be used.
                However, Cromwell Java process itself will run on CWD instead of this directory.
            java_heap_run:
                Java heap (java -Xmx) for Cromwell server mode.
            java_heap_womtool:
                Java heap (java -Xmx) for Womtool.
            dry_run:
                Stop before running Java command line for Cromwell.

        Returns:
            metadata_file:
                URI of metadata JSON file.
        """
        if not AutoURI(wdl).exists:
            msg = f'WDL does not exists. {wdl}'
            raise FileNotFoundError(msg)

        if str_label is None and inputs:
            str_label = AutoURI(inputs).basename_wo_ext

        if work_dir is None:
            work_dir = self.create_timestamped_work_dir(prefix=AutoURI(wdl).basename_wo_ext)

        logger.info('Localizing files on work_dir. %s', work_dir)

        if inputs:
            maybe_remote_file = self.localize_on_backend_if_modified(
                inputs, backend=backend, recursive=not no_deepcopy, make_md5_file=True
            )
            inputs = AutoURI(maybe_remote_file).localize_on(work_dir)

        if imports:
            imports = AutoURI(imports).localize_on(work_dir)
        elif not AbsPath(wdl).exists:
            # auto-zip sub WDLs only if main WDL is remote
            imports = WDLParser(wdl).create_imports_file(work_dir)

        # localize WDL to be passed to Cromwell Java
        wdl = AutoURI(wdl).localize_on(work_dir)

        if metadata_output:
            if not AbsPath(metadata_output).is_valid:
                msg = f'metadata_output is not a valid local abspath. {metadata_output}'
                raise ValueError(msg)
        else:
            metadata_output = os.path.join(work_dir, CromwellMetadata.DEFAULT_METADATA_BASENAME)

        backend_conf = self._caper_backend_conf.create_file(
            directory=work_dir, backend=backend, custom_backend_conf=custom_backend_conf
        )

        options = self._caper_workflow_opts.create_file(
            directory=work_dir,
            wdl=wdl,
            inputs=inputs,
            custom_options=options,
            docker=docker,
            singularity=singularity,
            conda=conda,
            backend=backend,
            max_retries=max_retries,
            memory_retry_multiplier=memory_retry_multiplier,
            gcp_monitoring_script=gcp_monitoring_script,
        )

        labels = self._caper_labels.create_file(
            directory=work_dir,
            backend=backend,
            custom_labels=labels,
            str_label=str_label,
            user=user,
        )

        if not ignore_womtool:
            self._cromwell.validate(wdl=wdl, inputs=inputs, imports=imports)

        logger.info('launching run: wdl=%s, inputs=%s, backend_conf=%s', wdl, inputs, backend_conf)
        return self._cromwell.run(
            wdl=wdl,
            backend_conf=backend_conf,
            inputs=inputs,
            options=options,
            imports=imports,
            labels=labels,
            metadata=metadata_output,
            fileobj_stdout=fileobj_stdout,
            fileobj_troubleshoot=fileobj_troubleshoot,
            dry_run=dry_run,
        )

    def server(
        self,
        default_backend: BackendProvider,
        server_port: int = CromwellRestAPI.DEFAULT_PORT,
        server_hostname: str | None = None,
        server_heartbeat: ServerHeartbeat | None = None,
        custom_backend_conf: str | None = None,
        fileobj_stdout: TextIO | None = None,
        embed_subworkflow: bool = False,
        java_heap_server: str = Cromwell.DEFAULT_JAVA_HEAP_CROMWELL_SERVER,
        auto_write_metadata: bool = True,
        on_server_start: Callable[[], None] | None = None,
        on_status_change: Callable[..., None] | None = None,
        work_dir: str | None = None,
        dry_run: bool = False,
    ) -> NBSubprocThread | None:
        """Run a Cromwell server.

        Args:
            default_backend:
                Default backend. If backend is not specified for a submitted workflow
                then default backend will be used.
                Choose among Caper's built-in backends.
                (aws, gcp, Local, slurm, sge, pbs, lsf).
                Or use a backend defined in your custom backend config file
                (above "backend_conf" file).
            server_port:
                Server port to run Cromwell server.
                Make sure to use different port for multiple Cromwell servers on the same
                machine.
            server_hostname:
                Server hostname. If not defined then socket.gethostname() will be used.
                If server_heartbeat is given, then this hostname will be written to
                the server heartbeat file defined in server_heartbeat.
            server_heartbeat:
                Server heartbeat to write hostname/port of a server.
            custom_backend_conf:
                Backend config file (HOCON) to override Caper's auto-generated backend config.
            fileobj_stdout:
                File-like object to write Cromwell's STDOUT.
            embed_subworkflow:
                Caper stores/updates metadata.JSON file on
                each workflow's root directory whenever there is status change
                of workflow (or its tasks).
                This flag ensures that any subworkflow's metadata JSON will be
                embedded in main (this) workflow's metadata JSON.
                This is to mimic behavior of Cromwell run mode's -m parameter.
            java_heap_server:
                Java heap (java -Xmx) for Cromwell server mode.
            auto_write_metadata:
                Automatic retrieval/writing of metadata.json upon workflow/task's status change.
            work_dir:
                Local temporary directory to store all temporary files.
                Temporary files mean intermediate files used for running Cromwell.
                For example, auto-generated backend config file and workflow options file.
                If this is not defined, then cache directory self._local_loc_dir with a timestamp
                will be used.
                However, Cromwell Java process itself will run on CWD instead of this directory.
            dry_run:
                Stop before running Java command line for Cromwell.
        """
        if work_dir is None:
            work_dir = self.create_timestamped_work_dir(prefix=CaperRunner.SERVER_TMP_DIR_PREFIX)

        backend_conf = self._caper_backend_conf.create_file(
            directory=work_dir,
            backend=default_backend,
            custom_backend_conf=custom_backend_conf,
        )
        logger.info('launching server: backend_conf=%s', backend_conf)

        return self._cromwell.server(
            backend_conf=backend_conf,
            server_port=server_port,
            server_hostname=server_hostname,
            server_heartbeat=server_heartbeat,
            fileobj_stdout=fileobj_stdout,
            embed_subworkflow=embed_subworkflow,
            java_heap_cromwell_server=java_heap_server,
            auto_write_metadata=auto_write_metadata,
            on_server_start=on_server_start,
            on_status_change=on_status_change,
            dry_run=dry_run,
        )
