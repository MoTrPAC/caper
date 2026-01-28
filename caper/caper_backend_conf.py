"""Module for creating the Cromwell backend configuration file."""
from __future__ import annotations

import logging
import os
from copy import deepcopy
from typing import TYPE_CHECKING

from autouri import AutoURI

from caper.cromwell_backend import BackendProvider, CachingDuplicationStrategyArgs

from .cromwell_backend import (
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
from .dict_tool import merge_dict
from .hocon_string import HOCONString

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


class CaperBackendConf:
    """Class for creating the Cromwell backend configuration file."""

    BACKEND_CONF_INCLUDE = 'include required(classpath("application"))'
    BASENAME_BACKEND_CONF = 'backend.conf'

    def __init__(
        self,
        default_backend: BackendProvider,
        local_out_dir: str,
        disable_call_caching: bool = False,
        max_concurrent_workflows: int = CromwellBackendCommon.DEFAULT_MAX_CONCURRENT_WORKFLOWS,
        memory_retry_error_keys: Sequence[str]
        | None = CromwellBackendCommon.DEFAULT_MEMORY_RETRY_ERROR_KEYS,
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
        gcp_call_caching_dup_strat: CachingDuplicationStrategyArgs = CromwellBackendGcp.DEFAULT_CALL_CACHING_DUP_STRAT,
        gcp_service_account_key_json: str | None = None,
        gcp_compute_service_account: str | None = None,
        gcp_network: str | None = None,
        gcp_subnetwork: str | None = None,
        gcp_region: str = CromwellBackendGcp.DEFAULT_REGION,
        aws_batch_arn: str | None = None,
        aws_region: str | None = None,
        aws_out_dir: str | None = None,
        aws_call_caching_dup_strat: CachingDuplicationStrategyArgs = CromwellBackendAws.DEFAULT_CALL_CACHING_DUP_STRAT,
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
        Initializes the backend conf's stanzas.

        Args:
            default_backend:
                Default backend.
            local_out_dir:
                Output directory for all local backends.
                Define this even if you don't want to run on local backends
                since "Local" is a Cromwell's default backend and it needs this.
            disable_call_caching:
                Disable call-caching (re-using outputs from previous workflows/tasks)
            max_concurrent_workflows:
                Limit for concurrent number of workflows.
            memory_retry_error_keys:
                List of error messages to catch failures due to OOM (out of memory error).
                e.g. ['OutOfMemory', 'Killed']
                If an error occurs caught by these keys, then instance's memory will
                be increased for next retrial by memory_retry_error_multiplier defined
                in workflow options JSON.
            max_concurrent_tasks:
                Limit for concurrent number of tasks for each workflow.
            soft_glob_output:
                Local backends only (local, sge, pbs, slurm, lsf).
                Glob with ln -s instead of hard-linking (ln alone).
                Useful for file-system like beeGFS, which does not allow hard-linking.
            local_hash_strat:
                Local file hashing strategy for call-caching.
            db:
                Metadata DB type. Defauling to use in-memory DB if not defined.
                You may need to define other parameters according to this DB type.
            db_timeout:
                DB connection timeout. Cromwell tries to connect to DB within this timeout.
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
                PostgreSQL DB user.
            postgresql_db_password:
                PostgreSQL DB password.
            postgresql_db_name:
                PostgreSQL DB name.
            file_db:
                For db == "file". File DB path prefix.
                File DB does not allow multiple connections, which means that
                you cannot run multiple caper run/server with the same file DB.
            gcp_prj:
                Google project name.
            gcp_out_dir:
                Output bucket path for gcp backend. Must start with gs://.
            gcp_call_caching_dup_strat:
                Call-caching duplication strategy for GCP backend.
            gcp_service_account_key_json:
                GCP service account key JSON.
            gcp_compute_service_account:
                The email of the GCP service account email to use for the
                Batch compute instances. If not provided, the default Compute
                Engine service account will be used. Ensure that this service
                account has the `roles/batch.agentReporter` role, so that
                VM instances can report their status to Batch.
            gcp_network:
                VPC network name for GCP Batch backend. Required for VPCs in custom subnet mode.
            gcp_subnetwork:
                VPC subnetwork name for GCP Batch backend. Required for VPCs in custom subnet mode.
            gcp_region:
                Region for Google Cloud Batch API.
            aws_batch_arn:
                ARN for AWS Batch.
            aws_region:
                AWS region. Multple regions are not allowed.
            aws_out_dir:
                Output bucket path for aws backend. Must start with s3://.
            aws_call_caching_dup_strat:
                Call-caching duplication strategy for AWS backend.
            slurm_partition:
                SLURM partition if required to sbatch jobs.
            slurm_account:
                SLURM account if required to sbatch jobs.
            slurm_extra_param:
                SLURM extra parameter to be appended to sbatch command line.
            slurm_resource_param:
                For slurm backend only.
                Resource parameters to be passed to sbatch.
                You can use WDL syntax and Cromwell's built-in variables in ${} notation.
                e.g. cpu, time, memory_mb
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
        self._template = {}

        merge_dict(
            self._template,
            CromwellBackendCommon(
                default_backend=default_backend,
                disable_call_caching=disable_call_caching,
                max_concurrent_workflows=max_concurrent_workflows,
                memory_retry_error_keys=memory_retry_error_keys,
            ),
        )

        merge_dict(
            self._template,
            CromwellBackendDatabase(
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
            ),
        )

        # local backends
        merge_dict(
            self._template,
            CromwellBackendLocal(
                local_out_dir=local_out_dir,
                max_concurrent_tasks=max_concurrent_tasks,
                soft_glob_output=soft_glob_output,
                local_hash_strat=local_hash_strat,
            ),
        )

        merge_dict(
            self._template,
            CromwellBackendSlurm(
                local_out_dir=local_out_dir,
                max_concurrent_tasks=max_concurrent_tasks,
                soft_glob_output=soft_glob_output,
                local_hash_strat=local_hash_strat,
                slurm_partition=slurm_partition,
                slurm_account=slurm_account,
                slurm_extra_param=slurm_extra_param,
                slurm_resource_param=slurm_resource_param,
            ),
        )

        merge_dict(
            self._template,
            CromwellBackendSge(
                local_out_dir=local_out_dir,
                max_concurrent_tasks=max_concurrent_tasks,
                soft_glob_output=soft_glob_output,
                local_hash_strat=local_hash_strat,
                sge_pe=sge_pe,
                sge_queue=sge_queue,
                sge_extra_param=sge_extra_param,
                sge_resource_param=sge_resource_param,
            ),
        )

        merge_dict(
            self._template,
            CromwellBackendPbs(
                local_out_dir=local_out_dir,
                max_concurrent_tasks=max_concurrent_tasks,
                soft_glob_output=soft_glob_output,
                local_hash_strat=local_hash_strat,
                pbs_queue=pbs_queue,
                pbs_extra_param=pbs_extra_param,
                pbs_resource_param=pbs_resource_param,
            ),
        )

        merge_dict(
            self._template,
            CromwellBackendLsf(
                local_out_dir=local_out_dir,
                max_concurrent_tasks=max_concurrent_tasks,
                soft_glob_output=soft_glob_output,
                local_hash_strat=local_hash_strat,
                lsf_queue=lsf_queue,
                lsf_extra_param=lsf_extra_param,
                lsf_resource_param=lsf_resource_param,
            ),
        )

        # cloud backends
        if gcp_prj and gcp_out_dir:
            if gcp_service_account_key_json:
                gcp_service_account_key_json = os.path.expanduser(gcp_service_account_key_json)
                if not os.path.exists(gcp_service_account_key_json):
                    msg = (
                        f'gcp_service_account_key_json does not exist. '
                        f'f={gcp_service_account_key_json}'
                    )
                    raise FileNotFoundError(msg)

            merge_dict(
                self._template,
                CromwellBackendGcp(
                    max_concurrent_tasks=max_concurrent_tasks,
                    gcp_prj=gcp_prj,
                    gcp_out_dir=gcp_out_dir,
                    call_caching_dup_strat=gcp_call_caching_dup_strat,
                    gcp_service_account_key_json=gcp_service_account_key_json,
                    gcp_compute_service_account=gcp_compute_service_account,
                    gcp_network=gcp_network,
                    gcp_subnetwork=gcp_subnetwork,
                    gcp_region=gcp_region,
                ),
            )

        if aws_batch_arn and aws_region and aws_out_dir:
            merge_dict(
                self._template,
                CromwellBackendAws(
                    max_concurrent_tasks=max_concurrent_tasks,
                    aws_batch_arn=aws_batch_arn,
                    aws_region=aws_region,
                    aws_out_dir=aws_out_dir,
                    call_caching_dup_strat=aws_call_caching_dup_strat,
                ),
            )

        # keep these variables for a backend checking later
        self._sge_pe = sge_pe
        self._gcp_prj = gcp_prj
        self._gcp_out_dir = gcp_out_dir

        self._aws_batch_arn = aws_batch_arn
        self._aws_region = aws_region
        self._aws_out_dir = aws_out_dir

    def create_file(
        self,
        directory: str,
        backend: BackendProvider | None = None,
        custom_backend_conf: str | None = None,
        basename: str = BASENAME_BACKEND_CONF,
    ) -> str:
        """
        Create a HOCON string and create a backend.conf file.

        Args:
            directory:
                Directory to create a backend.conf file.
            backend:
                Backend to run a workflow on.
                Default backend will be use if not defined.
            custom_backend_conf:
                User's custom backend conf file to override on
                Caper's auto-generated backend conf.
            basename:
                Basename.
        """
        template = deepcopy(self._template)

        if backend == BackendProvider.SGE:
            if self._sge_pe is None:
                msg = (
                    'sge-pe (Sun GridEngine parallel environment) is required for backend sge.'
                )
                raise ValueError(msg)
        elif backend == BackendProvider.GCP:
            if self._gcp_prj is None:
                msg = 'gcp-prj (Google Cloud Platform project) is required for backend gcp.'
                raise ValueError(msg)
            if self._gcp_out_dir is None:
                msg = 'gcp-out-dir (gs:// output bucket path) is required for backend gcp.'
                raise ValueError(msg)
        elif backend == BackendProvider.AWS:
            if self._aws_batch_arn is None:
                msg = 'aws-batch-arn (ARN for AWS Batch) is required for backend aws.'
                raise ValueError(msg)
            if self._aws_region is None:
                msg = 'aws-region (AWS region) is required for backend aws.'
                raise ValueError(msg)
            if self._aws_out_dir is None:
                msg = 'aws-out-dir (s3:// output bucket path) is required for backend aws.'
                raise ValueError(msg)

        hocon_s = HOCONString.from_dict(template, include=CaperBackendConf.BACKEND_CONF_INCLUDE)

        if custom_backend_conf is not None:
            s = AutoURI(custom_backend_conf).read()
            hocon_s.merge(s, update=True)

        final_backend_conf_file = os.path.join(directory, basename)
        AutoURI(final_backend_conf_file).write(str(hocon_s) + '\n')
        return final_backend_conf_file
