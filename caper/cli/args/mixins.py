"""Mixin dataclasses for shared argument groups.

Each mixin corresponds to a logical group of CLI arguments, matching the
current parent parser pattern (_add_common_args, _add_backend_args, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass

from caper.cli.arg_field import abspath, arg


@dataclass
class CommonArgs:
    """Common arguments for all commands (parent_all)."""

    conf: str = arg(
        '-c',
        '--conf',
        help_text='Specify config file',
        default='~/.caper/default.conf',
    )
    debug: bool = arg(
        '-D',
        '--debug',
        help_text='Prints all logs >= DEBUG level',
        action='store_true',
    )
    gcp_service_account_key_json: str | None = arg(
        '--gcp-service-account-key-json',
        help_text='Secret key JSON file for Google Cloud Platform service account',
        normalize=abspath,
    )


@dataclass
class LocalizationArgs:
    """Cache/temp directory arguments (parent_all)."""

    local_loc_dir: str | None = arg(
        '--local-loc-dir',
        '--tmp-dir',
        help_text="Temporary directory to store Cromwell's intermediate backend files",
        normalize=abspath,
    )
    gcp_loc_dir: str | None = arg(
        '--gcp-loc-dir',
        '--tmp-gcs-bucket',
        help_text='Temporary directory to store cached files for gcp backend (e.g., gs://bucket/cache)',
    )
    aws_loc_dir: str | None = arg(
        '--aws-loc-dir',
        '--tmp-s3-bucket',
        help_text='Temporary directory to store cached files for aws backend (e.g., s3://bucket/cache)',
    )


@dataclass
class BackendArgs:
    """Backend selection arguments (parent_backend)."""

    backend: str | None = arg(
        '-b',
        '--backend',
        help_text='Backend to run a workflow',
    )
    dry_run: bool = arg(
        '--dry-run',
        help_text='Caper localizes remote files and validates WDL but does not run',
        action='store_true',
    )
    gcp_zones: str | None = arg(
        '--gcp-zones',
        help_text='Comma-separated GCP zones for Batch (e.g. us-west1-b,us-central1-b)',
    )


@dataclass
class ServerClientArgs:
    """Server/client connection arguments (parent_server_client)."""

    port: int = arg(
        '--port',
        help_text='Port for Caper server',
        default=8000,
    )
    no_server_heartbeat: bool = arg(
        '--no-server-heartbeat',
        help_text='Disable server heartbeat file',
        action='store_true',
    )
    server_heartbeat_file: str = arg(
        '--server-heartbeat-file',
        help_text='Heartbeat file for Caper clients to get IP and port of a server',
        default='~/.caper/default_server_heartbeat',
        normalize=abspath,
    )
    server_heartbeat_timeout: int = arg(
        '--server-heartbeat-timeout',
        help_text='Timeout for a heartbeat file in milliseconds',
        default=120000,
    )


@dataclass
class ClientArgs:
    """Client-only arguments (parent_client)."""

    hostname: str = arg(
        '--hostname',
        '--ip',
        help_text='Hostname (or IP address) of Caper server',
        default='localhost',
    )


@dataclass
class SearchArgs:
    """Workflow search arguments (parent_search_wf)."""

    wf_id_or_label: list[str] = arg(
        'wf_id_or_label',
        help_text='List of workflow IDs. Wildcards (* and ?) are allowed',
        nargs='*',  # EXPLICIT - allows zero or more
        default_factory=list,
    )


@dataclass
class SubmitIOArgs:
    """Submit/run I/O arguments (parent_submit)."""

    inputs: str | None = arg(
        '-i',
        '--inputs',
        help_text='Workflow inputs JSON file',
        normalize=abspath,
    )
    options: str | None = arg(
        '-o',
        '--options',
        help_text='Workflow options JSON file',
        normalize=abspath,
    )
    labels: str | None = arg(
        '-l',
        '--labels',
        help_text='Workflow labels JSON file',
        normalize=abspath,
    )
    imports: str | None = arg(
        '-p',
        '--imports',
        help_text='Zip file of imported sub-workflows',
        normalize=abspath,
    )
    str_label: str | None = arg(
        '-s',
        '--str-label',
        help_text="Caper's special label for a workflow",
    )
    hold: bool = arg(
        '--hold',
        help_text='Put a hold on a workflow when submitted to a Cromwell server',
        action='store_true',
    )
    use_gsutil_for_s3: bool = arg(
        '--use-gsutil-for-s3',
        help_text='Use gsutil CLI for direct transfer between S3 and GCS buckets',
        action='store_true',
    )
    no_deepcopy: bool = arg(
        '--no-deepcopy',
        help_text='Disable deepcopy for JSON/TSV/CSV files specified in input JSON',
        action='store_true',
    )
    docker: str | None = arg(
        '--docker',
        help_text='URI for Docker image (e.g. ubuntu:latest) or flag to use WDL-defined image',
        nargs='?',  # Optional value: --docker or --docker ubuntu:latest
    )
    singularity: str | None = arg(
        '--singularity',
        help_text='URI or path for Singularity image or flag to use WDL-defined image',
        nargs='?',
    )
    conda: str | None = arg(
        '--conda',
        help_text="Default Conda environment's name or flag to use WDL-defined environment",
        nargs='?',
    )
    max_retries: int = arg(
        '--max-retries',
        help_text='Number of retries for failing tasks',
        default=0,
    )
    memory_retry_multiplier: float = arg(
        '--memory-retry-multiplier',
        help_text='Memory multiplier for retries',
        default=1.0,
    )
    ignore_womtool: bool = arg(
        '--ignore-womtool',
        help_text='Ignore warnings from womtool.jar',
        action='store_true',
    )
    womtool: str | None = arg(
        '--womtool',
        help_text="Path or URL for Cromwell's womtool JAR file",
        normalize=abspath,
    )
    java_heap_womtool: str = arg(
        '--java-heap-womtool',
        help_text='Java heap size for Womtool (java -Xmx)',
        default='1G',
    )
    gcp_monitoring_script: str | None = arg(
        '--gcp-monitoring-script',
        help_text='Monitoring script for gcp backend only',
    )


@dataclass
class DatabaseArgs:
    """Database configuration (parent_runner - DB section)."""

    db: str = arg(
        '--db',
        help_text='Cromwell metadata database type',
        default='file',
    )
    db_timeout: int = arg(
        '--db-timeout',
        help_text='Milliseconds to wait for DB connection',
        default=30000,
    )
    file_db: str | None = arg(
        '-d',
        '--file-db',
        help_text="Default DB file for Cromwell's built-in HyperSQL database",
        normalize=abspath,
    )
    mysql_db_ip: str = arg(
        '--mysql-db-ip',
        help_text='MySQL Database IP address (e.g. localhost)',
        default='localhost',
    )
    mysql_db_port: int = arg(
        '--mysql-db-port',
        help_text='MySQL Database TCP/IP port (e.g. 3306)',
        default=3306,
    )
    mysql_db_user: str = arg(
        '--mysql-db-user',
        help_text='MySQL DB username',
        default='cromwell',
    )
    mysql_db_password: str = arg(
        '--mysql-db-password',
        help_text='MySQL DB password',
        default='cromwell',
    )
    mysql_db_name: str = arg(
        '--mysql-db-name',
        help_text='MySQL DB name for Cromwell',
        default='cromwell',
    )
    postgresql_db_ip: str = arg(
        '--postgresql-db-ip',
        help_text='PostgreSQL DB IP address (e.g. localhost)',
        default='localhost',
    )
    postgresql_db_port: int = arg(
        '--postgresql-db-port',
        help_text='PostgreSQL DB TCP/IP port (e.g. 5432)',
        default=5432,
    )
    postgresql_db_user: str = arg(
        '--postgresql-db-user',
        help_text='PostgreSQL DB username',
        default='cromwell',
    )
    postgresql_db_password: str = arg(
        '--postgresql-db-password',
        help_text='PostgreSQL DB password',
        default='cromwell',
    )
    postgresql_db_name: str = arg(
        '--postgresql-db-name',
        help_text='PostgreSQL DB name for Cromwell',
        default='cromwell',
    )


@dataclass
class CromwellArgs:
    """Cromwell runtime arguments (parent_runner - Cromwell section)."""

    cromwell: str | None = arg(
        '--cromwell',
        help_text='Path or URL for Cromwell JAR file',
        normalize=abspath,
    )
    max_concurrent_tasks: int = arg(
        '--max-concurrent-tasks',
        help_text='Number of concurrent tasks',
        default=1000,
    )
    max_concurrent_workflows: int = arg(
        '--max-concurrent-workflows',
        help_text='Number of concurrent workflows',
        default=40,
    )
    memory_retry_error_keys: str | None = arg(
        '--memory-retry-error-keys',
        help_text='Comma-separated keys for memory retry errors',
    )
    disable_call_caching: bool = arg(
        '--disable-call-caching',
        help_text="Disable Cromwell's call caching",
        action='store_true',
    )
    backend_file: str | None = arg(
        '--backend-file',
        help_text='Custom Cromwell backend configuration file to override all',
        normalize=abspath,
    )
    cromwell_stdout: str = arg(
        '--cromwell-stdout',
        help_text='Local file to write STDOUT of Cromwell Java process to',
        default='./cromwell.out',
        normalize=abspath,
    )
    soft_glob_output: bool = arg(
        '--soft-glob-output',
        help_text='Use soft-linking when globbing outputs',
        action='store_true',
    )
    local_hash_strat: str = arg(
        '--local-hash-strat',
        help_text='File hashing strategy for call caching (local backends only)',
        default='path+modtime',
        choices=['file', 'path', 'path+modtime'],
    )


@dataclass
class LocalBackendArgs:
    """Local backend arguments (parent_runner - local section)."""

    local_out_dir: str = arg(
        '--local-out-dir',
        '--out-dir',
        help_text='Output directory path for local backend',
        default='.',
        normalize=abspath,
    )
    slurm_resource_param: str | None = arg(
        '--slurm-resource-param',
        help_text='SLURM resource parameters to be passed to sbatch',
    )


@dataclass
class GcpRunnerArgs:
    """GCP backend arguments (parent_runner - GCP section)."""

    gcp_prj: str | None = arg(
        '--gcp-prj',
        help_text='Google Cloud project',
    )
    gcp_region: str = arg(
        '--gcp-region',
        help_text='GCP region for Google Cloud Batch API',
        default='us-central1',
    )
    gcp_compute_service_account: str | None = arg(
        '--gcp-compute-service-account',
        help_text='Service account email to use for Google Cloud Batch compute instances',
    )
    gcp_call_caching_dup_strat: str = arg(
        '--gcp-call-caching-dup-strat',
        help_text='Duplication strategy for call-cached outputs for GCP backend',
        default='reference',
        choices=['reference', 'copy'],
    )
    gcp_out_dir: str | None = arg(
        '--gcp-out-dir',
        '--out-gcs-bucket',
        help_text='Output directory path for GCP backend (e.g., gs://my-bucket/my-output)',
    )


@dataclass
class AwsRunnerArgs:
    """AWS backend arguments (parent_runner - AWS section)."""

    aws_batch_arn: str | None = arg(
        '--aws-batch-arn',
        help_text='ARN for AWS Batch',
    )
    aws_region: str | None = arg(
        '--aws-region',
        help_text='AWS region (e.g. us-west-1)',
    )
    aws_out_dir: str | None = arg(
        '--aws-out-dir',
        '--out-s3-bucket',
        help_text='Output path on S3 bucket for AWS backend (e.g., s3://my-bucket/my-output)',
    )
    aws_call_caching_dup_strat: str = arg(
        '--aws-call-caching-dup-strat',
        help_text='Duplication strategy for call-cached outputs for AWS backend',
        default='reference',
        choices=['reference', 'copy'],
    )


@dataclass
class SchedulerArgs:
    """HPC scheduler arguments (parent_submit - scheduler section)."""

    slurm_partition: str | None = arg(
        '--slurm-partition',
        help_text='SLURM partition',
    )
    slurm_account: str | None = arg(
        '--slurm-account',
        help_text='SLURM account',
    )
    slurm_extra_param: str | None = arg(
        '--slurm-extra-param',
        help_text='SLURM extra parameters to be passed to sbatch',
    )
    sge_pe: str | None = arg(
        '--sge-pe',
        help_text='SGE parallel environment',
    )
    sge_queue: str | None = arg(
        '--sge-queue',
        help_text='SGE queue',
    )
    sge_extra_param: str | None = arg(
        '--sge-extra-param',
        help_text='SGE extra parameters',
    )
    pbs_queue: str | None = arg(
        '--pbs-queue',
        help_text='PBS queue',
    )
    pbs_extra_param: str | None = arg(
        '--pbs-extra-param',
        help_text='PBS extra parameters',
    )
    lsf_queue: str | None = arg(
        '--lsf-queue',
        help_text='LSF queue',
    )
    lsf_extra_param: str | None = arg(
        '--lsf-extra-param',
        help_text='LSF extra parameters',
    )


@dataclass
class RunSpecificArgs:
    """Run command specific arguments."""

    metadata_output: str | None = arg(
        '-m',
        '--metadata-output',
        help_text='An optional directory path to output metadata JSON file',
        normalize=abspath,
    )
    java_heap_run: str = arg(
        '--java-heap-run',
        help_text="Cromwell Java heap size for 'run' mode (java -Xmx)",
        default='8G',
    )


@dataclass
class ServerSpecificArgs:
    """Server command specific arguments."""

    java_heap_server: str = arg(
        '--java-heap-server',
        help_text="Cromwell Java heap size for 'server' mode (java -Xmx)",
        default='8G',
    )
    disable_auto_write_metadata: bool = arg(
        '--disable-auto-write-metadata',
        help_text='Disable automatic retrieval/update/writing of metadata.json',
        action='store_true',
    )


@dataclass
class ListDisplayArgs:
    """List command display options."""

    format: str = arg(
        '-f',
        '--format',
        help_text="Comma-separated list of items to show for 'list' subcommand",
        default='id,status,name,str_label,user,parent,submission',
    )
    hide_result_before: str | None = arg(
        '--hide-result-before',
        help_text='Hide workflows submitted before this date/time',
    )
    show_subworkflow: bool = arg(
        '--show-sub-workflow',
        help_text="Show sub-workflows in 'caper list'",
        action='store_true',
    )


@dataclass
class TroubleshootDisplayArgs:
    """Troubleshoot command display options."""

    show_completed_task: bool = arg(
        '--show-completed-task',
        help_text='Show information about completed tasks',
        action='store_true',
    )
    show_stdout: bool = arg(
        '--show-stdout',
        help_text='Show STDOUT for failed tasks',
        action='store_true',
    )


@dataclass
class GcpMonitorDisplayArgs:
    """GCP monitor display options."""

    json_format: bool = arg(
        '--json-format',
        help_text='Prints out outputs in a JSON format',
        action='store_true',
    )


@dataclass
class CleanupActionArgs:
    """Cleanup command options."""

    delete: bool = arg(
        '--delete',
        help_text='DELETE OUTPUTS. caper cleanup runs in a dry-run mode by default',
        action='store_true',
    )
    num_threads: int = arg(
        '--num-threads',
        help_text="Number of threads for cleaning up workflow's outputs",
        default=1,
    )
