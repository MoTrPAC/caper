## Introduction

`create_instance.sh` will create an instance on Google Cloud Compute Engine in your project and configure the instance for Caper with PostgreSQL database and Google Cloud Batch API.

> **NOTE**: Google Cloud Genomics API and Cloud Life Sciences API have been deprecated and removed. Caper now uses Google Cloud Batch API exclusively.

## Prerequisites

### Install Google Cloud CLI

Make sure that `gcloud` (Google Cloud CLI) is installed on your local system. See [Install the gcloud CLI](https://cloud.google.com/sdk/docs/install) for instructions.

### Enable Required APIs

Go to [APIs & Services](https://console.cloud.google.com/apis/dashboard) on your project and enable the following APIs:
* Compute Engine API
* Cloud Storage
* Cloud Storage JSON API
* Cloud Batch API

### Create a Service Account

Go to [Service accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) on your project and create a new service account with the following roles:
* Compute Admin
* Storage Admin (or configure permissions on individual buckets)
* Batch Admin
* **Service Account User** (required for impersonation)

> **IMPORTANT**: The service account specified above is used to launch Batch jobs. This is different from the Compute Service Account used by the Google Cloud Batch VMs to run the actual tasks. You can specify a different Compute Service Account using the `--gcp-compute-service-account` parameter. The Compute Service Account needs the `roles/batch.agentReporter` role to report status back to Batch.

## Authentication Methods

### Recommended: VM-Attached Service Account (Default Credentials)

The most secure approach is to attach the service account directly to the Compute Engine VM instance. This uses Google's metadata server for authentication and eliminates the need for JSON key files.

1. When creating the VM instance, attach the service account:
   ```bash
   gcloud compute instances create [INSTANCE_NAME] \
       --service-account=[SERVICE_ACCOUNT_EMAIL] \
       --scopes=cloud-platform \
       --zone=[ZONE]
   ```

2. On the VM, applications automatically authenticate using the attached service account via [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials).

3. No additional authentication setup is needed. Caper will automatically use the VM's credentials.

**Benefits:**
- No JSON key files to manage or secure
- Credentials are automatically rotated by Google
- No risk of key file exposure
- Works automatically with all Google Cloud client libraries

### Alternative: User Credentials (for local development)

For local development or testing, you can use your own Google account:

```bash
# Authenticate with your Google account
$ gcloud auth login --no-launch-browser

# Set up Application Default Credentials
$ gcloud auth application-default login --no-launch-browser
```

### Legacy: Service Account JSON Key (Not Recommended)

> **WARNING**: Using JSON key files is discouraged for production environments. Keys can be leaked, are difficult to rotate, and provide long-lived credentials.

If you must use a JSON key file:

1. Generate a key from the service account in the Google Cloud Console
2. Store the key securely with restricted file permissions (`chmod 600`)
3. Pass it to `create_instance.sh` with `--service-account-key-json`

**Security considerations for JSON keys:**
- Never commit key files to version control
- Rotate keys regularly
- Use short-lived keys when possible
- Consider using [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) as an alternative

## How to create an instance

Run without arguments to see detailed help:
```bash
$ bash create_instance.sh
```

**Recommended:** Create an instance with a VM-attached service account:
```bash
$ bash create_instance.sh [INSTANCE_NAME] [PROJECT_ID] [GCP_OUT_DIR] \
    --service-account [SERVICE_ACCOUNT_EMAIL]
```

Example:
```bash
$ bash create_instance.sh my-caper-server my-gcp-project gs://my-bucket/caper-out \
    --service-account caper-sa@my-gcp-project.iam.gserviceaccount.com
```

**Legacy:** Create an instance with a JSON key file (not recommended):
```bash
$ bash create_instance.sh [INSTANCE_NAME] [PROJECT_ID] [GCP_OUT_DIR] \
    --service-account-key-json [PATH_TO_KEY_FILE]
```

> **NOTE**: Some optional arguments are important depending on your region/zone, e.g., `--gcp-region` (for provisioning worker instances of Batch API) and `--zone` (for server instance creation). These default to US central region/zones.

## How to stop Caper server

On the instance, attach to the existing screen `caper_server` and stop it with Ctrl + C:
```bash
$ sudo su
$ screen -r caper_server
# Press Ctrl + C to send SIGINT to Caper
```

## How to start Caper server

On the instance, create a new screen `caper_server`:
```bash
$ cd /opt/caper
$ screen -dmS caper_server bash -c "caper server > caper_server.log 2>&1"
```

## How to submit workflow

Check if `caper list` works without any network errors:
```bash
$ caper list
```

Submit a workflow:
```bash
$ caper submit [WDL] -i input.json ...
```

Caper will localize big data files on a GCS bucket directory `--gcp-loc-dir`, which defaults to `[GCP_OUT_DIR]/.caper_tmp/` if not defined.

## How to configure Caper

Caper looks for a default configuration file at `~/.caper/default.conf`. For shared server setups, this can be symlinked to a global configuration at `/opt/caper/default.conf`.

To use your own configuration:
```bash
$ mkdir -p ~/.caper
$ cp /opt/caper/default.conf ~/.caper/default.conf
# Edit ~/.caper/default.conf as needed
```

## Troubleshooting

See the main [DETAILS.md](../../DETAILS.md) documentation for troubleshooting information.

If you see permission errors at runtime, ensure:
1. The VM has an attached service account with the correct roles
2. Or `GOOGLE_APPLICATION_CREDENTIALS` is not set (to use default credentials)
3. The service account has access to the required GCS buckets
