# Configuration for Google Cloud Platform backend (`gcp`)

> **NOTE**: For complete GCP server setup instructions, see [scripts/gcp_caper_server/README.md](../scripts/gcp_caper_server/README.md).

> **IMPORTANT**: Google Cloud Genomics API and Cloud Life Sciences API have been deprecated and removed. Caper now uses [Google Cloud Batch API](https://cloud.google.com/batch) exclusively.

## Prerequisites

1. Sign up for a Google account and set up billing in the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a [Google Project](https://console.developers.google.com/project).
3. Create a [Google Cloud Storage bucket](https://console.cloud.google.com/storage/browser) to store pipeline outputs.
4. Enable the following APIs in your [API Manager](https://console.developers.google.com/apis/library):
    * Compute Engine API
    * Google Cloud Storage
    * Google Cloud Storage JSON API
    * Cloud Batch API

5. Set your default Google Cloud Project:
    ```bash
    $ gcloud config set project [YOUR_PROJECT_NAME]
    ```

## Authentication

### Recommended: Application Default Credentials (ADC)

Caper uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) to authenticate with Google Cloud services. The recommended authentication method depends on your environment:

**On a Compute Engine VM:**

Attach a service account to the VM instance. Applications automatically use the VM's credentials via the metadata server—no additional configuration needed.

```bash
# When creating the VM
gcloud compute instances create [INSTANCE_NAME] \
    --service-account=[SERVICE_ACCOUNT_EMAIL] \
    --scopes=cloud-platform
```

**For local development:**

Use your Google account credentials:

```bash
$ gcloud auth login --no-launch-browser
$ gcloud auth application-default login --no-launch-browser
```

### Legacy: Service Account JSON Keys (Not Recommended)

> **WARNING**: JSON key files pose security risks—they can be leaked, are difficult to rotate, and provide long-lived credentials. Prefer VM-attached service accounts or user credentials instead.

If you must use JSON keys:
- Store securely with restricted permissions (`chmod 600`)
- Never commit to version control
- Rotate regularly
- Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

Consider [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) as a more secure alternative to JSON keys.

## Service Account Permissions

Create a service account with the following roles:
* Service Account User
* Compute Admin
* Batch Admin
* Storage Admin (or configure per-bucket permissions)

> **NOTE**: The service account used to launch Batch jobs is different from the Compute Service Account used by Batch VMs to run tasks. You can specify a different Compute Service Account using `--gcp-compute-service-account`. The Compute Service Account needs `roles/batch.agentReporter` to report status back to Batch.

## Troubleshooting

If you see permission errors at runtime:

1. Verify your VM has an attached service account with the correct roles
2. Ensure `GOOGLE_APPLICATION_CREDENTIALS` is unset if using default credentials
3. Check that the service account has access to required GCS buckets
4. Run `gcloud auth application-default print-access-token` to verify credentials are working
