#!/bin/bash
set -eo pipefail

if [[ $# -lt 1 ]]; then
  echo "Automated shell script to create Caper server instance with PostgreSQL on Google Cloud."
  echo
  echo "Usage: ./create_instance.sh [INSTANCE_NAME] [GCP_PRJ] [GCP_OUT_DIR] <OPTIONAL_ARGUMENTS>"
  echo
  echo "Positional arguments:"
  echo "  [INSTANCE_NAME]: New instance's name."
  echo "  [GCP_PRJ]: Your project's ID on Google Cloud Platform. --gcp-prj in Caper."
  echo "  [GCP_OUT_DIR]: gs:// bucket dir path for outputs. --gcp-out-dir in Caper."
  echo
  echo "Authentication (choose one):"
  echo "  --service-account: (RECOMMENDED) Service account email to attach to the VM."
  echo "                     The VM will authenticate via metadata server (ADC)."
  echo "                     Example: my-sa@my-project.iam.gserviceaccount.com"
  echo "  --service-account-key-json: (LEGACY) Path to service account JSON key file."
  echo "                              Not recommended for production use."
  echo
  echo "Optional arguments for Caper:"
  echo "  -l, --gcp-loc-dir: gs:// bucket dir path for localization."
  echo "  --gcp-region: Region for Google Cloud Batch API. us-central1 by default."
  echo "  --gcp-compute-service-account: Service account for Batch worker VMs (if different from main SA)."
  echo "  --postgresql-db-ip: localhost by default."
  echo "  --postgresql-db-port: 5432 by default."
  echo "  --postgresql-db-user: cromwell by default."
  echo "  --postgresql-db-password: cromwell by default."
  echo "  --postgresql-db-name: cromwell by default."
  echo
  echo "Optional arguments for instance creation (gcloud compute instances create):"
  echo "  -z, --zone: Zone. Check available zones: gcloud compute zones list. us-central1-a by default."
  echo "  -m, --machine-type: Machine type. Check available machine-types: gcloud compute machine-types list. n1-standard-4 by default."
  echo "  -b, --boot-disk-size: Boot disk size. Use a suffix for unit. e.g. GB and MB. 100GB by default."
  echo "  -u, --username: Username for SSH. ubuntu by default."
  echo "  --boot-disk-type: Boot disk type. pd-standard (Standard persistent disk) by default."
  echo "  --image: Image. Check available images: gcloud compute images list. ubuntu-2204-jammy-v20240119 by default."
  echo "  --image-project: Image project. ubuntu-os-cloud by default."
  echo "  --tags: Tags to apply to the new instance. caper-server by default."
  echo "  --startup-script: Startup script CONTENTS (NOT A FILE)."
  echo
  echo "Examples:"
  echo "  # Recommended: Using VM-attached service account"
  echo "  ./create_instance.sh my-caper prod-project gs://my-bucket/caper-out \\"
  echo "      --service-account caper-sa@prod-project.iam.gserviceaccount.com"
  echo
  echo "  # Legacy: Using JSON key file (not recommended)"
  echo "  ./create_instance.sh my-caper prod-project gs://my-bucket/caper-out \\"
  echo "      --service-account-key-json ~/keys/service-account.json"
  echo

  if [[ $# -lt 3 ]]; then
    echo "Error: Define all positional arguments."
  fi
  exit 1
fi

# parse opt args first.
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --service-account)
      SERVICE_ACCOUNT_EMAIL="$2"
      shift
      shift
      ;;
    --service-account-key-json)
      GCP_SERVICE_ACCOUNT_KEY_JSON_FILE="${2/#\~/$HOME}"
      shift
      shift
      ;;
    -l|--gcp-loc-dir)
      GCP_LOC_DIR="$2"
      shift
      shift
      ;;
    --gcp-region)
      GCP_REGION="$2"
      shift
      shift
      ;;
    --gcp-compute-service-account)
      GCP_COMPUTE_SERVICE_ACCOUNT="$2"
      shift
      shift
      ;;
    --postgresql-db-ip)
      POSTGRESQL_DB_IP="$2"
      shift
      shift
      ;;
    --postgresql-db-port)
      POSTGRESQL_DB_PORT="$2"
      shift
      shift
      ;;
    --postgresql-db-user)
      POSTGRESQL_DB_USER="$2"
      shift
      shift
      ;;
    --postgresql-db-password)
      POSTGRESQL_DB_PASSWORD="$2"
      shift
      shift
      ;;
    --postgresql-db-name)
      POSTGRESQL_DB_NAME="$2"
      shift
      shift
      ;;
    -z|--zone)
      ZONE="$2"
      shift
      shift
      ;;
    -m|--machine-type)
      MACHINE_TYPE="$2"
      shift
      shift
      ;;
    -b|--boot-disk-size)
      BOOT_DISK_SIZE="$2"
      shift
      shift
      ;;
    -u|--username)
      USERNAME="$2"
      shift
      shift
      ;;
    --boot-disk-type)
      BOOT_DISK_TYPE="$2"
      shift
      shift
      ;;
    --image)
      IMAGE="$2"
      shift
      shift
      ;;
    --image-project)
      IMAGE_PROJECT="$2"
      shift
      shift
      ;;
    --tags)
      TAGS="$2"
      shift
      shift
      ;;
    --startup-script)
      STARTUP_SCRIPT="$2"
      shift
      shift
      ;;
    -*)
      echo "Unknown parameter: $1."
      shift
      exit 1
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

# restore pos args.
set -- "${POSITIONAL[@]}"

# parse pos args.
INSTANCE_NAME="$1"
GCP_PRJ="$2"
GCP_OUT_DIR="$3"

# Determine authentication method
USE_VM_SERVICE_ACCOUNT=false
if [[ -n "$SERVICE_ACCOUNT_EMAIL" ]]; then
  USE_VM_SERVICE_ACCOUNT=true
  echo "Using VM-attached service account: $SERVICE_ACCOUNT_EMAIL"
elif [[ -n "$GCP_SERVICE_ACCOUNT_KEY_JSON_FILE" ]]; then
  echo "WARNING: Using JSON key file authentication (not recommended for production)."
  echo "Consider using --service-account for better security."
else
  echo "Error: Must specify either --service-account or --service-account-key-json"
  echo "  --service-account EMAIL      (recommended) Attach service account to VM"
  echo "  --service-account-key-json   (legacy) Use JSON key file"
  exit 1
fi

# set defaults for opt args. (caper)
if [[ -z "$GCP_LOC_DIR" ]]; then
  GCP_LOC_DIR="$GCP_OUT_DIR"/.caper_tmp
fi
if [[ -z "$GCP_REGION" ]]; then
  GCP_REGION=us-central1
fi
if [[ -z "$POSTGRESQL_DB_IP" ]]; then
  POSTGRESQL_DB_IP=localhost
fi
if [[ -z "$POSTGRESQL_DB_PORT" ]]; then
  POSTGRESQL_DB_PORT=5432
fi
if [[ -z "$POSTGRESQL_DB_USER" ]]; then
  POSTGRESQL_DB_USER=cromwell
fi
if [[ -z "$POSTGRESQL_DB_PASSWORD" ]]; then
  POSTGRESQL_DB_PASSWORD=cromwell
fi
if [[ -z "$POSTGRESQL_DB_NAME" ]]; then
  POSTGRESQL_DB_NAME=cromwell
fi

# set defaults for opt args. (gcloud)
if [[ -z "$ZONE" ]]; then
  ZONE=us-central1-a
fi
if [[ -z "$MACHINE_TYPE" ]]; then
  MACHINE_TYPE=n1-standard-4
fi
if [[ -z "$BOOT_DISK_SIZE" ]]; then
  BOOT_DISK_SIZE=100GB
fi
if [[ -z "$USERNAME" ]]; then
  USERNAME=ubuntu
fi
if [[ -z "$BOOT_DISK_TYPE" ]]; then
  BOOT_DISK_TYPE=pd-standard
fi
if [[ -z "$IMAGE" ]]; then
  IMAGE=ubuntu-2204-jammy-v20240119
fi
if [[ -z "$IMAGE_PROJECT" ]]; then
  IMAGE_PROJECT=ubuntu-os-cloud
fi
if [[ -z "$TAGS" ]]; then
  TAGS=caper-server
fi
if [[ -z "$STARTUP_SCRIPT" ]]; then
  STARTUP_SCRIPT="$(cat <<'EOF'
apt-get update
apt-get -y install \
  screen \
  git \
  curl \
  openjdk-17-jre-headless \
  postgresql \
  postgresql-contrib \
  acl \
  software-properties-common

# Install uv (official installer) and use it to provision Python 3.12 (no PPAs).
env UV_INSTALL_DIR=/usr/local/bin sh -c 'curl -LsSf https://astral.sh/uv/install.sh | sh'
EOF
)"
fi

# validate all args.
if [[ -z "$GCP_PRJ" ]]; then
  echo "[GCP_PRJ] is not valid."
  exit 1
fi
if [[ "$GCP_OUT_DIR" != gs://* ]]; then
  echo "[GCP_OUT_DIR] should be a GCS bucket path starting with gs://"
  exit 1
fi
if [[ "$GCP_LOC_DIR" != gs://* ]]; then
  echo "-l, --gcp-loc-dir should be a GCS bucket path starting with gs://"
  exit 1
fi
if [[ "$USE_VM_SERVICE_ACCOUNT" == false && ! -f "$GCP_SERVICE_ACCOUNT_KEY_JSON_FILE" ]]; then
  echo "[GCP_SERVICE_ACCOUNT_KEY_JSON_FILE] does not exist: $GCP_SERVICE_ACCOUNT_KEY_JSON_FILE"
  exit 1
fi
if [[ "$POSTGRESQL_DB_IP" == localhost && "$POSTGRESQL_DB_PORT" != 5432 ]]; then
  echo "--postgresql-db-port should be 5432 for locally installed PostgreSQL (--postgresql-db-ip localhost)."
  exit 1
fi

# constants for files/params on instance.
GCP_AUTH_SH="/etc/profile.d/gcp-auth.sh"
CAPER_CONF_DIR=/opt/caper
ROOT_CAPER_CONF_DIR=/root/.caper
GLOBAL_CAPER_CONF_FILE="$CAPER_CONF_DIR/default.conf"
REMOTE_KEY_FILE="$CAPER_CONF_DIR/service_account_key.json"

# Build the caper config content based on authentication method
CAPER_CONFIG_CONTENT="# caper
backend=gcp
no-server-heartbeat=True
# cromwell
max-concurrent-workflows=300
max-concurrent-tasks=1000
# local backend
local-out-dir=$CAPER_CONF_DIR/local_out_dir
local-loc-dir=$CAPER_CONF_DIR/local_loc_dir
# gcp backend
gcp-prj=$GCP_PRJ
gcp-region=$GCP_REGION
gcp-out-dir=$GCP_OUT_DIR
gcp-loc-dir=$GCP_LOC_DIR"

# Add compute service account if specified
if [[ -n "$GCP_COMPUTE_SERVICE_ACCOUNT" ]]; then
  CAPER_CONFIG_CONTENT="$CAPER_CONFIG_CONTENT
gcp-compute-service-account=$GCP_COMPUTE_SERVICE_ACCOUNT"
fi

# Add JSON key path only if using legacy authentication
if [[ "$USE_VM_SERVICE_ACCOUNT" == false ]]; then
  CAPER_CONFIG_CONTENT="$CAPER_CONFIG_CONTENT
gcp-service-account-key-json=$REMOTE_KEY_FILE"
fi

# Add database config
CAPER_CONFIG_CONTENT="$CAPER_CONFIG_CONTENT
# metadata DB
db=postgresql
postgresql-db-ip=$POSTGRESQL_DB_IP
postgresql-db-port=$POSTGRESQL_DB_PORT
postgresql-db-user=$POSTGRESQL_DB_USER
postgresql-db-password=$POSTGRESQL_DB_PASSWORD
postgresql-db-name=$POSTGRESQL_DB_NAME"

# Build GCP auth script content based on authentication method
if [[ "$USE_VM_SERVICE_ACCOUNT" == true ]]; then
  # VM-attached service account: no need for explicit auth, just set up symlinks
  GCP_AUTH_SCRIPT_CONTENT='# Authentication via VM-attached service account (metadata server)
# No explicit credentials needed - using Application Default Credentials
mkdir -p ~/.caper
ln -sf /opt/caper/default.conf ~/.caper/ 2>/dev/null || true'
else
  # Legacy JSON key file authentication
  GCP_AUTH_SCRIPT_CONTENT="gcloud auth activate-service-account --key-file=$REMOTE_KEY_FILE
mkdir -p ~/.caper
ln -sf /opt/caper/default.conf ~/.caper/ 2>/dev/null || true
export GOOGLE_APPLICATION_CREDENTIALS=$REMOTE_KEY_FILE"
fi

# prepend more init commands to the startup-script
STARTUP_SCRIPT="""#!/bin/bash
set -euo pipefail
### make caper's directories
mkdir -p $CAPER_CONF_DIR
mkdir -p $CAPER_CONF_DIR/local_loc_dir $CAPER_CONF_DIR/local_out_dir

### set default permission on caper's directories
chmod 777 -R $CAPER_CONF_DIR
setfacl -R -d -m u::rwX $CAPER_CONF_DIR
setfacl -R -d -m g::rwX $CAPER_CONF_DIR
setfacl -R -d -m o::rwX $CAPER_CONF_DIR

### make caper conf file
cat <<'EOF' > $GLOBAL_CAPER_CONF_FILE
$CAPER_CONFIG_CONTENT
EOF
chmod +r $GLOBAL_CAPER_CONF_FILE

### soft-link conf file for root
mkdir -p $ROOT_CAPER_CONF_DIR
ln -sf $GLOBAL_CAPER_CONF_FILE $ROOT_CAPER_CONF_DIR

### google auth shared for all users
touch $GCP_AUTH_SH
cat <<'AUTHEOF' > $GCP_AUTH_SH
$GCP_AUTH_SCRIPT_CONTENT
AUTHEOF

$STARTUP_SCRIPT
"""

# append more init commands to the startup-script
STARTUP_SCRIPT="""$STARTUP_SCRIPT
### init PostgreSQL for Cromwell
sudo -u postgres createuser root -s
createdb $POSTGRESQL_DB_NAME
psql -d $POSTGRESQL_DB_NAME -c \"create extension lo;\"
psql -d $POSTGRESQL_DB_NAME -c \"create role $POSTGRESQL_DB_USER with superuser login password '$POSTGRESQL_DB_PASSWORD'\"

### upgrade pip and install caper croo
# Install CLI tools into isolated uv tool environments and link executables to /usr/local/bin.
mkdir -p /opt/caper/uv-tools
env UV_TOOL_DIR=/opt/caper/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin \\
  /usr/local/bin/uv tool install --python 3.12 git+https://github.com/MoTrPAC/caper
env UV_TOOL_DIR=/opt/caper/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin \\
  /usr/local/bin/uv tool install --python 3.12 croo
"""

# Authenticate locally if using JSON key file
if [[ "$USE_VM_SERVICE_ACCOUNT" == false ]]; then
  echo "$(date): Google auth with service account key file."
  gcloud auth activate-service-account --key-file="$GCP_SERVICE_ACCOUNT_KEY_JSON_FILE"
  export GOOGLE_APPLICATION_CREDENTIALS="$GCP_SERVICE_ACCOUNT_KEY_JSON_FILE"
fi

echo "$(date): Making a temporary startup script..."
echo "$STARTUP_SCRIPT" > tmp_startup_script.sh

echo "$(date): Creating an instance..."
if [[ "$USE_VM_SERVICE_ACCOUNT" == true ]]; then
  # Create VM with attached service account
  gcloud --project "$GCP_PRJ" compute instances create \
    "$INSTANCE_NAME" \
    --boot-disk-size="$BOOT_DISK_SIZE" \
    --boot-disk-type="$BOOT_DISK_TYPE" \
    --machine-type="$MACHINE_TYPE" \
    --zone="$ZONE" \
    --image="$IMAGE" \
    --image-project="$IMAGE_PROJECT" \
    --tags="$TAGS" \
    --service-account="$SERVICE_ACCOUNT_EMAIL" \
    --scopes=cloud-platform \
    --metadata-from-file startup-script=tmp_startup_script.sh
else
  # Create VM without attached service account (will use JSON key)
  gcloud --project "$GCP_PRJ" compute instances create \
    "$INSTANCE_NAME" \
    --boot-disk-size="$BOOT_DISK_SIZE" \
    --boot-disk-type="$BOOT_DISK_TYPE" \
    --machine-type="$MACHINE_TYPE" \
    --zone="$ZONE" \
    --image="$IMAGE" \
    --image-project="$IMAGE_PROJECT" \
    --tags="$TAGS" \
    --metadata-from-file startup-script=tmp_startup_script.sh
fi
echo "$(date): Created an instance successfully."

echo "$(date): Deleting the temporary startup script..."
rm -f tmp_startup_script.sh

while [[ $(gcloud --project "$GCP_PRJ" compute instances describe "${INSTANCE_NAME}" --zone "${ZONE}" --format="value(status)") != "RUNNING" ]]; do
    echo "$(date): Waiting for 20 seconds for the instance to spin up..."
    sleep 20
done

# Transfer key file only if using legacy authentication
if [[ "$USE_VM_SERVICE_ACCOUNT" == false ]]; then
  echo "$(date): If key file transfer fails for several times then manually transfer it to $REMOTE_KEY_FILE on the instance."
  echo "$(date): Transferring service account key file to the instance..."
  until gcloud --project "$GCP_PRJ" compute scp "$GCP_SERVICE_ACCOUNT_KEY_JSON_FILE" "$USERNAME"@"$INSTANCE_NAME":"$REMOTE_KEY_FILE" --zone="$ZONE"; do
    echo "$(date): Key file transfer failed. Retrying in 20 seconds..."
    sleep 20
  done
  echo "$(date): Transferred a key file to instance successfully."
fi

echo "$(date): Waiting for the instance finishing up installing Caper..."
until gcloud --project "$GCP_PRJ" compute ssh --zone="$ZONE" "$USERNAME"@"$INSTANCE_NAME" --command="caper -v"; do
  echo "$(date): Caper has not been installed yet. Retrying in 40 seconds..."
  sleep 40
done
echo "$(date): Finished installing Caper on the instance. Ready to run Caper server."

echo "$(date): Spinning up Caper server..."
gcloud --project "$GCP_PRJ" compute ssh --zone="$ZONE" "$USERNAME"@"$INSTANCE_NAME" --command="cd $CAPER_CONF_DIR && sudo screen -dmS caper_server bash -c \"sudo caper server > caper_server.log 2>&1\""
sleep 60
until gcloud --project "$GCP_PRJ" compute ssh --zone="$ZONE" "$USERNAME"@"$INSTANCE_NAME" --command="caper list"; do
  echo "$(date): Caper server has not been started yet. Retrying in 60 seconds..."
  sleep 60
done
echo
echo "$(date): Caper server is up and ready to take submissions."
echo "$(date): You can find Caper server log file at $CAPER_CONF_DIR/caper_server.log."
echo "$(date): Cromwell's STDERR will be written to $CAPER_CONF_DIR/cromwell.out*."
echo
if [[ "$USE_VM_SERVICE_ACCOUNT" == true ]]; then
  echo "$(date): Authentication: VM-attached service account ($SERVICE_ACCOUNT_EMAIL)"
  echo "$(date): The VM uses Application Default Credentials via the metadata server."
else
  echo "$(date): Authentication: JSON key file at $REMOTE_KEY_FILE"
  echo "$(date): WARNING: Consider migrating to VM-attached service accounts for better security."
fi
echo
echo "$(date): Use the following command line to SSH to the instance."
echo
echo "gcloud compute ssh --zone $ZONE $INSTANCE_NAME --project $GCP_PRJ"
echo
