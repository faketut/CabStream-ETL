#!/usr/bin/env bash
set -euo pipefail

# Required environment:
#   GCP_PROJECT_ID  - target GCP project ID
#   ADMIN_CIDR      - CIDR block allowed to reach the Airflow UI (e.g. 203.0.113.4/32)
# Optional environment:
#   CREDENTIALS_PATH - path to the service account JSON (defaults to ./credentials.json)

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be exported}"
: "${ADMIN_CIDR:?ADMIN_CIDR must be exported (e.g. 203.0.113.4/32)}"

CREDENTIALS_PATH="${CREDENTIALS_PATH:-$(pwd)/credentials.json}"

if ! command -v terraform >/dev/null 2>&1; then
    echo "Terraform not found. Please install it first." >&2
    exit 1
fi

if [[ ! -f "$CREDENTIALS_PATH" ]]; then
    echo "Credentials file not found at $CREDENTIALS_PATH" >&2
    exit 1
fi

export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_PATH"

cd nyc_taxi_pipeline/terraform

terraform init
terraform plan \
    -var="project_id=$GCP_PROJECT_ID" \
    -var="admin_cidr=$ADMIN_CIDR"

read -r -p "Apply the plan above? [y/N] " confirm
if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
    terraform apply \
        -var="project_id=$GCP_PROJECT_ID" \
        -var="admin_cidr=$ADMIN_CIDR"
else
    echo "Aborted."
fi
