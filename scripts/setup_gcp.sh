#!/bin/bash
# One-time GCP setup: enables required APIs and creates the two GCS buckets
# used by this project. Run from Cloud Shell (or any shell with gcloud auth'd
# to your project).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

echo ">> Setting active project to ${PROJECT_ID}"
gcloud config set project "${PROJECT_ID}"

echo ">> Enabling required APIs"
gcloud services enable \
  dataproc.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com

echo ">> Creating buckets (skips if they already exist)"
gsutil mb -l "${REGION}" "${DATA_BUCKET}" 2>/dev/null || echo "   ${DATA_BUCKET} already exists"
gsutil mb -l "${REGION}" "${STAGING_BUCKET}" 2>/dev/null || echo "   ${STAGING_BUCKET} already exists"

echo ">> Done. Next: run scripts/download_data.sh"
