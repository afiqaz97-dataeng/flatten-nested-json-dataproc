#!/bin/bash
# Submits the flatten_json.py job as a Dataproc Serverless (Batches) job.
# Pass --validate as an extra arg to also run validate_output.py afterwards.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo ">> Staging source files to ${STAGING_BUCKET}"
gsutil cp "${REPO_ROOT}/src/flatten_json.py" "${STAGING_BUCKET}/"
gsutil cp "${REPO_ROOT}/src/validate_output.py" "${STAGING_BUCKET}/"

BATCH_ID="flatten-openfda-$(date +%s)"

echo ">> Submitting batch ${BATCH_ID}"
gcloud dataproc batches submit pyspark \
  "${STAGING_BUCKET}/flatten_json.py" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --batch="${BATCH_ID}" \
  --deps-bucket="${STAGING_BUCKET}" \
  -- \
  --input="${RAW_PATH}" \
  --output="${FLATTENED_PATH}" \
  --output-format=parquet \
  --explode-arrays=true \
  --multiline=true \
  --root-array=results

if [[ "${1:-}" == "--validate" ]]; then
  VALIDATE_BATCH_ID="validate-openfda-$(date +%s)"
  echo ">> Submitting validation batch ${VALIDATE_BATCH_ID}"
  gcloud dataproc batches submit pyspark \
    "${STAGING_BUCKET}/validate_output.py" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --batch="${VALIDATE_BATCH_ID}" \
    --deps-bucket="${STAGING_BUCKET}" \
    -- \
    --raw-input="${RAW_PATH}" \
    --flattened-input="${FLATTENED_PATH}" \
    --root-array=results \
    --multiline=true
  echo ">> View validation logs with: gcloud dataproc batches describe ${VALIDATE_BATCH_ID} --region=${REGION}"
fi

echo ">> Submitted. Check status with: gcloud dataproc batches describe ${BATCH_ID} --region=${REGION}"
