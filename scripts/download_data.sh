#!/bin/bash
# Downloads the openFDA food/event dataset (no API key required) and uploads
# it to GCS. Run from Cloud Shell or any machine with open internet egress
# and gsutil configured.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

WORKDIR="$(mktemp -d)"
cd "${WORKDIR}"

echo ">> Downloading dataset from ${DATASET_URL}"
curl -sS -o dataset.json.zip "${DATASET_URL}"

echo ">> Unzipping"
unzip -o dataset.json.zip
JSON_FILE="$(find . -maxdepth 1 -name '*.json' | head -n 1)"

echo ">> Uploading ${JSON_FILE} to ${RAW_PATH}"
gsutil cp "${JSON_FILE}" "${RAW_PATH}"

cd /
rm -rf "${WORKDIR}"

echo ">> Done. Next: run scripts/submit_batch.sh"
