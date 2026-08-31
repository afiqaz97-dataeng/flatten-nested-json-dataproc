#!/bin/bash
# Shared configuration sourced by the other scripts in this folder.
#
# This is a TEMPLATE. Copy it to config.sh (which is gitignored) and fill in
# your own values — never commit your real project ID or bucket names here.
#
#   cp scripts/config.sh.example scripts/config.sh
#   nano scripts/config.sh

export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"

# GCS buckets (must be globally unique — change these)
export DATA_BUCKET="gs://your-project-openfda-data"
export STAGING_BUCKET="gs://your-project-dataproc-staging"

# Paths within the data bucket
export RAW_PATH="${DATA_BUCKET}/raw/openfda/food-event-0001-of-0001.json"
export FLATTENED_PATH="${DATA_BUCKET}/flattened/openfda_food_events"

export DATASET_URL="https://download.open.fda.gov/food/event/food-event-0001-of-0001.json.zip"

# BigQuery
export BQ_DATASET="openfda"
export BQ_TABLE="food_events"