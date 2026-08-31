# Flattening Nested JSON with PySpark on Dataproc Serverless

A small, focused data engineering project: take real-world, deeply nested
JSON from a public API, and turn it into flat, analytics-ready Parquet using
PySpark, running as a **Dataproc Serverless (Batches)** job on GCP — no
cluster to provision or manage.

## Why this exists

Nested JSON (structs inside structs, arrays of structs, API response
envelopes) is one of the most common real-world data engineering problems,
and naive flattening approaches break down fast — especially once you have
**multiple arrays at the same nesting level**, which cause a cross-join
rather than a simple concatenation (see [Multiple sibling
arrays](#multiple-sibling-arrays-a-real-gotcha) below). This project handles
that deliberately and includes a test that pins the expected behavior down,
plus a validation job that checks the output for data loss and unexpected
nulls after the fact.

## Dataset

[openFDA Food Adverse Event Reports](https://open.fda.gov/apis/food/event/)
— publicly downloadable, no API key required, ~150K records, ~9 MB
compressed. Each report has:
- top-level scalar fields (`report_number`, `date_created`, ...)
- a nested `consumer` struct (`age`, `gender`)
- a `products` array of structs (one report can name multiple products)
- a `reactions` array of strings

The whole dataset is wrapped in an API-style envelope:
`{"meta": {...}, "results": [ {...}, {...} ]}`.

## Architecture

```
openFDA (public download)
        │  curl
        ▼
  GCS raw/ (json)
        │  Dataproc Serverless batch: flatten_json.py
        ▼
  GCS flattened/ (parquet)
        │  Dataproc Serverless batch: validate_output.py
        ▼
  console output: row counts, expansion ratio, null-rate per column
```

## Repo layout

```
.
├── src/
│   ├── flatten_json.py       # main Spark job: recursively flattens structs + explodes arrays
│   └── validate_output.py    # sanity-checks flattened output against the raw input
├── scripts/
│   ├── config.sh.example     # template — copy to config.sh and fill in your own values
│   ├── setup_gcp.sh          # enables APIs, creates GCS buckets
│   ├── download_data.sh      # downloads the dataset and stages it to GCS
│   ├── submit_batch.sh       # submits the Dataproc Serverless batch job
│   └── create_bq_table.sh    # creates a BigQuery external table over the output
├── tests/
│   ├── conftest.py
│   ├── test_flatten.py       # unit tests against a small synthetic fixture
│   └── fixtures/sample_events.json
└── .github/workflows/ci.yml  # runs tests on every push
```

## Running it

### 1. Configure

```bash
cp scripts/config.sh.example scripts/config.sh
```

Edit `scripts/config.sh` with your GCP project ID and two globally-unique
bucket names. This file is gitignored — your real project details never get
committed.

### 2. One-time GCP setup

```bash
bash scripts/setup_gcp.sh
```

Enables the Dataproc, Storage, and BigQuery APIs and creates the two buckets.

### 3. Get the data

```bash
bash scripts/download_data.sh
```

Downloads the openFDA dataset and uploads it to your raw GCS path. Run this
from **Cloud Shell** or any machine with open internet access — not required
to be the same machine you run the other scripts from.

### 4. Run the flatten job

```bash
bash scripts/submit_batch.sh --validate
```

Submits `flatten_json.py` as a Dataproc Serverless batch, then submits
`validate_output.py` as a second batch to sanity-check the result. Drop
`--validate` to skip the second step.

Check status any time with:
```bash
gcloud dataproc batches list --region=$REGION
gcloud dataproc batches describe BATCH_ID --region=$REGION
```

### 5. Query it in BigQuery

```bash
bash scripts/create_bq_table.sh
```

Creates a BigQuery **external table** directly over the Parquet in GCS —
queryable immediately, no separate load step or duplicated storage:

```bash
bq query --use_legacy_sql=false \
  'SELECT * FROM `'"${PROJECT_ID}"'.openfda.food_events` LIMIT 10'
```

### 6. Run tests locally (no GCP needed)

```bash
pip install -r requirements.txt
pytest tests/ -v
```

This runs PySpark locally against a tiny synthetic fixture — fast, and
doesn't touch GCP at all. Useful for iterating on `flatten_df()` itself.

## Actual results from a real run

| Metric | Value |
|---|---|
| Raw input | 101.5 MB (uncompressed JSON, ~150K records) |
| Flattened output | 11.6 MB (Parquet) |
| Compression ratio | ~9x |
| Flatten batch runtime | a few minutes (Dataproc Serverless cold start dominates at this data size) |

## The script's design

`flatten_json.py` works generically on **any** nested JSON, not just this
dataset:

- Struct columns are recursively expanded into `parent_child` named columns.
- Array columns are exploded into rows via `explode_outer` (keeps a null row
  rather than dropping records with an empty/missing array).
- `--root-array results` unwraps API-style envelopes
  (`{"meta": ..., "results": [...]}`) before flattening — this generalizes
  to most REST APIs (openFDA, GitHub, etc.), not just this one endpoint.
- `--explode-arrays false` disables array exploding entirely if you'd rather
  keep arrays as array columns (e.g. to write to BigQuery's native REPEATED
  field type instead of exploding to rows).

### Multiple sibling arrays: a real gotcha

If a record has two independent arrays at the same level — here, `products`
and `reactions` — exploding both produces one row per **(product,
reaction) pair**, not one row per product plus one row per reaction. A
report with 2 products and 3 reactions becomes **6** rows, not 5. This is
correct, standard Spark `explode` behavior, but it silently inflates row
counts if you're not expecting it. `tests/test_flatten.py` pins this down
with an explicit assertion and comment so it can't regress unnoticed.

If your own data has multiple *logically unrelated* sibling arrays, the
better long-term design is usually to explode them into **separate output
tables** (e.g. `reports`, `reports_products`, `reports_reactions`, joined
back on `report_number`) rather than flattening everything into one wide
table — closer to how BigQuery's own public GH Archive dataset is modeled.
This repo keeps it to one table for simplicity, but the validation job's
null-rate report is a natural place to notice you've hit this and split
things out.

## Possible extensions

- Split output by record/event type into separate tables (see above).
- Write directly to BigQuery via the `--output-format bigquery` flag
  (`spark-bigquery-connector` needs to be added — see comments in the
  submit command).
- Schedule `download_data.sh` + `submit_batch.sh` on a Cloud Scheduler +
  Cloud Function trigger for a recurring pipeline instead of a one-off run.

## License

MIT — see [LICENSE](LICENSE).

## Data attribution

Dataset from [openFDA](https://open.fda.gov/), a service of the U.S. Food
and Drug Administration. Per openFDA's terms: *"Do not rely on openFDA to
make decisions regarding medical care."*
