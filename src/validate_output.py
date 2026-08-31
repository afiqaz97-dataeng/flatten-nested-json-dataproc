"""
validate_output.py

Sanity-checks a flatten_json.py run by comparing the raw input against the
flattened output:
  - counts raw top-level records (or records inside --root-array, if used)
  - counts flattened output rows
  - if arrays were exploded, one raw record can map to many output rows,
    so this reports the expansion ratio rather than asserting equality
  - checks the flattened output for columns that are entirely null (a sign
    something didn't flatten the way you expected)

Run as a second Dataproc Serverless batch, or locally against small samples.

Usage:
    spark-submit validate_output.py \
        --raw-input gs://bucket/raw/openfda/*.json \
        --flattened-input gs://bucket/flattened/openfda_food_events \
        --root-array results \
        --multiline true
"""

import argparse

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, count, when # type: ignore
from pyspark.sql.functions import explode # type: ignore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-input", required=True)
    parser.add_argument("--flattened-input", required=True)
    parser.add_argument("--root-array", required=False)
    parser.add_argument("--multiline", default="true", choices=["true", "false"])
    args = parser.parse_args()

    spark = SparkSession.builder.appName("validate-flatten-output").getOrCreate()

    raw = spark.read.option("multiLine", args.multiline == "true").json(args.raw_input)
    if args.root_array:
        raw_count = raw.select(explode(col(args.root_array)).alias("_r")).count()
    else:
        raw_count = raw.count()

    flat = spark.read.parquet(args.flattened_input)
    flat_count = flat.count()

    print(f"Raw record count:        {raw_count}")
    print(f"Flattened row count:     {flat_count}")
    if raw_count > 0:
        print(f"Expansion ratio:         {flat_count / raw_count:.2f}x "
              f"(>1 means arrays were exploded into multiple rows per record, as expected)")

    print("\n== Null-rate per column in flattened output ==")
    total = flat_count if flat_count > 0 else 1
    null_counts = flat.select([
        count(when(col(c).isNull(), c)).alias(c) for c in flat.columns
    ]).collect()[0].asDict()

    for column, null_count in sorted(null_counts.items(), key=lambda kv: -kv[1]):
        pct = 100.0 * null_count / total
        flag = "  <-- fully null, check this" if pct == 100.0 else ""
        print(f"  {column:40s} {pct:6.2f}% null{flag}")

    spark.stop()


if __name__ == "__main__":
    main()
