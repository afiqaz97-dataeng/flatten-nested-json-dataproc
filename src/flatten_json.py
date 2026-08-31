"""
flatten_json.py

Recursively flattens nested JSON (structs and arrays-of-structs) read from
GCS into a flat tabular DataFrame, then writes the result to GCS (Parquet
by default) — or optionally straight to BigQuery.

Designed to run as a Dataproc Serverless (Batches) PySpark job.

Usage (see submit command in the accompanying notes):
    spark-submit flatten_json.py \
        --input gs://my-bucket/raw/events/*.json \
        --output gs://my-bucket/flattened/events \
        --output-format parquet \
        --explode-arrays true

To write to BigQuery instead, set --output-format bigquery and
--bq-table project.dataset.table (requires the spark-bigquery-connector
jar, see notes).
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, explode_outer
from pyspark.sql.types import ArrayType, StructType


def unwrap_root_array(df: DataFrame, field_name: str) -> DataFrame:
    """
    Many REST APIs (openFDA, GitHub, etc.) wrap the actual records in an
    envelope like {"meta": {...}, "results": [ {...}, {...} ]}. This pulls
    the named array field out to the top level, one row per record, and
    discards the envelope (meta/pagination info) so it isn't duplicated
    onto every row.
    """
    return df.select(explode(col(field_name)).alias("_root")).select("_root.*")


def flatten_df(df: DataFrame, separator: str = "_", explode_arrays: bool = True) -> DataFrame:
    """
    Recursively flattens all struct columns into top-level columns using
    `separator` as the join character (e.g. address.city -> address_city).

    If explode_arrays is True, array-of-struct (and plain array) columns
    are exploded into rows before flattening continues, so nested arrays
    are fully unrolled. If False, array columns are left as-is (untouched)
    and only struct nesting is flattened.

    WARNING — sibling arrays cause a cross-join, not a sum: if a record has
    two independent array fields at the same level (e.g. "products": [...]
    and "reactions": [...]), exploding both produces one row per
    (product, reaction) *pair*, not one row per product plus one row per
    reaction. A record with 2 products and 3 reactions becomes 6 rows, not
    5. This is correct, standard Spark explode behavior — but it means
    row counts on multi-array records can blow up unexpectedly. If your
    data has multiple sibling arrays that are logically unrelated, explode
    them into separate output tables instead of flattening them together
    into one wide table.
    """
    keep_going = True

    while keep_going:
        keep_going = False
        for field in df.schema.fields:
            f_name = field.name
            f_type = field.dataType

            if isinstance(f_type, StructType):
                # Expand struct into its child fields, prefixed with parent name.
                expanded = [
                    col(f"`{f_name}`.`{child.name}`").alias(f"{f_name}{separator}{child.name}")
                    for child in f_type.fields
                ]
                other_cols = [col(f"`{c}`") for c in df.columns if c != f_name]
                df = df.select(*other_cols, *expanded)
                keep_going = True
                break

            elif isinstance(f_type, ArrayType) and explode_arrays:
                # explode_outer keeps rows with empty/null arrays (as a null row)
                # instead of dropping them, which is usually what you want.
                df = df.withColumn(f_name, explode_outer(col(f"`{f_name}`")))
                keep_going = True
                break

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS path/glob to input JSON, e.g. gs://bucket/raw/*.json")
    parser.add_argument("--output", required=False, help="GCS output path (required unless --output-format=bigquery)")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "csv", "json", "bigquery"])
    parser.add_argument("--explode-arrays", default="true", choices=["true", "false"])
    parser.add_argument("--multiline", default="true", choices=["true", "false"],
                         help="Set true if each JSON file is one big pretty-printed object/array "
                              "rather than one JSON object per line (JSONL).")
    parser.add_argument("--bq-table", required=False, help="project.dataset.table, only used when --output-format=bigquery")
    parser.add_argument("--bq-temp-bucket", required=False, help="GCS bucket for BigQuery connector temp files")
    parser.add_argument("--root-array", required=False,
                         help="If the JSON is an API-style envelope (e.g. openFDA's "
                              "{\"meta\":..., \"results\": [...]}), name the array field here "
                              "(e.g. 'results') to unwrap it before flattening.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("flatten-nested-json").getOrCreate()

    df = (
        spark.read
        .option("multiLine", args.multiline == "true")
        .json(args.input)
    )

    print("== Original schema ==")
    df.printSchema()

    if args.root_array:
        df = unwrap_root_array(df, args.root_array)
        print(f"== Schema after unwrapping '{args.root_array}' ==")
        df.printSchema()

    flat_df = flatten_df(df, explode_arrays=(args.explode_arrays == "true"))

    print("== Flattened schema ==")
    flat_df.printSchema()

    if args.output_format == "bigquery":
        if not args.bq_table:
            raise ValueError("--bq-table is required when --output-format=bigquery")
        writer = flat_df.write.format("bigquery").option("table", args.bq_table)
        if args.bq_temp_bucket:
            writer = writer.option("temporaryGcsBucket", args.bq_temp_bucket)
        writer.mode("overwrite").save()
    else:
        if not args.output:
            raise ValueError("--output is required unless --output-format=bigquery")
        (
            flat_df.write
            .mode("overwrite")
            .format(args.output_format)
            .save(args.output)
        )

    spark.stop()


if __name__ == "__main__":
    main()
