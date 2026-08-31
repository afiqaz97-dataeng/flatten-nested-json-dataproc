from flatten_json import flatten_df, unwrap_root_array


def test_unwrap_root_array_explodes_results(spark, fixture_path):
    raw = spark.read.option("multiLine", True).json(fixture_path)
    unwrapped = unwrap_root_array(raw, "results")

    # Two records in the fixture's "results" array -> two rows, and the
    # "meta" envelope should be gone (not duplicated onto every row).
    assert unwrapped.count() == 2
    assert "meta" not in unwrapped.columns
    assert "report_number" in unwrapped.columns


def test_flatten_df_expands_struct_columns(spark, fixture_path):
    raw = spark.read.option("multiLine", True).json(fixture_path)
    unwrapped = unwrap_root_array(raw, "results")
    flat = flatten_df(unwrapped, explode_arrays=True)

    # consumer.age / consumer.gender should become top-level columns
    assert "consumer_age" in flat.columns
    assert "consumer_gender" in flat.columns
    assert "consumer" not in flat.columns


def test_flatten_df_explodes_array_of_structs(spark, fixture_path):
    raw = spark.read.option("multiLine", True).json(fixture_path)
    unwrapped = unwrap_root_array(raw, "results")
    flat = flatten_df(unwrapped, explode_arrays=True)

    # IMPORTANT / non-obvious: each report has TWO sibling arrays
    # ("products" and "reactions"), and flatten_df explodes them
    # independently and sequentially. That produces a cross-join, not a
    # simple sum: report 1 has 2 products x 2 reactions = 4 rows, report 2
    # has 1 product x 1 reaction = 1 row, for 5 rows total (not 2+1=3).
    # This is standard Spark explode behavior, not a bug in flatten_df —
    # but it's exactly the kind of row-count blow-up you must account for
    # before trusting aggregate counts on multi-array nested data. See the
    # "Multiple sibling arrays" note in the README.
    assert flat.count() == 5
    assert "products_name_brand" in flat.columns
    assert "products_industry_code" in flat.columns


def test_flatten_df_preserves_arrays_when_disabled(spark, fixture_path):
    raw = spark.read.option("multiLine", True).json(fixture_path)
    unwrapped = unwrap_root_array(raw, "results")
    flat = flatten_df(unwrapped, explode_arrays=False)

    # With explode disabled, row count stays at 2 (no array expansion),
    # and "products" remains a single array column rather than being
    # expanded into product-level columns.
    assert flat.count() == 2
    assert "products" in flat.columns
