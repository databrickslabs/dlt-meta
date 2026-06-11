# Databricks notebook source
"""Validate the multi-source AUTO CDC demo (issue #294).

We assert two layers of correctness:

1. Each regional bronze CDC table received the raw rows from its own
   source folder. We know exact counts from the seed JSON files
   committed under ``demo/resources/data/multi_source_cdc/``.

2. The unified silver ``customers`` table reflects the CDC merge result
   of all three regional bronze tables: SCD-1 + apply-as-deletes means
   the live row count equals the count of distinct customer_ids that
   were not deleted in their latest event. With the seed data:

     * US: 3 customers seeded, 1 deleted (us-002) -> 2 live US rows
     * EU: 3 customers seeded, 1 deleted (eu-003) -> 2 live EU rows
     * APAC: 3 customers seeded, 1 deleted (apac-003) -> 2 live APAC rows
     * Total live silver rows = 6

   We also verify the per-region breakdown so the per-flow
   ``select_exp`` normalization (each flow tags its rows with a
   constant ``region`` literal) actually ran.
"""

import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

log_list.append("Completed Bronze DLT Pipeline.")
log_list.append("Completed Silver DLT Pipeline (multi-source AUTO CDC).")

# Bronze: raw CDC events landed per region. The counts here are the
# total event count (including UPDATE/DELETE rows), not distinct
# customer count.
BRONZE_TABLES = {
    f"{uc_catalog_name}.{bronze_schema}.customers_us_cdc": 5,
    f"{uc_catalog_name}.{bronze_schema}.customers_eu_cdc": 5,
    f"{uc_catalog_name}.{bronze_schema}.customers_apac_cdc": 5,
}

log_list.append("Validating bronze regional CDC table counts...")
for table, expected in BRONZE_TABLES.items():
    actual = spark.sql(f"SELECT count(*) AS cnt FROM {table}").collect()[0].cnt
    log_list.append(f"Validating counts for table {table}.")
    try:
        assert int(actual) == expected
        log_list.append(f"Expected: {expected} Actual: {actual}. Passed!")
    except AssertionError:
        log_list.append(f"Expected: {expected} Actual: {actual}. Failed!")

# Silver: post-CDC unified customers table. SCD-1 + apply-as-deletes.
silver_customers = f"{uc_catalog_name}.{silver_schema}.customers"
log_list.append(f"Validating unified silver customers table {silver_customers}.")

total = spark.sql(f"SELECT count(*) AS cnt FROM {silver_customers}").collect()[0].cnt
try:
    assert int(total) == 6
    log_list.append(f"Total live silver rows. Expected: 6 Actual: {total}. Passed!")
except AssertionError:
    log_list.append(f"Total live silver rows. Expected: 6 Actual: {total}. Failed!")

# Per-region breakdown — proves the per-flow select_exp ran (each flow
# adds a constant ``region`` column that's only set by that flow).
PER_REGION_EXPECTED = {"US": 2, "EU": 2, "APAC": 2}
log_list.append("Validating per-region live counts (proves per-flow select_exp ran)...")
region_counts = {
    row["region"]: row["cnt"]
    for row in spark.sql(
        f"SELECT region, count(*) AS cnt FROM {silver_customers} GROUP BY region"
    ).collect()
}
for region, expected in PER_REGION_EXPECTED.items():
    actual = region_counts.get(region, 0)
    try:
        assert int(actual) == expected
        log_list.append(
            f"region={region}. Expected: {expected} Actual: {actual}. Passed!"
        )
    except AssertionError:
        log_list.append(
            f"region={region}. Expected: {expected} Actual: {actual}. Failed!"
        )

# Verify that customer_id values from all 3 regions landed in the
# unified silver table. We picked one surviving customer per region in
# the seed data so this is a sharp identity check.
expected_ids = {"us-001", "us-003", "eu-001", "eu-002", "apac-001", "apac-002"}
actual_ids = {
    row["customer_id"]
    for row in spark.sql(
        f"SELECT customer_id FROM {silver_customers}"
    ).collect()
}
try:
    assert expected_ids == actual_ids
    log_list.append(f"customer_id set match. Passed! ids={sorted(actual_ids)}")
except AssertionError:
    log_list.append(
        f"customer_id set MISMATCH. "
        f"Expected: {sorted(expected_ids)} Actual: {sorted(actual_ids)}. Failed!"
    )

# Row-filter attachment check: confirm the merged silver `customers`
# table has the `region_filter` UDF attached via `silver_row_filter`.
# We use `information_schema.row_filters` (UC system view) which
# exposes `filter_name` (UDF FQN) and `target_columns` for every
# filter currently bound to a table.
#
# This is the multi-source-CDC analogue of the cloudfiles validate
# step at integration_tests/notebooks/cloudfile_runners/validate.py.
# It proves the row filter survives the full path:
#   onboarding spec (silver_row_filter)
#     -> populate_dataflow_spec
#     -> DataflowPipeline.cdc_apply_changes_flows
#     -> create_streaming_table(row_filter=...)
#     -> dp.create_streaming_table
#     -> UC table-creation
# breaking the chain at any step will surface here as a missing or
# wrong filter_name.
log_list.append(
    "Validating silver row_filter attachment on merged customers table..."
)
expected_filter = f"{uc_catalog_name}.{silver_schema}.region_filter"
expected_target_columns = ["region"]
filters_df = spark.sql(f"""
    SELECT filter_name, target_columns
    FROM {uc_catalog_name}.information_schema.row_filters
    WHERE table_catalog = '{uc_catalog_name}'
      AND table_schema  = '{silver_schema}'
      AND table_name    = 'customers'
""")
filter_rows = filters_df.collect()
try:
    assert len(filter_rows) == 1, (
        f"expected exactly 1 row_filter on {silver_customers}, got "
        f"{len(filter_rows)}"
    )
    actual_filter = filter_rows[0]["filter_name"]
    actual_target = list(filter_rows[0]["target_columns"])
    assert actual_filter == expected_filter, (
        f"filter_name mismatch. Expected: {expected_filter} "
        f"Actual: {actual_filter}"
    )
    assert actual_target == expected_target_columns, (
        f"target_columns mismatch. Expected: {expected_target_columns} "
        f"Actual: {actual_target}"
    )
    log_list.append(
        f"row_filter attached. filter={actual_filter} "
        f"target_columns={actual_target}. Passed!"
    )
except AssertionError as e:
    log_list.append(f"row_filter attachment check FAILED: {e}")

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
