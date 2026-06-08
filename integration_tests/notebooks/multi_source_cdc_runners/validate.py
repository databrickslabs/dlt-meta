# Databricks notebook source
"""Integration test validation for multi-source AUTO CDC (issue #294).

Mirrors the demo's validation contract (same seed data, same expected
counts) but writes a structured pass/fail CSV the integration-test
harness downloads at the end of the run, matching the existing
``cloudfile_runners/validate.py`` signature so the rest of the harness
(``download_test_results`` etc.) keeps working unchanged.

Assertions:

1. Per-region bronze CDC table received exactly the raw rows seeded
   under ``integration_tests/resources/data/multi_source_cdc/<region>/``.
2. Unified silver ``customers`` table reflects the SCD-1 + apply-as-
   deletes result of merging all 3 regional bronze tables. With the
   seed data: 3 customers per region, 1 delete per region -> 6 live
   silver rows.
3. Per-region row breakdown matches expectation. This proves the per-
   flow ``select_exp`` actually ran (each flow tags rows with a
   constant ``region`` literal that only that flow produces).
4. The exact set of surviving ``customer_id`` values matches the
   subset of seeded ids whose latest event was not a DELETE.
"""

import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = dbutils.widgets.get("uc_enabled").strip().lower() == "true"
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

log_list.append("Completed Bronze Lakeflow Spark Declarative Pipeline.")
log_list.append(
    "Completed Silver Lakeflow Spark Declarative Pipeline (multi-source AUTO CDC)."
)


def _qualify(schema, table):
    return (
        f"{uc_catalog_name}.{schema}.{table}"
        if uc_enabled
        else f"{schema}.{table}"
    )


BRONZE_TABLES = {
    _qualify(bronze_schema, "customers_us_cdc"): 5,
    _qualify(bronze_schema, "customers_eu_cdc"): 5,
    _qualify(bronze_schema, "customers_apac_cdc"): 5,
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

silver_customers = _qualify(silver_schema, "customers")
log_list.append(
    f"Validating unified silver customers table {silver_customers}."
)
total = (
    spark.sql(f"SELECT count(*) AS cnt FROM {silver_customers}")
    .collect()[0]
    .cnt
)
try:
    assert int(total) == 6
    log_list.append(
        f"Total live silver rows. Expected: 6 Actual: {total}. Passed!"
    )
except AssertionError:
    log_list.append(
        f"Total live silver rows. Expected: 6 Actual: {total}. Failed!"
    )

PER_REGION_EXPECTED = {"US": 2, "EU": 2, "APAC": 2}
log_list.append(
    "Validating per-region live counts (proves per-flow select_exp ran)..."
)
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

expected_ids = {
    "us-001",
    "us-003",
    "eu-001",
    "eu-002",
    "apac-001",
    "apac-002",
}
actual_ids = {
    row["customer_id"]
    for row in spark.sql(
        f"SELECT customer_id FROM {silver_customers}"
    ).collect()
}
try:
    assert expected_ids == actual_ids
    log_list.append(
        f"customer_id set match. Passed! ids={sorted(actual_ids)}"
    )
except AssertionError:
    log_list.append(
        f"customer_id set MISMATCH. "
        f"Expected: {sorted(expected_ids)} Actual: {sorted(actual_ids)}. Failed!"
    )

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
