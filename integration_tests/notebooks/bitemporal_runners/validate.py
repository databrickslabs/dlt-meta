# Databricks notebook source
"""Integration test validation for bitemporal AUTO CDC (issue #359).

The seed file ``integration_tests/resources/data/bitemporal/customers_cdc/``
carries 4 CDC events for 2 keys, including one LATE-ARRIVING correction:
``c-002`` is corrected at business time 11:30 but only ingested at 14:00.
With ``scd_type: bitemporal`` + ``system_sequence_by: ingest_ts`` the
target must track validity on two time axes (``__START_AT``/``__END_AT``
for business time from ``sequence_by``, ``__SYSTEM_START_AT``/
``__SYSTEM_END_AT`` for system time from ``system_sequence_by``).

Assertions:

1. The target schema carries all four bitemporal columns.
2. Exactly 6 version rows exist (3 per key: superseded belief, corrected
   history, current).
3. Exactly 2 rows are current on both axes, and they are the latest
   business-time values (``alice_v2``, ``bob_corrected``).
4. An as-of-system-time query at 13:00 still returns the UNCORRECTED
   belief for ``c-002`` (``bob``) — what the system believed before the
   late event arrived at 14:00.
5. The corrected history row exists: ``c-002`` valid in business time
   [11:00 -> 11:30), known only from system time 14:00 onward.
"""

import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = dbutils.widgets.get("uc_enabled").strip().lower() == "true"
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

log_list.append("Completed Bronze Lakeflow Spark Declarative Pipeline (bitemporal AUTO CDC).")


def _qualify(schema, table):
    return (
        f"{uc_catalog_name}.{schema}.{table}"
        if uc_enabled
        else f"{schema}.{table}"
    )


target = _qualify(bronze_schema, "customers_bitemporal")

log_list.append(f"Validating bitemporal schema of {target}...")
columns = {f.name for f in spark.table(target).schema.fields}
expected_bt_columns = {"__START_AT", "__END_AT", "__SYSTEM_START_AT", "__SYSTEM_END_AT"}
try:
    assert expected_bt_columns.issubset(columns)
    log_list.append(f"Bitemporal columns {sorted(expected_bt_columns)} present. Passed!")
except AssertionError:
    log_list.append(
        f"Bitemporal columns missing. Expected: {sorted(expected_bt_columns)} "
        f"Actual: {sorted(columns)}. Failed!"
    )

log_list.append("Validating total version-row count...")
total = spark.sql(f"SELECT count(*) AS cnt FROM {target}").collect()[0].cnt
try:
    assert int(total) == 6
    log_list.append(f"Total version rows. Expected: 6 Actual: {total}. Passed!")
except AssertionError:
    log_list.append(f"Total version rows. Expected: 6 Actual: {total}. Failed!")

log_list.append("Validating current rows (both time axes open)...")
current_names = {
    row["name"]
    for row in spark.sql(
        f"SELECT name FROM {target} "
        "WHERE __END_AT IS NULL AND __SYSTEM_END_AT IS NULL"
    ).collect()
}
expected_current = {"alice_v2", "bob_corrected"}
try:
    assert current_names == expected_current
    log_list.append(f"Current rows. Expected: {sorted(expected_current)} Passed!")
except AssertionError:
    log_list.append(
        f"Current rows MISMATCH. Expected: {sorted(expected_current)} "
        f"Actual: {sorted(current_names)}. Failed!"
    )

log_list.append("Validating as-of-system-time query (pre-correction belief)...")
belief_rows = spark.sql(
    f"SELECT name FROM {target} "
    "WHERE id = 'c-002' "
    "AND __SYSTEM_START_AT <= TIMESTAMP'2024-01-15 13:00:00+00:00' "
    "AND (__SYSTEM_END_AT IS NULL OR __SYSTEM_END_AT > TIMESTAMP'2024-01-15 13:00:00+00:00') "
    "AND __END_AT IS NULL"
).collect()
try:
    assert len(belief_rows) == 1 and belief_rows[0]["name"] == "bob"
    log_list.append(
        "As-of system time 13:00 the believed-current value for c-002 is 'bob'. Passed!"
    )
except AssertionError:
    log_list.append(
        f"As-of system time 13:00 belief for c-002. Expected: ['bob'] "
        f"Actual: {[r['name'] for r in belief_rows]}. Failed!"
    )

log_list.append("Validating late-arriving correction rewrote business history...")
corrected = spark.sql(
    f"SELECT count(*) AS cnt FROM {target} "
    "WHERE id = 'c-002' AND name = 'bob' "
    "AND __START_AT = TIMESTAMP'2024-01-15 11:00:00+00:00' "
    "AND __END_AT = TIMESTAMP'2024-01-15 11:30:00+00:00' "
    "AND __SYSTEM_START_AT = TIMESTAMP'2024-01-15 14:00:00+00:00' "
    "AND __SYSTEM_END_AT IS NULL"
).collect()[0].cnt
try:
    assert int(corrected) == 1
    log_list.append(
        "Corrected history row (c-002 business [11:00 -> 11:30) known from 14:00). Passed!"
    )
except AssertionError:
    log_list.append(
        f"Corrected history row for c-002. Expected: 1 Actual: {corrected}. Failed!"
    )

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
