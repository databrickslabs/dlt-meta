# Databricks notebook source
import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
output_file_path = dbutils.widgets.get("output_file_path")
bronze_schema = dbutils.widgets.get("bronze_schema")
log_list = []

# Assumption is that to get to this notebook Bronze and Silver completed successfully
log_list.append("Completed Bronze Eventhub DLT Pipeline.")

TABLES = {
    f"{uc_catalog_name}.{bronze_schema}.bronze_{run_id}_iot": 20,
    f"{uc_catalog_name}.{bronze_schema}.bronze_{run_id}_iot_quarantine": 2,
}

log_list.append("Validating DLT EVenthub Bronze Table Counts...")
for table, counts in TABLES.items():
    query = spark.sql(f"SELECT count(*) as cnt FROM {table}")
    cnt = query.collect()[0].cnt

    log_list.append(f"Validating Counts for Table {table}.")
    try:
        assert int(cnt) >= counts
        log_list.append(f"Expected >= {counts} Actual: {cnt}. Passed!")
    except AssertionError:
        log_list.append(f"Expected {counts} Actual: {cnt}. Failed!")

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
