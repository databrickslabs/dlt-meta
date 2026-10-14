# Databricks notebook source
import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = dbutils.widgets.get("uc_enabled").strip().lower() == "true"
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

def _sql_name(*parts):
    return ".".join(f"`{part}`" for part in parts)

# Assumption is that to get to this notebook Bronze and Silver completed successfully
log_list.append("Completed Bronze DLT Pipeline.")
log_list.append("Completed Silver DLT Pipeline.")

UC_TABLES = {
    _sql_name(uc_catalog_name, bronze_schema, "transactions"): 10002,
    _sql_name(uc_catalog_name, bronze_schema, "transactions_quarantine"): 6,
    _sql_name(uc_catalog_name, bronze_schema, "customers"): 51453,
    _sql_name(uc_catalog_name, bronze_schema, "customers_quarantine"): 256,
    _sql_name(uc_catalog_name, silver_schema, "transactions"): 8759,
    _sql_name(uc_catalog_name, silver_schema, "customers"): 73212,
}


log_list.append("Validating Lakeflow Spark Declarative Pipeline Bronze and Silver Table Counts...")
for table, counts in UC_TABLES.items():
    query = spark.sql(f"SELECT count(*) as cnt FROM {table}")
    cnt = query.collect()[0].cnt

    log_list.append(f"Validating Counts for Table {table}.")
    try:
        assert int(cnt) == counts
        log_list.append(f"Expected: {counts} Actual: {cnt}. Passed!")
    except AssertionError:
        log_list.append(f"Expected: {counts} Actual: {cnt}. Failed!")

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
