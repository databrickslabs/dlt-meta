# Databricks notebook source
import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = eval(dbutils.widgets.get("uc_enabled"))
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

# Assumption is that to get to this notebook Bronze and Silver completed successfully
log_list.append("Completed Bronze DLT Pipeline.")
log_list.append("Completed Silver DLT Pipeline.")

UC_TABLES = {
    f"{uc_catalog_name}.{bronze_schema}.transactions": 10002,
    f"{uc_catalog_name}.{bronze_schema}.transactions_quarantine": 6,
    f"{uc_catalog_name}.{bronze_schema}.customers": 51453,
    f"{uc_catalog_name}.{bronze_schema}.customers_quarantine": 256,
    f"{uc_catalog_name}.{silver_schema}.transactions": 8759,
    f"{uc_catalog_name}.{silver_schema}.customers": 73212,
}

NON_UC_TABLES = {
    f"{bronze_schema}.transactions": 10002,
    f"{bronze_schema}.transactions_quarantine": 6,
    f"{bronze_schema}.customers": 51453,
    f"{bronze_schema}.customers_quarantine": 256,
    f"{silver_schema}.transactions": 8759,
    f"{silver_schema}.customers": 73212,
}

log_list.append("Validating Lakeflow Declarative Pipeline Bronze and Silver Table Counts...")
tables = UC_TABLES if uc_enabled else NON_UC_TABLES
for table, counts in tables.items():
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
