# Databricks notebook source
# MAGIC %md
# MAGIC # Row-filter UDF setup
# MAGIC
# MAGIC The bronze and silver onboarding rows reference a UC row-filter
# MAGIC UDF via `bronze_row_filter` / `silver_row_filter`. UC requires
# MAGIC the function to exist BEFORE the pipeline first creates the
# MAGIC target table — otherwise CREATE TABLE fails. This notebook is
# MAGIC the very first task in the demo workflow and creates the UDF
# MAGIC so the pipeline can attach the filter.
# MAGIC
# MAGIC Predicate: members of the `admins` account group see every
# MAGIC row; everyone else sees only rows where `region IN ('US', 'UK')`.

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("bronze_schema", "")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
print(f"uc_catalog_name : {uc_catalog_name}")
print(f"bronze_schema   : {bronze_schema}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog_name}.{bronze_schema}")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION
        {uc_catalog_name}.{bronze_schema}.region_filter(region STRING)
    RETURNS BOOLEAN
    RETURN
        is_account_group_member('admins')
        OR region IS NULL
        OR region IN ('US', 'UK')
""")

print(
    f"Created row-filter UDF: "
    f"{uc_catalog_name}.{bronze_schema}.region_filter"
)
