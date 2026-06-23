# Databricks notebook source
# MAGIC %md
# MAGIC # Pre-pipeline: create the row-filter UDF for the customers flow
# MAGIC
# MAGIC The cloudfiles integration suite declares
# MAGIC `bronze_row_filter` / `silver_row_filter` on the `customers` flow that
# MAGIC reference `<uc_catalog_name>.<bronze_schema>.customer_op_filter(operation)`.
# MAGIC The pipeline cannot create the bronze/silver target tables until this UDF
# MAGIC exists, so this runner notebook is wired as a pre-step before the bronze
# MAGIC pipeline task in `run_integration_tests.py::create_workflow_spec`.
# MAGIC
# MAGIC Predicate (reader-time):
# MAGIC   * admins (CI service principal) → all rows
# MAGIC   * everyone else → only rows whose `operation` is `APPEND` or `UPDATE`
# MAGIC     (DELETE rows are hidden)
# MAGIC
# MAGIC Because the integration test runs as a workspace admin, all 51453/73212
# MAGIC bronze/silver `customers` rows remain visible -- the existing count
# MAGIC assertions in `validate.py` keep passing.

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("bronze_schema", "")

uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")

if not uc_catalog_name or not bronze_schema:
    raise ValueError(
        "Both `uc_catalog_name` and `bronze_schema` widgets must be supplied."
    )

# COMMAND ----------

# Schemas are pre-created by initialize_uc_resources(); the catalog is supplied
# by the caller and must already exist (see README). We only need to publish
# the UDF here.
spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION
        {uc_catalog_name}.{bronze_schema}.customer_op_filter(op STRING)
    RETURNS BOOLEAN
    RETURN
        is_account_group_member('admins')
        OR op IS NULL
        OR op IN ('APPEND', 'UPDATE')
    """
)

print(
    f"Row filter UDF ready at "
    f"`{uc_catalog_name}.{bronze_schema}.customer_op_filter`."
)
