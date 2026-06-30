# Databricks notebook source
# MAGIC %md
# MAGIC # Row-filter UDF setup for the multi-source CDC demo
# MAGIC
# MAGIC The silver `customers` table is the merged target of three regional
# MAGIC CDC bronze flows (US / EU / APAC). Every per-flow `select_exp`
# MAGIC tags its rows with a constant `region` literal — so a row filter
# MAGIC on `region` is the natural privacy boundary for the merged table.
# MAGIC
# MAGIC The onboarding row references this UDF via `silver_row_filter`,
# MAGIC and DLT requires the function to exist BEFORE the pipeline first
# MAGIC creates the target table — otherwise CREATE TABLE … WITH ROW
# MAGIC FILTER fails. This notebook is the very first task in the demo
# MAGIC workflow and creates the UDF so the pipeline can attach the
# MAGIC filter when it mints the merged streaming table inside
# MAGIC `cdc_apply_changes_flows`.
# MAGIC
# MAGIC Predicate: members of the `admins` account group see every row;
# MAGIC everyone else sees only rows where `region IN ('US', 'EU')`.
# MAGIC The APAC rows from `customers_apac_silver` therefore land in the
# MAGIC merged silver table but are invisible to non-admin readers — a
# MAGIC straightforward demonstration that one row filter governs ALL N
# MAGIC flows feeding the same DLT streaming table (per the DLT mandate
# MAGIC that row filters bind at table-creation, not at write time).

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("silver_schema", "")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
silver_schema = dbutils.widgets.get("silver_schema")
print(f"uc_catalog_name : {uc_catalog_name}")
print(f"silver_schema   : {silver_schema}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog_name}.{silver_schema}")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION
        {uc_catalog_name}.{silver_schema}.region_filter(region STRING)
    RETURNS BOOLEAN
    RETURN
        is_account_group_member('admins')
        OR region IS NULL
        OR region IN ('US', 'EU')
""")

print(
    f"Created row-filter UDF: "
    f"{uc_catalog_name}.{silver_schema}.region_filter"
)
