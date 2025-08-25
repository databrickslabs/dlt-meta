# Databricks notebook source
dbutils.widgets.text("base_path", "", "base_path")
dbutils.widgets.text("version", "", "version")
dbutils.widgets.text("source_catalog", "", "source_catalog")
dbutils.widgets.text("source_database", "", "source_database")
dbutils.widgets.text("source_table", "", "source_table")
base_path = dbutils.widgets.get("base_path")
version = dbutils.widgets.get("version")
source_catalog = dbutils.widgets.get("source_catalog")
source_database = dbutils.widgets.get("source_database")
source_table = dbutils.widgets.get("source_table")
print(f"base_path: {base_path}", f"version: {version}", f"source_catalog: {source_catalog}", f"source_database: {source_database}", f"source_table: {source_table}")

# COMMAND ----------
if version in ["2","3"]:
  base_path = base_path.replace("//","/")
  dbutils.fs.cp(f"{base_path}/incremental_snapshots/products/LOAD_{version}.csv",f"{base_path}/products/LOAD_{version}.csv",True)
  dbutils.fs.cp(f"{base_path}/incremental_snapshots/stores/LOAD_{version}.csv",f"{base_path}/stores/LOAD_{version}.csv",True)

# COMMAND ----------

df = spark.read.format("csv").option("header","true").load(f"{base_path}/products/LOAD_{version}.csv")
df.write.format("delta").mode("overwrite").saveAsTable(f"{source_catalog}.{source_database}.{source_table}")
