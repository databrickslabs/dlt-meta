# Databricks notebook source
dbfs_tmp_path = dbutils.widgets.get("dbfs_tmp_path")

# COMMAND ----------

source_base_path = f"{dbfs_tmp_path}/demo/resources/incremental_data/"
target_base_path = f"{dbfs_tmp_path}/demo/resources/data/"
domains = ["customers","transactions","stores","products"]
for domain in domains:
  dbutils.fs.cp(f"{source_base_path}{domain}/",f"{target_base_path}{domain}/",True)