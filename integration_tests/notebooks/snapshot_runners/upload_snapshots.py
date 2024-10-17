# Databricks notebook source
dbutils.widgets.text("base_path", "", "base_path")
dbutils.widgets.text("version", "", "version")

base_path = dbutils.widgets.get("base_path")
version = dbutils.widgets.get("version")


# COMMAND ----------

domains = ["stores","products"]
base_path = base_path.replace("//","/")
for domain in domains:
  dbutils.fs.cp(f"{base_path}/incremental_snapshots/{domain}/LOAD_{version}.csv",f"{base_path}{domain}/LOAD_{version}.csv",True)
