# Databricks notebook source
# MAGIC %md
# MAGIC # Seed delta source tables for the dlt-meta `delta` demo scenario
# MAGIC
# MAGIC This notebook is uploaded + executed by `demo/launch_dab_template_demo.py`
# MAGIC during STAGE 4 of the `delta` scenario. It reads CSV files from a UC
# MAGIC volume (uploaded earlier in STAGE 2) and materializes them as Delta
# MAGIC tables under a target UC schema. The downstream `recipes/from_uc.py`
# MAGIC then discovers those tables and registers them as bronze/silver flows.
# MAGIC
# MAGIC ## Parameters
# MAGIC | name              | description                                                  |
# MAGIC | ----------------- | ------------------------------------------------------------ |
# MAGIC | `source_catalog`  | UC catalog the seeded tables live in.                        |
# MAGIC | `source_schema`   | UC schema (must exist; launcher pre-creates it).             |
# MAGIC | `input_volume_path` | UC-volume base path containing `<table>/*.csv` subdirs.    |
# MAGIC | `tables`          | Comma-separated list of table names to seed.                 |
# MAGIC
# MAGIC The notebook is intentionally idempotent: each run uses
# MAGIC `mode=overwrite` + `overwriteSchema=true` so reseeding produces a
# MAGIC clean, repeatable starting point for downstream pipelines.

# COMMAND ----------

dbutils.widgets.text("source_catalog", "")
dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("input_volume_path", "")
dbutils.widgets.text("tables", "customers,transactions,products")

source_catalog = dbutils.widgets.get("source_catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
input_volume_path = dbutils.widgets.get("input_volume_path").strip().rstrip("/")
tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]

if not (source_catalog and source_schema and input_volume_path and tables):
    raise ValueError(
        f"Missing required parameter(s). Got source_catalog={source_catalog!r}, "
        f"source_schema={source_schema!r}, input_volume_path={input_volume_path!r}, "
        f"tables={tables!r}"
    )

print(f"source_catalog    = {source_catalog}")
print(f"source_schema     = {source_schema}")
print(f"input_volume_path = {input_volume_path}")
print(f"tables            = {tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ensure the source schema exists
# MAGIC
# MAGIC The launcher already creates this when `--create-missing-uc` is set,
# MAGIC but we re-issue the DDL here so the notebook is also runnable
# MAGIC standalone.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{source_catalog}`.`{source_schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Materialize one Delta table per `<table>` subdir
# MAGIC
# MAGIC Reads every CSV under `<input_volume_path>/<table>/` (header row +
# MAGIC inferred schema) and writes it as a managed Delta table at
# MAGIC `<source_catalog>.<source_schema>.<table>`.

# COMMAND ----------

results = []
for table in tables:
    src = f"{input_volume_path}/{table}"
    target = f"`{source_catalog}`.`{source_schema}`.`{table}`"
    print(f"\n--- {table} ---")
    print(f"reading from {src}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(src)
    )
    row_count = df.count()
    print(f"loaded {row_count} row(s); writing to {target}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    results.append((table, row_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Summary
# MAGIC
# MAGIC The launcher's STAGE 4 banner picks this up via the run output so
# MAGIC the demo log clearly shows which tables were seeded with how many rows.

# COMMAND ----------

import json
summary = {
    "source_catalog": source_catalog,
    "source_schema": source_schema,
    "input_volume_path": input_volume_path,
    "tables": [{"name": t, "row_count": rc} for t, rc in results],
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
