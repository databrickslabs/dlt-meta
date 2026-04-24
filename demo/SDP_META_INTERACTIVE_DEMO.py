# Databricks notebook source
# MAGIC %md
# MAGIC # SDP-META (formerly DLT-META) - Interactive Demo
# MAGIC
# MAGIC **SDP-META** is a metadata-driven framework for
# MAGIC [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/index.html).
# MAGIC It automates Bronze and Silver data pipelines by leveraging metadata
# MAGIC recorded in an onboarding JSON/YAML file.
# MAGIC A single generic pipeline reads the **DataflowSpec** metadata and
# MAGIC orchestrates all data processing workloads.
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC ┌──────────────────┐    ┌───────────────────┐    ┌────────────────────────┐
# MAGIC │  Onboarding File │───>│  OnboardDataflow   │───>│  DataflowSpec Tables   │
# MAGIC │  (JSON / YAML)   │    │  spec API          │    │  (Bronze/Silver Delta) │
# MAGIC └──────────────────┘    └───────────────────┘    └──────────┬─────────────┘
# MAGIC                                                             │
# MAGIC                                                             ▼
# MAGIC                                                ┌────────────────────────┐
# MAGIC                                                │  Generic Lakeflow      │
# MAGIC                                                │  Declarative Pipeline  │
# MAGIC                                                │  (reads DataflowSpec,  │
# MAGIC                                                │  creates Bronze/Silver)│
# MAGIC                                                └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### GitHub Resources
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | **Source Code** | [sdp_meta](https://github.com/databrickslabs/dlt-meta/tree/main/src/databricks/labs/sdp_meta) |
# MAGIC | **Onboarding Template (JSON / YAML)** | [json/onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/onboarding.template) / [yml/onboarding.template.yml](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/onboarding.template.yml) |
# MAGIC | **Append Flow Template (JSON / YAML)** | [json/cloudfiles-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template) / [yml/cloudfiles-onboarding.template.yml](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/cloudfiles-onboarding.template.yml) |
# MAGIC | **Snapshot Template (JSON / YAML)** | [json/snapshot-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/snapshot-onboarding.template) / [yml/snapshot-onboarding.template.yml](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/snapshot-onboarding.template.yml) |
# MAGIC | **Sink Template (JSON / YAML)** | [json/kafka-sink-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/kafka-sink-onboarding.template) / [yml/kafka-sink-onboarding.template.yml](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/kafka-sink-onboarding.template.yml) |
# MAGIC | **Silver Transformations (JSON / YAML)** | [json/silver_transformations.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/silver_transformations.json) / [yml/silver_transformations.yml](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/silver_transformations.yml) |
# MAGIC | **Data Quality (DQE)** | [json/dqe/](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/json/dqe) / [yml/dqe/](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/yml/dqe) |
# MAGIC | **Documentation** | [databrickslabs.github.io/dlt-meta](https://databrickslabs.github.io/dlt-meta/) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Prerequisites: Fill in Your Details
# MAGIC
# MAGIC Set the parameters below then run **all cells in order**.
# MAGIC Everything else is created automatically.
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | **Git Branch** | Branch to install SDP-META from |
# MAGIC | **UC Catalog Name** | Unity Catalog catalog for the demo |
# MAGIC | **UC Schema Name** | Schema within the catalog |
# MAGIC | **Data Source** | `dbdatagen` (generate synthetic data) or `github` (download from repo) |

# COMMAND ----------

dbutils.widgets.text(
    name="git_branch",
    defaultValue="main",
    label="Git Branch"
)
dbutils.widgets.text(
    name="uc_catalog_name",
    defaultValue="sdp_meta_demo",
    label="UC Catalog Name"
)
dbutils.widgets.text(
    name="uc_schema_name",
    defaultValue="retail_data",
    label="UC Schema Name"
)
dbutils.widgets.dropdown(
    name="data_source",
    defaultValue="dbdatagen",
    choices=["dbdatagen", "github"],
    label="Data Source"
)
dbutils.widgets.dropdown(
    name="onboarding_format",
    defaultValue="json",
    choices=["json", "yml"],
    label="Onboarding File Format"
)

# COMMAND ----------

git_branch = dbutils.widgets.get("git_branch")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
uc_schema_name = dbutils.widgets.get("uc_schema_name")
data_source = dbutils.widgets.get("data_source")
onboarding_format = dbutils.widgets.get("onboarding_format")

print(f"Git Branch         : {git_branch}")
print(f"UC Catalog         : {uc_catalog_name}")
print(f"UC Schema          : {uc_schema_name}")
print(f"Data Source        : {data_source}")
print(f"Onboarding Format  : {onboarding_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install SDP-META from Git

# COMMAND ----------

git_url = (
    f"git+https://github.com/databrickslabs/"
    f"dlt-meta.git@{dbutils.widgets.get('git_branch')}"
)
# dbldatagen is only needed for the "dbdatagen" data source option
extra_packages = " dbldatagen" if data_source == "dbdatagen" else ""
packages = git_url + extra_packages
%pip install $packages  # noqa: E999
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Summary
# MAGIC
# MAGIC **Use Case:** MySQL → AWS DMS Replication → S3 (CSV)
# MAGIC → Bronze (Lakeflow Spark Declarative Pipeline)
# MAGIC → Silver (CDC / SCD Type 2)
# MAGIC
# MAGIC | Stage | What You'll Do |
# MAGIC |-------|---------------|
# MAGIC | **1** | Setup — catalog, schema, volume, create resources |
# MAGIC | **2** | Onboard **Customers** & **Transactions** feeds |
# MAGIC | **3** | Create pipeline runner & Lakeflow Spark Declarative Pipeline |
# MAGIC | **4** | Validate initial load (Bronze + Silver tables) |
# MAGIC | **5** | Add **Products** & **Stores** feeds |
# MAGIC | **6** | Push incremental CDC data and re-run |
# MAGIC | **7** | Validate incremental results (SCD Type 2 history) |
# MAGIC | **8** | **Append Flow** — multi-source ingestion with file metadata |
# MAGIC | **9** | **Apply Changes From Snapshot** — SCD Type 1 & 2 |
# MAGIC | **10** | **DLT Sink** — write to external delta table |
# MAGIC
# MAGIC ### Features Demonstrated
# MAGIC - Metadata-driven onboarding (JSON or YAML → DataflowSpec tables, controlled by the `Onboarding File Format` widget)
# MAGIC - CloudFiles (Autoloader) ingestion
# MAGIC - CDC with `apply_changes` (SCD Type 2)
# MAGIC - Data quality (`expect_or_drop`, `expect_or_quarantine`)
# MAGIC - Quarantine tables for bad data
# MAGIC - Liquid clustering (`cluster_by`, `cluster_by_auto`)
# MAGIC - Silver transformations (column selection, expressions)
# MAGIC - Adding new feeds without modifying the pipeline
# MAGIC - Incremental processing
# MAGIC - **Append Flow** — `dp.append_flow` for multi-source → same target
# MAGIC - **File Metadata** — `_metadata.file_name`, `_metadata.file_path`
# MAGIC - **Apply Changes From Snapshot** — snapshot-based SCD Type 1 & 2
# MAGIC - **Pipeline Sink** — `dp.create_sink` to write to external delta

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1: Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Re-read widget values (after Python restart)

# COMMAND ----------

import csv
import json
import yaml
import os
import time

from pyspark.sql import Row

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    NotebookLibrary,
    PipelineLibrary,
)
from databricks.sdk.service.workspace import (
    ExportFormat, Language,
)

git_branch = dbutils.widgets.get("git_branch")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
uc_schema_name = dbutils.widgets.get("uc_schema_name")
data_source = dbutils.widgets.get("data_source")
onboarding_format = dbutils.widgets.get("onboarding_format")

w = WorkspaceClient()


def run_pipeline_and_wait(w, pipeline_id, label=""):
    """Start a pipeline update and block until it completes."""
    resp = w.pipelines.start_update(pipeline_id=pipeline_id)
    update_id = resp.update_id
    host = w.config.host.rstrip("/")
    pipeline_url = f"{host}/pipelines/{pipeline_id}/updates/{update_id}"
    tag = f" ({label})" if label else ""
    print(f"Pipeline started{tag} — update_id: {update_id}")
    print(f"  URL: {pipeline_url}")
    while True:
        info = w.pipelines.get_update(
            pipeline_id=pipeline_id, update_id=update_id
        )
        state = info.update.state.value
        print(f"  state: {state}")
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(20)
    if state != "COMPLETED":
        raise RuntimeError(
            f"Pipeline ended with state: {state}. "
            f"Check the pipeline UI for details: {pipeline_url}"
        )
    print("Pipeline completed successfully.")


def _write_onboarding(data, path):
    with open(path, "w") as fh:
        if path.endswith(".yml") or path.endswith(".yaml"):
            yaml.dump(data, fh, default_flow_style=False, allow_unicode=True)
        else:
            json.dump(data, fh, indent=2)


def _read_onboarding(path):
    with open(path, "r") as fh:
        if path.endswith(".yml") or path.endswith(".yaml"):
            return yaml.safe_load(fh)
        else:
            return json.load(fh)


# Extension used for every silver-transformation and DQE config file. Driven
# by the `onboarding_format` widget so the entire demo (onboarding spec +
# referenced silver/DQ files) stays in one consistent format.
conf_ext = "yml" if onboarding_format in ("yml", "yaml") else "json"


def _write_conf(data, path):
    """Serialize a config dict/list as YAML or JSON based on the path suffix."""
    with open(path, "w") as fh:
        if path.endswith((".yml", ".yaml")):
            yaml.dump(data, fh, default_flow_style=False, allow_unicode=True)
        else:
            json.dump(data, fh, indent=4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Catalog, Schema, and Volume

# COMMAND ----------

bronze_schema = f"{uc_schema_name}_bronze"
silver_schema = f"{uc_schema_name}_silver"
# DLT direct publishing mode requires a pipeline-level target schema.
# This schema is a placeholder only — every table sets its own schema
# via DataflowSpec so nothing is ever written here.
pipeline_target_schema = f"{uc_schema_name}_pipeline_default"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {uc_catalog_name}")
spark.sql(f"USE CATALOG {uc_catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_schema_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {pipeline_target_schema}")
spark.sql(f"USE SCHEMA {uc_schema_name}")
spark.sql("CREATE VOLUME IF NOT EXISTS config")

uc_volume_path = (
    f"/Volumes/{uc_catalog_name}/{uc_schema_name}/config"
)
pipeline_id_file = f"{uc_volume_path}/pipeline_id.txt"
pipeline_name = f"sdp_meta_demo_{uc_schema_name}"

print(f"Catalog          : {uc_catalog_name}")
print(f"Config Schema    : {uc_catalog_name}.{uc_schema_name}")
print(f"Bronze Schema    : {uc_catalog_name}.{bronze_schema}")
print(f"Silver Schema    : {uc_catalog_name}.{silver_schema}")
print(f"Pipeline Default : {uc_catalog_name}.{pipeline_target_schema}")
print(f"Volume           : {uc_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Define Paths

# COMMAND ----------

demo_path = f"{uc_volume_path}/demo"
resources_path = f"{demo_path}/resources"
data_path = f"{resources_path}/data"
ddl_path = f"{resources_path}/ddl"
incremental_data_path = f"{resources_path}/incremental_data"
conf_path = f"{demo_path}/conf"
dqe_path = f"{conf_path}/dqe"
transformation_path = conf_path
onboarding_file_path = f"{uc_volume_path}/onboarding.{onboarding_format}"
af_data_path = f"{data_path}/append_flow"
snapshot_data_path = f"{data_path}/snapshots"
sink_path = f"{uc_volume_path}/data/sink"

for path in [
    demo_path, resources_path, data_path, ddl_path,
    incremental_data_path, conf_path, dqe_path,
    af_data_path, snapshot_data_path, sink_path,
]:
    os.makedirs(path, exist_ok=True)

print(f"Volume path       : {uc_volume_path}")
print(f"Data path         : {data_path}")
print(f"DDL path          : {ddl_path}")
print(f"DQE path          : {dqe_path}")
print(f"Append Flow path  : {af_data_path}")
print(f"Snapshot path     : {snapshot_data_path}")
print(f"Sink path         : {sink_path}")
print(f"Onboarding file   : {onboarding_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Create Configuration Files
# MAGIC
# MAGIC DDL schemas, data quality expectations, and silver transformation
# MAGIC configs are created inline so the notebook is fully self-contained.
# MAGIC
# MAGIC > **DDL Schemas**: define column types for each source.
# MAGIC > See: [demo/resources/ddl/](https://github.com/databrickslabs/dlt-meta/tree/main/demo/resources/ddl)
# MAGIC
# MAGIC > **Data Quality**: `expect_or_drop` and `expect_or_quarantine`.
# MAGIC > See: [json/dqe/](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/json/dqe) or [yml/dqe/](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/yml/dqe)
# MAGIC
# MAGIC > **Silver Transformations**: column selection and expressions.
# MAGIC > See: [json/silver_transformations.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/silver_transformations.json) or [yml/silver_transformations.yml](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/silver_transformations.yml)

# COMMAND ----------

ddl_files = {
    "customers.ddl": (
        "Op: string, dmsTimestamp: timestamp, customer_id: int, "
        "first_name: string, last_name: string, email: string, "
        "address: string, dob: date"
    ),
    "transactions.ddl": (
        "Op: string, dmsTimestamp: string, transaction_id: int, "
        "transaction_date: timestamp, customer_id: string, "
        "product_id: string, store_id: string"
    ),
    "products.ddl": (
        "Op: string, dmsTimestamp: string, product_id: int, "
        "name: string, price: double"
    ),
    "stores.ddl": (
        "Op: string, dmsTimestamp: string, "
        "store_id: string, address: string"
    ),
    "af_orders.ddl": (
        "order_id STRING, customer_id STRING, amount DOUBLE, "
        "item_count DOUBLE, order_date STRING, "
        "operation STRING, operation_date STRING"
    ),
    "iot_events.ddl": (
        "device_id STRING, device_name STRING, "
        "temp DOUBLE, humidity DOUBLE, "
        "battery_level DOUBLE, timestamp STRING"
    ),
}

for filename, content in ddl_files.items():
    with open(f"{ddl_path}/{filename}", "w") as fh:
        fh.write(content)
    print(f"  Created: {ddl_path}/{filename}")

# COMMAND ----------

# DQE configs keyed by base filename (no extension). The actual file
# extension (.json or .yml) is appended at write time based on conf_ext.
bronze_dqe = {
    "customers": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_customer_id": "customer_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR customer_id IS NULL"
            ),
        },
    },
    "transactions": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_transaction_id": "transaction_id IS NOT NULL",
            "valid_customer_id": "customer_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL "
                "OR transaction_id IS NULL "
                "OR customer_id IS NULL"
            ),
        },
    },
    "products": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_product_id": "product_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR product_id IS NULL"
            ),
        },
    },
    "stores": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_store_id": "store_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR store_id IS NULL"
            ),
        },
    },
    "af_orders_bronze_dqe": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_order_id": "order_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR order_id IS NULL"
            ),
        },
    },
    "iot_events_bronze_dqe": {
        "expect_or_drop": {
            "valid_device_id": "device_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": "device_id IS NULL",
        },
    },
}

silver_dqe = {
    "customers_silver_dqe": {
        "expect_or_drop": {
            "valid_customer_id": "customer_id IS NOT NULL",
        },
    },
    "transactions_silver_dqe": {
        "expect_or_drop": {
            "valid_transaction_id": "transaction_id IS NOT NULL",
            "valid_customer_id": "customer_id IS NOT NULL",
        },
    },
    "products_silver_dqe": {
        "expect_or_drop": {
            "valid_product_id": "product_id IS NOT NULL",
        },
    },
    "stores_silver_dqe": {
        "expect_or_drop": {
            "valid_store_id": "store_id IS NOT NULL",
        },
    },
    "af_orders_silver_dqe": {
        "expect_or_drop": {
            "valid_order_id": "order_id IS NOT NULL",
        },
    },
}

for dqe_set in [bronze_dqe, silver_dqe]:
    for basename, content in dqe_set.items():
        out_path = f"{dqe_path}/{basename}.{conf_ext}"
        _write_conf(content, out_path)
        print(f"  Created: {out_path}")

# COMMAND ----------

silver_transformations = [
    {
        "target_table": "customers",
        "select_exp": [
            "customer_id",
            "concat(first_name,' ',last_name) as full_name",
            "email", "address", "dob",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
    {
        "target_table": "transactions",
        "select_exp": [
            "transaction_id", "transaction_date",
            "customer_id", "product_id", "store_id",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
    {
        "target_table": "products",
        "select_exp": [
            "product_id", "name", "price",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
    {
        "target_table": "stores",
        "select_exp": [
            "store_id", "address",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
]

st_path = f"{transformation_path}/silver_transformations.{conf_ext}"
_write_conf(silver_transformations, st_path)
print(f"  Created: {st_path}")

af_silver_transformations = [
    {
        "target_table": "orders",
        "select_exp": [
            "order_id", "customer_id", "amount",
            "item_count", "order_date",
            "operation", "operation_date",
            "_rescued_data",
        ],
    },
]

af_st_path = (
    f"{transformation_path}/af_silver_transformations.{conf_ext}"
)
_write_conf(af_silver_transformations, af_st_path)
print(f"  Created: {af_st_path}")

snapshot_silver_transformations = [
    {
        "target_table": "snap_products",
        "select_exp": [
            "product_id", "name", "price", "dmsTimestamp",
        ],
        "where_clause": ["__END_AT IS NULL"],
    },
    {
        "target_table": "snap_stores",
        "select_exp": [
            "store_id", "address", "dmsTimestamp",
        ],
    },
]

snap_st_path = (
    f"{transformation_path}/snapshot_silver_transformations.{conf_ext}"
)
_write_conf(snapshot_silver_transformations, snap_st_path)
print(f"  Created: {snap_st_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Create Demo Data
# MAGIC
# MAGIC Based on your **Data Source** widget selection:
# MAGIC - **dbdatagen**: generates synthetic data using
# MAGIC   [dbldatagen](https://github.com/databrickslabs/dbldatagen)
# MAGIC - **github**: downloads sample data from the
# MAGIC   [dlt-meta repo](https://github.com/databrickslabs/dlt-meta/tree/main/demo/resources)

# COMMAND ----------

if data_source == "dbdatagen":
    import dbldatagen as dg
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        TimestampType, DateType, DoubleType,
    )
    from datetime import datetime, timedelta

    base_ts = "2022-06-24 18:53:24"
    incr_ts = "2022-06-24 19:01:10"
    NUM_CUSTOMERS = 200
    NUM_TRANSACTIONS = 2000
    NUM_PRODUCTS = 20
    NUM_STORES = 4
    INCR_CUSTOMERS = 100
    INCR_PRODUCTS = 10
    INCR_STORES = 2
    INCR_TRANSACTIONS = 200

    # --- Initial load (no Op column — full load, not CDC) ---
    # Op will be NULL in Bronze for the initial dataset; this is intentional.
    # The DDL defines Op: string to match the DMS schema, but initial full-load
    # files don't carry an Op value. silver_cdc_apply_changes treats NULL Op
    # as an insert (apply_as_deletes: "Op = 'D'" won't match NULL).
    # --- Customers initial load ---
    customers_df = (
        dg.DataGenerator(spark, name="customers",
                         rowcount=NUM_CUSTOMERS, seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("customer_id", IntegerType(),
                    minValue=1, maxValue=NUM_CUSTOMERS,
                    uniqueValues=NUM_CUSTOMERS)
        .withColumn("first_name", StringType(),
                    values=["Alice", "Bob", "Carol", "David",
                            "Eve", "Frank", "Grace", "Henry",
                            "Ivy", "Jack", "Karen", "Leo",
                            "Mia", "Noah", "Olivia", "Paul"])
        .withColumn("last_name", StringType(),
                    values=["Smith", "Jones", "Brown", "Wilson",
                            "Taylor", "Davis", "Clark", "Hall",
                            "Allen", "Young", "King", "Wright"])
        .withColumn("email", "string",
                    template=r"\\w.\\w@example.com")
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Main St, City, ST \\d\\d\\d\\d\\d")
        .withColumn("dob", DateType(),
                    begin="1960-01-01", end="2005-12-31")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/customers", exist_ok=True)
    (customers_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/customers"))
    print(f"  Generated {NUM_CUSTOMERS} customers")

    # --- Transactions initial load ---
    transactions_df = (
        dg.DataGenerator(spark, name="transactions",
                         rowcount=NUM_TRANSACTIONS,
                         seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("transaction_id", IntegerType(),
                    minValue=1, maxValue=NUM_TRANSACTIONS,
                    uniqueValues=NUM_TRANSACTIONS)
        .withColumn("transaction_date", DateType(),
                    begin="2022-05-01", end="2022-06-24")
        .withColumn("customer_id", IntegerType(),
                    minValue=1, maxValue=NUM_CUSTOMERS)
        .withColumn("product_id", IntegerType(),
                    minValue=1, maxValue=NUM_PRODUCTS)
        .withColumn("store_id", IntegerType(),
                    minValue=1, maxValue=NUM_STORES)
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/transactions", exist_ok=True)
    (transactions_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/transactions"))
    print(f"  Generated {NUM_TRANSACTIONS} transactions")

    # --- Products initial load ---
    products_df = (
        dg.DataGenerator(spark, name="products",
                         rowcount=NUM_PRODUCTS, seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("product_id", IntegerType(),
                    minValue=1, maxValue=NUM_PRODUCTS,
                    uniqueValues=NUM_PRODUCTS)
        .withColumn("name", StringType(),
                    values=["shorts", "hat", "accessories",
                            "sneakers", "coat", "sweater",
                            "boots", "sweatshirt", "jacket",
                            "scarf"])
        .withColumn("price", DoubleType(),
                    minValue=10.0, maxValue=999.0)
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/products", exist_ok=True)
    (products_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/products"))
    print(f"  Generated {NUM_PRODUCTS} products")

    # --- Stores initial load ---
    stores_df = (
        dg.DataGenerator(spark, name="stores",
                         rowcount=NUM_STORES, seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("store_id", IntegerType(),
                    minValue=1, maxValue=NUM_STORES,
                    uniqueValues=NUM_STORES)
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Store Blvd, City, ST \\d\\d\\d\\d\\d")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/stores", exist_ok=True)
    (stores_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/stores"))
    print(f"  Generated {NUM_STORES} stores")

    # --- Incremental CDC data (Op=I for new inserts) ---
    incr_customers_df = (
        dg.DataGenerator(spark, name="incr_customers",
                         rowcount=INCR_CUSTOMERS,
                         seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("customer_id", IntegerType(),
                    minValue=NUM_CUSTOMERS + 1,
                    maxValue=NUM_CUSTOMERS + INCR_CUSTOMERS,
                    uniqueValues=INCR_CUSTOMERS)
        .withColumn("first_name", StringType(),
                    values=["Alice", "Bob", "Carol", "David",
                            "Eve", "Frank", "Grace", "Henry"])
        .withColumn("last_name", StringType(),
                    values=["Smith", "Jones", "Brown", "Wilson",
                            "Taylor", "Davis", "Clark", "Hall"])
        .withColumn("email", "string",
                    template=r"\\w.\\w@example.com")
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Main St, City, ST \\d\\d\\d\\d\\d")
        .withColumn("dob", DateType(),
                    begin="1960-01-01", end="2005-12-31")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{incremental_data_path}/customers", exist_ok=True)
    (incr_customers_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/customers"))
    print(f"  Generated {INCR_CUSTOMERS} incremental customers")

    incr_products_df = (
        dg.DataGenerator(spark, name="incr_products",
                         rowcount=INCR_PRODUCTS,
                         seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("product_id", IntegerType(),
                    minValue=NUM_PRODUCTS + 1,
                    maxValue=NUM_PRODUCTS + INCR_PRODUCTS,
                    uniqueValues=INCR_PRODUCTS)
        .withColumn("name", StringType(),
                    values=["sneakers", "sweater", "accessories",
                            "coat", "boots"])
        .withColumn("price", DoubleType(),
                    minValue=10.0, maxValue=999.0)
        .build()
        .drop("_id")
    )
    os.makedirs(f"{incremental_data_path}/products", exist_ok=True)
    (incr_products_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/products"))
    print(f"  Generated {INCR_PRODUCTS} incremental products")

    incr_stores_df = (
        dg.DataGenerator(spark, name="incr_stores",
                         rowcount=INCR_STORES, seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("store_id", IntegerType(),
                    minValue=NUM_STORES + 1,
                    maxValue=NUM_STORES + INCR_STORES,
                    uniqueValues=INCR_STORES)
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Store Blvd, City, ST \\d\\d\\d\\d\\d")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{incremental_data_path}/stores", exist_ok=True)
    (incr_stores_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/stores"))
    print(f"  Generated {INCR_STORES} incremental stores")

    incr_txn_df = (
        dg.DataGenerator(spark, name="incr_transactions",
                         rowcount=INCR_TRANSACTIONS,
                         seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("transaction_id", IntegerType(),
                    minValue=NUM_TRANSACTIONS + 1,
                    maxValue=NUM_TRANSACTIONS + INCR_TRANSACTIONS,
                    uniqueValues=INCR_TRANSACTIONS)
        .withColumn("transaction_date", DateType(),
                    begin="2022-06-25", end="2022-07-10")
        .withColumn("customer_id", IntegerType(),
                    minValue=1,
                    maxValue=NUM_CUSTOMERS + INCR_CUSTOMERS)
        .withColumn("product_id", IntegerType(),
                    minValue=1,
                    maxValue=NUM_PRODUCTS + INCR_PRODUCTS)
        .withColumn("store_id", IntegerType(),
                    minValue=1,
                    maxValue=NUM_STORES + INCR_STORES)
        .build()
        .drop("_id")
    )
    os.makedirs(
        f"{incremental_data_path}/transactions", exist_ok=True
    )
    (incr_txn_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/transactions"))
    print(f"  Generated {INCR_TRANSACTIONS} incremental transactions")

    print("\nAll data generated with dbdatagen.")

else:  # github
    import requests

    REPO_OWNER = "databrickslabs"
    REPO_NAME = "dlt-meta"

    def download_file(source, destination):
        raw_url = (
            f"https://raw.githubusercontent.com/"
            f"{REPO_OWNER}/{REPO_NAME}/{git_branch}/{source}"
        )
        os.makedirs(
            os.path.dirname(destination), exist_ok=True
        )
        resp = requests.get(raw_url)
        if resp.status_code == 200:
            with open(destination, "wb") as fh:
                fh.write(resp.content)
            print(f"  downloaded: {source}")
        else:
            print(
                f"  FAILED: {source} (HTTP {resp.status_code})"
            )

    api_url = (
        f"https://api.github.com/repos/"
        f"{REPO_OWNER}/{REPO_NAME}"
        f"/git/trees/{git_branch}?recursive=1"
    )
    headers = {"Accept": "application/vnd.github.v3+json"}
    response = requests.get(api_url, headers=headers)

    if response.status_code != 200:
        raise Exception(
            "Failed to fetch repo tree. "
            f"Status: {response.status_code}"
        )

    repo_data = response.json()
    print(
        f"Downloading data from GitHub "
        f"(branch: {git_branch})...\n"
    )
    excluded = ("afam", "snapshots", "iot", "cars", "eventhub")

    for item in repo_data.get("tree", []):
        if item["type"] != "blob":
            continue
        fp = item["path"]

        if (
            "demo/resources/data/" in fp
            and not any(x in fp for x in excluded)
        ):
            rel = fp.replace("demo/resources/data/", "")
            download_file(fp, f"{data_path}/{rel}")

        elif "demo/resources/incremental_data/" in fp:
            rel = fp.replace(
                "demo/resources/incremental_data/", ""
            )
            download_file(
                fp, f"{incremental_data_path}/{rel}"
            )

    print("\nAll data downloaded from GitHub.")

# --- Generate bad records for quarantine (both data sources) ---
bad_customers = (
    "dmsTimestamp,customer_id,first_name,last_name,"
    "email,address,dob\n"
    "2022-06-24 18:55:00,,,,bad_email,bad addr,\n"
    "2022-06-24 18:55:01,,Jane,Doe,"
    "jane@example.com,123 Main St,1990-01-01\n"
)
bad_transactions = (
    "dmsTimestamp,transaction_id,transaction_date,"
    "customer_id,product_id,store_id\n"
    "2022-06-24 18:55:00,,,,,\n"
    "2022-06-24 18:55:01,,2022-06-01,,,\n"
)
bad_products = (
    "dmsTimestamp,product_id,name,price\n"
    "2022-06-24 18:55:00,,,\n"
    "2022-06-24 18:55:01,,bad_product,not_a_price\n"
)
bad_stores = (
    "dmsTimestamp,store_id,address\n"
    "2022-06-24 18:55:00,,\n"
    "2022-06-24 18:55:01,,bad address\n"
)

bad_data_files = {
    "customers": bad_customers,
    "transactions": bad_transactions,
    "products": bad_products,
    "stores": bad_stores,
}

print("\nGenerating bad records for quarantine testing...")
for domain, content in bad_data_files.items():
    bad_path = f"{data_path}/{domain}/BAD_RECORDS.csv"
    os.makedirs(f"{data_path}/{domain}", exist_ok=True)
    with open(bad_path, "w") as fh:
        fh.write(content)
    print(f"  Created: {bad_path}")
print("Quarantine tables will capture these records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Create Append Flow Data
# MAGIC
# MAGIC Append Flow reads from **multiple source paths** and writes to the
# MAGIC **same target table** using `dp.append_flow`.
# MAGIC We create two separate source directories for orders.
# MAGIC See: [cloudfiles-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template)

# COMMAND ----------

af_orders_main = f"{af_data_path}/orders"
af_orders_secondary = f"{af_data_path}/orders_af"
os.makedirs(af_orders_main, exist_ok=True)
os.makedirs(af_orders_secondary, exist_ok=True)

orders_main_data = [
    {"order_id": "ORD001", "customer_id": "C001",
     "amount": 150.50, "item_count": 3,
     "order_date": "2022-06-24", "operation": "APPEND",
     "operation_date": "2022-06-24 18:55:00"},
    {"order_id": "ORD002", "customer_id": "C002",
     "amount": 89.99, "item_count": 1,
     "order_date": "2022-06-24", "operation": "APPEND",
     "operation_date": "2022-06-24 18:56:00"},
    {"order_id": "ORD003", "customer_id": "C003",
     "amount": 320.00, "item_count": 5,
     "order_date": "2022-06-24", "operation": "APPEND",
     "operation_date": "2022-06-24 18:57:00"},
    {"order_id": "ORD004", "customer_id": "C001",
     "amount": 45.25, "item_count": 2,
     "order_date": "2022-06-25", "operation": "APPEND",
     "operation_date": "2022-06-25 10:00:00"},
]

orders_af_data = [
    {"order_id": "ORD005", "customer_id": "C004",
     "amount": 200.00, "item_count": 4,
     "order_date": "2022-06-25", "operation": "APPEND",
     "operation_date": "2022-06-25 11:00:00"},
    {"order_id": "ORD006", "customer_id": "C005",
     "amount": 75.50, "item_count": 1,
     "order_date": "2022-06-25", "operation": "APPEND",
     "operation_date": "2022-06-25 12:00:00"},
    {"order_id": "ORD007", "customer_id": "C002",
     "amount": 500.00, "item_count": 10,
     "order_date": "2022-06-26", "operation": "APPEND",
     "operation_date": "2022-06-26 09:00:00"},
]

main_path = f"{af_orders_main}/orders_batch_1.json"
with open(main_path, "w") as fh:
    for record in orders_main_data:
        fh.write(json.dumps(record) + "\n")
print(f"  Created: {main_path} ({len(orders_main_data)} records)")

af_path = f"{af_orders_secondary}/orders_batch_af.json"
with open(af_path, "w") as fh:
    for record in orders_af_data:
        fh.write(json.dumps(record) + "\n")
print(f"  Created: {af_path} ({len(orders_af_data)} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Create Snapshot Data
# MAGIC
# MAGIC Snapshot-based ingestion uses `apply_changes_from_snapshot` instead
# MAGIC of streaming. Each snapshot represents a full point-in-time view.
# MAGIC See: [snapshot-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/snapshot-onboarding.template)

# COMMAND ----------

snap_stores_dir = f"{snapshot_data_path}/stores"
snap_incr_stores = f"{snapshot_data_path}/incremental_snapshots/stores"
os.makedirs(snap_stores_dir, exist_ok=True)
os.makedirs(snap_incr_stores, exist_ok=True)

snap_stores_load1 = (
    "dmsTimestamp,store_id,address\n"
    '2022-06-24 18:53:25,1,'
    '"6761 Brian Falls Navarrobury, VA 17977"\n'
    '2022-06-24 18:53:25,2,'
    '"4215 Bruce Shoals Apt. 920 Port Travis, SC 71335"\n'
    '2022-06-24 18:53:25,3,'
    '"96924 Gregory Mill Pricefurt, GA 68691"\n'
    '2022-06-24 18:53:25,4,'
    '"070 Cynthia Cliff Paulport, FL 21469"\n'
)
with open(f"{snap_stores_dir}/LOAD_1.csv", "w") as fh:
    fh.write(snap_stores_load1)
print(f"  Created: {snap_stores_dir}/LOAD_1.csv")

snap_stores_load2 = (
    "dmsTimestamp,store_id,address\n"
    '2022-06-25 10:00:00,1,'
    '"V2 6761 Brian Falls Navarrobury, VA 17977"\n'
    '2022-06-25 10:00:00,2,'
    '"V2 4215 Bruce Shoals Apt. 920 Port Travis, SC 71335"\n'
)
with open(f"{snap_incr_stores}/LOAD_2.csv", "w") as fh:
    fh.write(snap_stores_load2)
print(f"  Created: {snap_incr_stores}/LOAD_2.csv")

snap_products_dir = f"{snapshot_data_path}/products"
snap_incr_products = (
    f"{snapshot_data_path}/incremental_snapshots/products"
)
os.makedirs(snap_products_dir, exist_ok=True)
os.makedirs(snap_incr_products, exist_ok=True)

snap_products_load1 = (
    "dmsTimestamp,product_id,name,price\n"
    "2022-06-24 18:53:24,1,shorts,793.50\n"
    "2022-06-24 18:53:24,2,hat,598.91\n"
    "2022-06-24 18:53:24,3,coat,914.34\n"
    "2022-06-24 18:53:24,4,accessories,717.76\n"
    "2022-06-24 18:53:24,5,sneakers,975.06\n"
)
with open(f"{snap_products_dir}/LOAD_1.csv", "w") as fh:
    fh.write(snap_products_load1)
print(f"  Created: {snap_products_dir}/LOAD_1.csv")

snap_products_load2 = (
    "dmsTimestamp,product_id,name,price\n"
    "2022-06-25 10:00:00,1,shorts_v2,793.50\n"
    "2022-06-25 10:00:00,2,hat_v2,598.91\n"
    "2022-06-25 10:00:00,3,coat_v2,914.34\n"
    "2022-06-25 10:00:00,4,accessories,717.76\n"
    "2022-06-25 10:00:00,5,sneakers,975.06\n"
)
with open(f"{snap_incr_products}/LOAD_2.csv", "w") as fh:
    fh.write(snap_products_load2)
print(f"  Created: {snap_incr_products}/LOAD_2.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 Create IoT Events Data (for DLT Sink Demo)
# MAGIC
# MAGIC Simple IoT sensor events used to demonstrate writing
# MAGIC to external delta sinks.

# COMMAND ----------

iot_data_dir = f"{data_path}/iot_events"
os.makedirs(iot_data_dir, exist_ok=True)

iot_events = [
    {"device_id": "dev001", "device_name": "sensor-alpha",
     "temp": 22.5, "humidity": 45.0,
     "battery_level": 85.0, "timestamp": "2022-06-24 18:55:00"},
    {"device_id": "dev002", "device_name": "sensor-beta",
     "temp": 31.2, "humidity": 60.0,
     "battery_level": 72.0, "timestamp": "2022-06-24 18:55:01"},
    {"device_id": "dev003", "device_name": "sensor-gamma",
     "temp": 18.0, "humidity": 38.0,
     "battery_level": 95.0, "timestamp": "2022-06-24 18:55:02"},
    {"device_id": "dev001", "device_name": "sensor-alpha",
     "temp": 23.1, "humidity": 44.0,
     "battery_level": 84.0, "timestamp": "2022-06-24 19:00:00"},
    {"device_id": "dev002", "device_name": "sensor-beta",
     "temp": 32.0, "humidity": 62.0,
     "battery_level": 70.0, "timestamp": "2022-06-24 19:00:01"},
]

iot_path = f"{iot_data_dir}/iot_batch_1.csv"
with open(iot_path, "w", newline="") as fh:
    writer = csv.DictWriter(
        fh, fieldnames=iot_events[0].keys()
    )
    writer.writeheader()
    writer.writerows(iot_events)
print(f"  Created: {iot_path} ({len(iot_events)} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Verify Created Resources

# COMMAND ----------

print("=== Sample Data (Initial Load) ===")
for domain in ["customers", "transactions", "products", "stores"]:
    dp = f"{data_path}/{domain}"
    if os.path.exists(dp):
        files = [
            f for f in os.listdir(dp)
            if not f.startswith("_") and not f.startswith(".")
        ]
        print(f"  {domain}/: {len(files)} file(s)")

print("\n=== Incremental Data (CDC) ===")
for domain in ["customers", "transactions", "products", "stores"]:
    dp = f"{incremental_data_path}/{domain}"
    if os.path.exists(dp):
        files = [
            f for f in os.listdir(dp)
            if not f.startswith("_") and not f.startswith(".")
        ]
        print(f"  {domain}/: {len(files)} file(s)")

print("\n=== Append Flow Data ===")
for d in ["orders", "orders_af"]:
    dp = f"{af_data_path}/{d}"
    if os.path.exists(dp):
        files = os.listdir(dp)
        print(f"  {d}/: {len(files)} file(s)")

print("\n=== Snapshot Data ===")
for d in ["products", "stores"]:
    dp = f"{snapshot_data_path}/{d}"
    if os.path.exists(dp):
        files = os.listdir(dp)
        print(f"  {d}/: {len(files)} snapshot(s)")

print(f"\n=== IoT Events ===")
print(f"  iot_events/: {len(os.listdir(iot_data_dir))} file(s)")

print(f"\n=== DDL Schemas ===\n  {os.listdir(ddl_path)}")
print(f"\n=== DQE Files ===\n  {os.listdir(dqe_path)}")
print(f"\n=== Config ===\n  {os.listdir(conf_path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2: Onboarding — Customers & Transactions
# MAGIC
# MAGIC Onboarding converts a JSON config into **DataflowSpec** metadata
# MAGIC tables that drive the pipeline.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **[Onboarding File](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/onboarding.template)**:
# MAGIC   JSON defining source/target for each data feed
# MAGIC - **[OnboardDataflowspec API](https://github.com/databrickslabs/dlt-meta/blob/main/src/databricks/labs/sdp_meta/onboard_dataflowspec.py)**:
# MAGIC   Reads the file and writes Bronze/Silver DataflowSpec tables
# MAGIC - **DataflowSpec Tables**: Delta tables storing pipeline metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create Onboarding File
# MAGIC
# MAGIC The spec for **Customers** and **Transactions** lives in the repo as a
# MAGIC committed sample, in **whichever format you picked in the widget**:
# MAGIC
# MAGIC - `json` → [`demo/conf/json/sample_onboarding.json`](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/sample_onboarding.json)
# MAGIC - `yml`  → [`demo/conf/yml/sample_onboarding.yml`](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/sample_onboarding.yml)
# MAGIC
# MAGIC The cell below loads that file, substitutes runtime tokens
# MAGIC (`$data_path`, `$dqe_path`, `$uc_catalog_name`, …), prints the
# MAGIC rendered text so you actually see the chosen format, and writes
# MAGIC it to the UC volume as `onboarding.{json|yml}`. Each entry
# MAGIC defines:
# MAGIC - Source details (path, schema, format)
# MAGIC - Bronze target (table, data quality, quarantine)
# MAGIC - Silver target (CDC config, transformations)
# MAGIC
# MAGIC > **Note on field names:** keys like
# MAGIC > `bronze_data_quality_expectations_json_prod` and
# MAGIC > `silver_transformation_json_prod` are the runner's spec schema
# MAGIC > (see `OnboardDataflowspec`); the `_json_` suffix is part of
# MAGIC > the field name and does **not** mean the value must be JSON —
# MAGIC > the value may point to a `.json` or a `.yml` file.
# MAGIC >
# MAGIC > **CDC (SCD Type 2)**: Silver uses `apply_changes` for
# MAGIC > history tracking.
# MAGIC > See: [dataflow_pipeline.py](https://github.com/databrickslabs/dlt-meta/blob/main/src/databricks/labs/sdp_meta/dataflow_pipeline.py)

# COMMAND ----------

from string import Template

import requests


def _load_sample_onboarding_text(onboarding_format, conf_ext, git_branch):
    """Load the committed sample onboarding file for the chosen format.

    Resolution order (first match wins):

    1. **Workspace co-located** — when the notebook was imported as part of
       the dlt-meta repo (so its workspace path contains ``/demo/``), we
       read the sibling ``demo/conf/{json|yml}/sample_onboarding.{json|yml}``
       directly via ``open()``. Fast, offline-friendly.
    2. **GitHub raw fallback** — when the notebook was uploaded standalone
       (no ``/demo/`` in its workspace path, no co-located repo files), we
       fetch the same sample file from
       ``raw.githubusercontent.com/databrickslabs/dlt-meta/<git_branch>/...``.
       This mirrors the GitHub-download path already used in stage 1 for
       datasets, so the ``git_branch`` widget controls both.

    Returns:
        tuple[str, str]: ``(sample_text, source_label)`` where
        ``source_label`` is the path/URL the text was loaded from (for
        display in the cell output).
    """
    rel_path = f"demo/conf/{onboarding_format}/sample_onboarding.{conf_ext}"

    ctx = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
    )
    nb_path = ctx.notebookPath().get()

    if "/demo/" in nb_path:
        repo_root_ws = nb_path.rsplit("/demo/", 1)[0]
        if not repo_root_ws.startswith("/Workspace"):
            repo_root_ws = "/Workspace" + repo_root_ws
        ws_path = f"{repo_root_ws}/{rel_path}"
        try:
            with open(ws_path, "r") as fh:
                return fh.read(), ws_path
        except (FileNotFoundError, IsADirectoryError, PermissionError):
            pass

    raw_url = (
        f"https://raw.githubusercontent.com/databrickslabs/dlt-meta/"
        f"{git_branch}/{rel_path}"
    )
    resp = requests.get(raw_url, timeout=30)
    if resp.status_code == 200:
        return resp.text, raw_url
    raise RuntimeError(
        f"Could not load sample onboarding file. Notebook path "
        f"({nb_path}) is not under a dlt-meta repo checkout, and the "
        f"GitHub fallback at {raw_url} returned HTTP "
        f"{resp.status_code}. Either re-import the notebook under the "
        f"dlt-meta repo, or set the 'git_branch' widget to a branch that "
        f"contains {rel_path}."
    )


sample_text, sample_onboarding_source = _load_sample_onboarding_text(
    onboarding_format, conf_ext, git_branch
)

rendered_text = Template(sample_text).safe_substitute(
    data_path=data_path,
    ddl_path=ddl_path,
    dqe_path=dqe_path,
    transformation_path=transformation_path,
    uc_catalog_name=uc_catalog_name,
    bronze_schema=bronze_schema,
    silver_schema=silver_schema,
)

print(
    f"--- {sample_onboarding_source} (rendered as {onboarding_format}) ---\n"
)
print(rendered_text)

if conf_ext in ("yml", "yaml"):
    onboarding_json = yaml.safe_load(rendered_text)
else:
    onboarding_json = json.loads(rendered_text)

_write_onboarding(onboarding_json, onboarding_file_path)

print(f"Onboarding file: {onboarding_file_path}")
print(f"Data flows: {len(onboarding_json)} (customers, transactions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Verify the Onboarding File on the UC Volume
# MAGIC
# MAGIC Cell 2.1 printed the *rendered template*. This cell prints the
# MAGIC bytes that actually landed on the UC volume after
# MAGIC `_write_onboarding(...)` serialized them — confirming round-trip
# MAGIC fidelity in the format you selected. This is the exact file the
# MAGIC `OnboardDataflowspec` runner will read in the next cell.

# COMMAND ----------

print(f"--- {onboarding_file_path} ({onboarding_format}) ---\n")
with open(onboarding_file_path, "r") as fh:
    print(fh.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Run Onboarding
# MAGIC
# MAGIC The
# MAGIC [OnboardDataflowspec](https://github.com/databrickslabs/dlt-meta/blob/main/src/databricks/labs/sdp_meta/onboard_dataflowspec.py)
# MAGIC API reads the onboarding file (JSON or YAML) and creates two Delta tables:
# MAGIC - `bronze_dataflowspec` — metadata for Bronze layer
# MAGIC - `silver_dataflowspec` — metadata for Silver layer

# COMMAND ----------

from databricks.labs.sdp_meta.onboard_dataflowspec import (
    OnboardDataflowspec,
)

onboarding_params = {
    "onboarding_file_path": onboarding_file_path,
    "database": f"{uc_catalog_name}.{uc_schema_name}",
    "env": "prod",
    "bronze_dataflowspec_table": "bronze_dataflowspec",
    "silver_dataflowspec_table": "silver_dataflowspec",
    "overwrite": "True",
    "version": "v1",
    "import_author": "demo_user",
}
print("Onboarding parameters:")
print(json.dumps(onboarding_params, indent=2))

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("\nOnboarding complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Inspect DataflowSpec Tables

# COMMAND ----------

# DBTITLE 1,Bronze DataflowSpec
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{uc_schema_name}"
        ".bronze_dataflowspec"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver DataflowSpec
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{uc_schema_name}"
        ".silver_dataflowspec"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 3: Create Lakeflow Spark Declarative Pipeline
# MAGIC
# MAGIC The pipeline uses a single generic runner notebook that calls
# MAGIC `DataflowPipeline.invoke_dlt_pipeline(spark, layer)`.
# MAGIC
# MAGIC **Source**:
# MAGIC [DataflowPipeline](https://github.com/databrickslabs/dlt-meta/blob/main/src/databricks/labs/sdp_meta/dataflow_pipeline.py)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create Pipeline Runner Notebook
# MAGIC
# MAGIC This cell writes the runner notebook into your workspace
# MAGIC automatically. No manual upload needed.

# COMMAND ----------

import base64

notebook_name = "sdp_meta_pipeline_runner"
notebook_dir = (
    f"/Workspace/Users/"
    f"{spark.sql('SELECT current_user()').first()[0]}"
    f"/sdp_meta_demo"
)

runner_content = (
    "# Databricks notebook source\n"
    'sdp_meta_whl = spark.conf.get("sdp_meta_whl")\n'
    "%pip install $sdp_meta_whl  # noqa: E999\n"
    "\n"
    "# COMMAND ----------\n"
    "\n"
    'layer = spark.conf.get("layer", None)\n'
    "\n"
    "from databricks.labs.sdp_meta.dataflow_pipeline "
    "import DataflowPipeline\n"
    "DataflowPipeline.invoke_dlt_pipeline(\n"
    "    spark, layer,\n"
    ")\n"
)

encoded = base64.b64encode(
    runner_content.encode("utf-8")
).decode("utf-8")

w.workspace.mkdirs(notebook_dir)
w.workspace.import_(
    content=encoded,
    path=f"{notebook_dir}/{notebook_name}",
    format=ExportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)

runner_notebook_path = f"{notebook_dir}/{notebook_name}"
print(f"Runner notebook created: {runner_notebook_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2 Create and Start the Pipeline
# MAGIC
# MAGIC The cell below creates the Lakeflow Spark Declarative Pipeline
# MAGIC programmatically using the Databricks SDK, then starts it and
# MAGIC waits for completion. The pipeline ID is persisted to the UC
# MAGIC Volume so later stages can trigger reruns without manual
# MAGIC interaction.

# COMMAND ----------

git_url_for_pip = (
    f"git+https://github.com/databrickslabs/"
    f"dlt-meta.git@{git_branch}"
)

pipeline_config = {
    "layer": "bronze_silver",
    "bronze.group": "A1",
    "silver.group": "A1",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.bronze_dataflowspec"
    ),
    "silver.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.silver_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

# Create pipeline (idempotent: skip if already exists)
existing = [
    p for p in w.pipelines.list_pipelines()
    if p.name == pipeline_name
]
if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"Reusing existing pipeline: {pipeline_id}")
else:
    created = w.pipelines.create(
        name=pipeline_name,
        catalog=uc_catalog_name,
        # Spark Declarative Pipelines direct publishing mode requires
        # a pipeline-level target schema. DataflowPipeline sets
        # catalog+schema on every @dp.table() call from DataflowSpec
        # (bronze_database_prod / silver_database_prod),
        # so this target is never used for actual routing.
        # A dedicated placeholder schema is used to keep it separate from
        # the DataflowSpec metadata schema and the Bronze/Silver data schemas.
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(path=runner_notebook_path)
            )
        ],
        configuration=pipeline_config,
        development=True,
        serverless=True
    )
    pipeline_id = created.pipeline_id
    print(f"Pipeline created: {pipeline_id}")

with open(pipeline_id_file, "w") as fh:
    fh.write(pipeline_id)
print(f"Pipeline ID saved to: {pipeline_id_file}")

run_pipeline_and_wait(w, pipeline_id, label="initial load")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 4: Validate Initial Load
# MAGIC
# MAGIC After the pipeline completes in Stage 3, validate that Bronze
# MAGIC and Silver tables were created correctly.
# MAGIC
# MAGIC ### Data Flow
# MAGIC ```
# MAGIC Source Files (CSV)
# MAGIC   │
# MAGIC   ├─── CloudFiles (Autoloader) ──► Bronze Tables
# MAGIC   │                                   │
# MAGIC   │                     ┌──────────────┼──────────────┐
# MAGIC   │                     │              │              │
# MAGIC   │              expect_or_drop   Good Rows    expect_or_quarantine
# MAGIC   │              (dropped)             │         (quarantine table)
# MAGIC   │                                    │
# MAGIC   │                              Silver Tables
# MAGIC   │                          (CDC / SCD Type 2)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Data Flow Summary — Initial Load
# MAGIC
# MAGIC Shows row counts across all layers for each table:
# MAGIC **Source Files → Bronze → Quarantine → Silver**

# COMMAND ----------

# DBTITLE 1,Pipeline Data Flow Summary (Initial Load)
summary_rows = []
for table in ["customers", "transactions"]:
    src_dir = f"{data_path}/{table}"
    file_count = len([
        f for f in os.listdir(src_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(src_dir) else 0

    bronze_fqn = (
        f"{uc_catalog_name}.{bronze_schema}.{table}"
    )
    quarantine_fqn = (
        f"{uc_catalog_name}.{bronze_schema}"
        f".{table}_quarantine"
    )
    silver_fqn = (
        f"{uc_catalog_name}.{silver_schema}.{table}"
    )

    bronze_count = spark.sql(
        f"SELECT count(*) FROM {bronze_fqn}"
    ).first()[0]
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0
    silver_count = spark.sql(
        f"SELECT count(*) FROM {silver_fqn}"
    ).first()[0]

    summary_rows.append(Row(
        Table=table,
        Source_Files=file_count,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Bronze Tables — Sample Data

# COMMAND ----------

# DBTITLE 1,Bronze Customers
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze Transactions
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".transactions LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Quarantine Tables — Bad Records
# MAGIC
# MAGIC Records with NULL primary keys or malformed data are routed
# MAGIC here by `expect_or_quarantine` rules.
# MAGIC See: [DQE config](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/dqe/customers.json)

# COMMAND ----------

# DBTITLE 1,Customers Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Transactions Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".transactions_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Silver Tables — CDC / SCD Type 2
# MAGIC
# MAGIC Silver tables use `apply_changes` for CDC processing.
# MAGIC Only clean records (passed DQE) flow to Silver.

# COMMAND ----------

# DBTITLE 1,Silver Customers (SCD Type 2)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".customers LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Transactions (SCD Type 2)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".transactions LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 5: Add Products & Stores Feeds
# MAGIC
# MAGIC **Add new data feeds without modifying the pipeline.**
# MAGIC Update the onboarding file and re-run onboarding.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Update Onboarding File
# MAGIC
# MAGIC Add **Products** (data_flow_id: 103) and **Stores** (104).
# MAGIC
# MAGIC > **DDL**:
# MAGIC > [products.ddl](https://github.com/databrickslabs/dlt-meta/blob/main/demo/resources/ddl/products.ddl),
# MAGIC > [stores.ddl](https://github.com/databrickslabs/dlt-meta/blob/main/demo/resources/ddl/stores.ddl)

# COMMAND ----------

products_feed = {
    "data_flow_id": "103",
    "data_flow_group": "A1",
    "source_system": "mysql",
    "source_format": "cloudFiles",
    "source_details": {
        "source_path_prod": f"{data_path}/products",
        "source_schema_path": f"{ddl_path}/products.ddl",
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "products",
    "bronze_table_comment": "products bronze table",
    "bronze_reader_options": {
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true",
    },
    "bronze_cluster_by_auto": True,
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/products.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "products_quarantine",
    "bronze_quarantine_table_comment": (
        "products quarantine bronze table"
    ),
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "products",
    "silver_table_comment": "products silver table",
    "silver_cdc_apply_changes": {
        "keys": ["product_id"],
        "sequence_by": "dmsTimestamp",
        "scd_type": "2",
        "apply_as_deletes": "Op = 'D'",
        "except_column_list": [
            "Op", "dmsTimestamp", "_rescued_data",
        ],
    },
    "silver_cluster_by_auto": True,
    "silver_transformation_json_prod": (
        f"{transformation_path}/silver_transformations.{conf_ext}"
    ),
    "silver_data_quality_expectations_json_prod": (
        f"{dqe_path}/products_silver_dqe.{conf_ext}"
    ),
}

stores_feed = {
    "data_flow_id": "104",
    "data_flow_group": "A1",
    "source_system": "mysql",
    "source_format": "cloudFiles",
    "source_details": {
        "source_path_prod": f"{data_path}/stores",
        "source_schema_path": f"{ddl_path}/stores.ddl",
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "stores",
    "bronze_table_comment": "stores bronze table",
    "bronze_reader_options": {
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true",
    },
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/stores.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "stores_quarantine",
    "bronze_quarantine_table_comment": (
        "stores quarantine bronze table"
    ),
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "stores",
    "silver_table_comment": "stores silver table",
    "silver_cdc_apply_changes": {
        "keys": ["store_id"],
        "sequence_by": "dmsTimestamp",
        "scd_type": "2",
        "apply_as_deletes": "Op = 'D'",
        "except_column_list": [
            "Op", "dmsTimestamp", "_rescued_data",
        ],
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}/silver_transformations.{conf_ext}"
    ),
    "silver_data_quality_expectations_json_prod": (
        f"{dqe_path}/stores_silver_dqe.{conf_ext}"
    ),
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.extend([products_feed, stores_feed])

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding file updated: "
    f"{len(onboarding_json)} data flows"
)
print(
    "  customers (100), transactions (101), "
    "products (103), stores (104)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("Onboarding updated with products and stores!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Verify Updated DataflowSpec

# COMMAND ----------

# DBTITLE 1,Bronze DataflowSpec (4 feeds)
display(
    spark.sql(
        f"SELECT dataFlowId, sourceFormat, "
        f"targetDetails['table'] as target_table "
        f"FROM {uc_catalog_name}.{uc_schema_name}"
        ".bronze_dataflowspec"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver DataflowSpec (4 feeds)
display(
    spark.sql(
        f"SELECT dataFlowId, "
        f"targetDetails['table'] as target_table "
        f"FROM {uc_catalog_name}.{uc_schema_name}"
        ".silver_dataflowspec"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Re-run the Pipeline
# MAGIC
# MAGIC The same generic pipeline automatically picks up Products and
# MAGIC Stores from the updated DataflowSpec — no pipeline code changes needed.

# COMMAND ----------

with open(pipeline_id_file, "r") as fh:
    pipeline_id = fh.read().strip()
run_pipeline_and_wait(w, pipeline_id, label="add products & stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Data Flow Summary — All 4 Feeds
# MAGIC
# MAGIC After re-running the pipeline with Products and Stores added,
# MAGIC verify row counts across all layers for every table.

# COMMAND ----------

# DBTITLE 1,Pipeline Data Flow Summary (4 Feeds)
all_tables = ["customers", "transactions", "products", "stores"]
summary_rows = []
for table in all_tables:
    src_dir = f"{data_path}/{table}"
    file_count = len([
        f for f in os.listdir(src_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(src_dir) else 0

    bronze_fqn = (
        f"{uc_catalog_name}.{bronze_schema}.{table}"
    )
    quarantine_fqn = (
        f"{uc_catalog_name}.{bronze_schema}"
        f".{table}_quarantine"
    )
    silver_fqn = (
        f"{uc_catalog_name}.{silver_schema}.{table}"
    )

    bronze_count = spark.sql(
        f"SELECT count(*) FROM {bronze_fqn}"
    ).first()[0]
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0
    silver_count = spark.sql(
        f"SELECT count(*) FROM {silver_fqn}"
    ).first()[0]

    summary_rows.append(Row(
        Table=table,
        Source_Files=file_count,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# DBTITLE 1,Bronze Products — Sample Data
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".products LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze Stores — Sample Data
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".stores LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Products Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".products_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Stores Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".stores_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Products
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".products LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Stores
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".stores LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 6: Incremental Data Load (CDC)
# MAGIC
# MAGIC Push incremental CDC data (Inserts, Updates, Deletes) and
# MAGIC re-run the pipeline. Incremental files use CDC format with an
# MAGIC `Op` column: `I` (Insert), `U` (Update), `D` (Delete).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Preview Incremental Data

# COMMAND ----------

# DBTITLE 1,Incremental Files Available
for domain in ["customers", "transactions", "products", "stores"]:
    dp = f"{incremental_data_path}/{domain}"
    if os.path.exists(dp):
        files = [
            f for f in os.listdir(dp)
            if not f.startswith("_") and not f.startswith(".")
        ]
        print(f"{domain}: {len(files)} incremental file(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Copy Incremental Data to Source Directories
# MAGIC
# MAGIC Simulate the arrival of new CDC data by copying incremental
# MAGIC files to the source data directories.

# COMMAND ----------

for domain in ["customers", "transactions", "stores", "products"]:
    source = f"{incremental_data_path}/{domain}/"
    target = f"{data_path}/{domain}/"
    if os.path.exists(source.rstrip("/")):
        dbutils.fs.cp(source, target, recurse=True)
        print(f"Copied incremental data: {domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Re-run the Pipeline
# MAGIC
# MAGIC CloudFiles (Autoloader) automatically detects the new incremental files.

# COMMAND ----------

with open(pipeline_id_file, "r") as fh:
    pipeline_id = fh.read().strip()
run_pipeline_and_wait(w, pipeline_id, label="incremental CDC")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 7: Validate Incremental Results
# MAGIC
# MAGIC Verify that CDC changes (Insert / Update / Delete) were applied
# MAGIC correctly. Compare row counts before and after incremental load.
# MAGIC
# MAGIC ### Expected Behavior
# MAGIC ```
# MAGIC Incremental CDC Files (Op: I/U/D)
# MAGIC   │
# MAGIC   ├─── Autoloader picks up new files ──► Bronze (rows grow)
# MAGIC   │                                          │
# MAGIC   │                          DQE rules applied again
# MAGIC   │                                          │
# MAGIC   │                  Good rows ──► Silver (SCD Type 2)
# MAGIC   │                                  ├─ INSERTs: new rows
# MAGIC   │                                  ├─ UPDATEs: old row closed (__END_AT set),
# MAGIC   │                                  │           new row opened
# MAGIC   │                                  └─ DELETEs: row closed (__END_AT set)
# MAGIC   │
# MAGIC   └─── Bad records ──► Quarantine (rows grow)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Data Flow Summary — After Incremental Load

# COMMAND ----------

# DBTITLE 1,Pipeline Data Flow Summary (After Incremental)
all_tables = ["customers", "transactions", "products", "stores"]
summary_rows = []
for table in all_tables:
    src_dir = f"{data_path}/{table}"
    file_count = len([
        f for f in os.listdir(src_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(src_dir) else 0

    inc_dir = f"{incremental_data_path}/{table}"
    inc_file_count = len([
        f for f in os.listdir(inc_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(inc_dir) else 0

    bronze_fqn = (
        f"{uc_catalog_name}.{bronze_schema}.{table}"
    )
    quarantine_fqn = (
        f"{uc_catalog_name}.{bronze_schema}"
        f".{table}_quarantine"
    )
    silver_fqn = (
        f"{uc_catalog_name}.{silver_schema}.{table}"
    )

    bronze_count = spark.sql(
        f"SELECT count(*) FROM {bronze_fqn}"
    ).first()[0]
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0
    silver_count = spark.sql(
        f"SELECT count(*) FROM {silver_fqn}"
    ).first()[0]

    summary_rows.append(Row(
        Table=table,
        Initial_Files=file_count,
        Incremental_Files=inc_file_count,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Silver Tables — SCD Type 2 History
# MAGIC
# MAGIC Look for `__START_AT` and `__END_AT` columns:
# MAGIC - **Current rows**: `__END_AT` is NULL
# MAGIC - **Historical rows**: `__END_AT` is set (closed by an Update or Delete)

# COMMAND ----------

# DBTITLE 1,Silver Customers — SCD Type 2 History
display(spark.sql(f"""
  SELECT customer_id, full_name, email, address,
         __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.customers
  ORDER BY customer_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Silver Transactions — SCD Type 2 History
display(spark.sql(f"""
  SELECT transaction_id, customer_id, product_id,
         store_id, __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.transactions
  ORDER BY transaction_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Silver Products — SCD Type 2 History
display(spark.sql(f"""
  SELECT product_id, name, price,
         __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.products
  ORDER BY product_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Silver Stores — SCD Type 2 History
display(spark.sql(f"""
  SELECT store_id, address, __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.stores
  ORDER BY store_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 8: Append Flow — Multi-Source Ingestion
# MAGIC
# MAGIC **Append Flow** reads from **multiple source paths** and writes to the
# MAGIC **same target table** using `dp.append_flow`. This enables:
# MAGIC - Consolidating data from multiple upstream systems
# MAGIC - Adding **file metadata columns** (file name, file path)
# MAGIC - Gradually adding new data feeds without schema changes
# MAGIC
# MAGIC ```
# MAGIC Source Path 1 (orders/)
# MAGIC   │
# MAGIC   ├─── Autoloader ──► bronze_orders ◄── Autoloader ──── Source Path 2 (orders_af/)
# MAGIC   │                        │                                  (append_flow)
# MAGIC   │                        │
# MAGIC   │              DQE + apply_changes
# MAGIC   │                        │
# MAGIC   │                   silver_orders
# MAGIC ```
# MAGIC
# MAGIC See: [cloudfiles-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Append Flow Onboarding
# MAGIC
# MAGIC The `bronze_append_flows` list defines additional sources that
# MAGIC feed into the same bronze target using `dp.append_flow`.
# MAGIC `source_metadata` adds file name/path to each record.

# COMMAND ----------

append_flow_feed = {
    "data_flow_id": "200",
    "data_flow_group": "A1",
    "source_system": "MYSQL",
    "source_format": "cloudFiles",
    "source_details": {
        "source_database": "APP",
        "source_table": "ORDERS",
        "source_path_prod": f"{af_data_path}/orders",
        "source_metadata": {
            "include_autoloader_metadata_column": "True",
            "autoloader_metadata_col_name": "source_metadata",
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path",
            },
        },
        "source_schema_path": f"{ddl_path}/af_orders.ddl",
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "orders",
    "bronze_reader_options": {
        "cloudFiles.format": "json",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
    },
    "bronze_cluster_by_auto": True,
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/af_orders_bronze_dqe.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "orders_quarantine",
    "bronze_append_flows": [
        {
            "name": "orders_bronze_append_flow",
            "create_streaming_table": False,
            "source_format": "cloudFiles",
            "source_details": {
                "source_path_prod": (
                    f"{af_data_path}/orders_af"
                ),
                "source_schema_path": (
                    f"{ddl_path}/af_orders.ddl"
                ),
            },
            "reader_options": {
                "cloudFiles.format": "json",
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.rescuedDataColumn":
                    "_rescued_data",
            },
            "once": False,
        },
    ],
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "orders",
    "silver_cdc_apply_changes": {
        "keys": ["order_id"],
        "sequence_by": "operation_date",
        "scd_type": "1",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": [
            "operation", "operation_date", "_rescued_data",
        ],
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}/af_silver_transformations.{conf_ext}"
    ),
    "silver_data_quality_expectations_json_prod": (
        f"{dqe_path}/af_orders_silver_dqe.{conf_ext}"
    ),
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.append(append_flow_feed)

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print("  Added: orders (200) with append_flow from orders_af")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("Onboarding updated with append flow orders!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Re-run the Pipeline
# MAGIC
# MAGIC The pipeline now also processes the **orders** table from two
# MAGIC sources via append flow.

# COMMAND ----------

with open(pipeline_id_file, "r") as fh:
    pipeline_id = fh.read().strip()
run_pipeline_and_wait(w, pipeline_id, label="append flow orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Validate Append Flow Results
# MAGIC
# MAGIC Both source paths feed into the same `orders` table.
# MAGIC The `source_metadata` column shows which file each record came from.

# COMMAND ----------

# DBTITLE 1,Bronze Orders (from both sources via append_flow)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".orders LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze Orders — Row Count
display(
    spark.sql(
        f"SELECT count(*) as total_rows "
        f"FROM {uc_catalog_name}.{bronze_schema}.orders"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Orders
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".orders LIMIT 20"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 9: Apply Changes From Snapshot
# MAGIC
# MAGIC **Snapshot ingestion** uses `apply_changes_from_snapshot` to process
# MAGIC full point-in-time snapshots instead of streaming CDC events.
# MAGIC
# MAGIC - **SCD Type 2** (products): Updated records get expired, new
# MAGIC   versions added
# MAGIC - **SCD Type 1** (stores): Latest snapshot overwrites previous;
# MAGIC   missing records are deleted
# MAGIC
# MAGIC ```
# MAGIC Snapshot CSVs (LOAD_1, LOAD_2, ...)
# MAGIC   │
# MAGIC   ├── next_snapshot_and_version() callback
# MAGIC   │   reads versioned CSV files
# MAGIC   │
# MAGIC   ├── Bronze (apply_changes_from_snapshot)
# MAGIC   │       SCD Type 2: old versions expired
# MAGIC   │       SCD Type 1: overwrite
# MAGIC   │
# MAGIC   └── Silver (apply_changes_from_snapshot)
# MAGIC ```
# MAGIC
# MAGIC See: [snapshot-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/snapshot-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Create Source Delta Table for Products Snapshots
# MAGIC
# MAGIC Products uses a **delta source table** for snapshot ingestion.
# MAGIC We load the initial CSV snapshot into a delta table.

# COMMAND ----------

snap_products_source_db = f"{uc_catalog_name}.{uc_schema_name}"
snap_products_source_table = "source_products_delta"

df = spark.read.format("csv").option("header", "true").load(
    f"{snapshot_data_path}/products/LOAD_1.csv"
)
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{snap_products_source_db}.{snap_products_source_table}"
)
print(
    f"Source delta table created: "
    f"{snap_products_source_db}.{snap_products_source_table}"
)
display(spark.sql(
    f"SELECT * FROM {snap_products_source_db}"
    f".{snap_products_source_table}"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Snapshot Onboarding
# MAGIC
# MAGIC - **Products** (data_flow_id 301): delta source with SCD Type 2
# MAGIC - **Stores** (data_flow_id 302): CSV snapshots with SCD Type 1

# COMMAND ----------

snap_products_feed = {
    "data_flow_id": "301",
    "data_flow_group": "SNAP",
    "source_system": "delta",
    "source_format": "snapshot",
    "source_details": {
        "snapshot_format": "delta",
        "source_catalog_prod": uc_catalog_name,
        "source_table": snap_products_source_table,
        "source_database": uc_schema_name,
    },
    # Snapshot feeds use a fully-qualified "catalog.schema" string in
    # bronze_database_prod (no separate bronze_catalog_prod field).
    # This differs from cloudFiles feeds which split catalog and schema.
    "bronze_database_prod": (
        f"{uc_catalog_name}.{bronze_schema}"
    ),
    "bronze_table": "snap_products",
    "bronze_apply_changes_from_snapshot": {
        "keys": ["product_id"],
        "scd_type": "2",
    },
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "snap_products",
    "silver_table_comment": "products from snapshot SCD2",
    "silver_apply_changes_from_snapshot": {
        "keys": ["product_id"],
        "scd_type": "2",
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}"
        f"/snapshot_silver_transformations.{conf_ext}"
    ),
}

snap_stores_feed = {
    "data_flow_id": "302",
    "data_flow_group": "SNAP",
    "source_system": "delta",
    "source_format": "snapshot",
    "source_details": {
        "source_path_prod": (
            f"{snapshot_data_path}/stores/LOAD_"
        ),
        "snapshot_format": "csv",
    },
    "bronze_reader_options": {"header": "true"},
    # Snapshot feeds use a fully-qualified "catalog.schema" string in
    # bronze_database_prod (no separate bronze_catalog_prod field).
    # This differs from cloudFiles feeds which split catalog and schema.
    "bronze_database_prod": (
        f"{uc_catalog_name}.{bronze_schema}"
    ),
    "bronze_table": "snap_stores",
    "bronze_apply_changes_from_snapshot": {
        "keys": ["store_id"],
        "scd_type": "1",
    },
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "snap_stores",
    "silver_apply_changes_from_snapshot": {
        "keys": ["store_id"],
        "scd_type": "1",
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}"
        f"/snapshot_silver_transformations.{conf_ext}"
    ),
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.extend([snap_products_feed, snap_stores_feed])

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print(
    "  Added: snap_products (301, SCD2), "
    "snap_stores (302, SCD1)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("Onboarding updated with snapshot feeds!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.4 Create Snapshot Runner Notebook & Pipeline
# MAGIC
# MAGIC Snapshot ingestion requires a `next_snapshot_and_version` callback
# MAGIC passed to `invoke_dlt_pipeline`. A separate runner notebook and
# MAGIC pipeline are created for the **SNAP** data-flow group.

# COMMAND ----------

snapshot_notebook_name = "sdp_meta_snapshot_runner"

snapshot_runner_content = (
    "# Databricks notebook source\n"
    'sdp_meta_whl = spark.conf.get("sdp_meta_whl")\n'
    "%pip install $sdp_meta_whl  # noqa: E999\n"
    "\n"
    "# COMMAND ----------\n"
    "\n"
    "# DBTITLE 1,Snapshot reader for apply_changes_from_snapshot\n"
    "import dlt\n"
    "from databricks.labs.sdp_meta.dataflow_spec "
    "import BronzeDataflowSpec\n"
    "\n"
    "\n"
    "def exist(path):\n"
    "    try:\n"
    "        return dbutils.fs.ls(path) is not None\n"
    "    except Exception:\n"
    "        return False\n"
    "\n"
    "\n"
    "def next_snapshot_and_version("
    "latest_snapshot_version, dataflow_spec):\n"
    "    latest_snapshot_version = "
    "latest_snapshot_version or 0\n"
    "    next_version = latest_snapshot_version + 1\n"
    "    bronze_dataflow_spec: BronzeDataflowSpec "
    "= dataflow_spec\n"
    "    options = bronze_dataflow_spec"
    ".readerConfigOptions\n"
    "    snapshot_format = bronze_dataflow_spec"
    '.sourceDetails["snapshot_format"]\n'
    "    snapshot_root_path = bronze_dataflow_spec"
    ".sourceDetails['path']\n"
    '    snapshot_path = f"{snapshot_root_path}'
    '{next_version}.csv"\n'
    "    if exist(snapshot_path):\n"
    "        snapshot = spark.read.format("
    "snapshot_format).options(**options)"
    ".load(snapshot_path)\n"
    "        return (snapshot, next_version)\n"
    "    else:\n"
    "        return None\n"
    "\n"
    "\n"
    "# COMMAND ----------\n"
    "\n"
    'layer = spark.conf.get("layer", None)\n'
    "\n"
    "from databricks.labs.sdp_meta.dataflow_pipeline "
    "import DataflowPipeline\n"
    "DataflowPipeline.invoke_dlt_pipeline(\n"
    "    spark, layer,\n"
    "    bronze_next_snapshot_and_version="
    "next_snapshot_and_version,\n"
    "    silver_next_snapshot_and_version=None,\n"
    ")\n"
)

snapshot_encoded = base64.b64encode(
    snapshot_runner_content.encode("utf-8")
).decode("utf-8")

w.workspace.import_(
    content=snapshot_encoded,
    path=f"{notebook_dir}/{snapshot_notebook_name}",
    format=ExportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)

snapshot_runner_path = f"{notebook_dir}/{snapshot_notebook_name}"
print(f"Snapshot runner notebook created: {snapshot_runner_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.5 Create & Start Snapshot Pipeline
# MAGIC
# MAGIC The pipeline processes the initial snapshot (LOAD_1).

# COMMAND ----------

snapshot_pipeline_name = f"sdp_meta_demo_snapshot_{uc_schema_name}"
snapshot_pipeline_id_file = (
    f"{uc_volume_path}/snapshot_pipeline_id.txt"
)

snapshot_pipeline_config = {
    "layer": "bronze_silver",
    "bronze.group": "SNAP",
    "silver.group": "SNAP",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.bronze_dataflowspec"
    ),
    "silver.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.silver_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

existing_snap = [
    p for p in w.pipelines.list_pipelines()
    if p.name == snapshot_pipeline_name
]
if existing_snap:
    snapshot_pipeline_id = existing_snap[0].pipeline_id
    print(
        f"Reusing existing snapshot pipeline: "
        f"{snapshot_pipeline_id}"
    )
else:
    created_snap = w.pipelines.create(
        name=snapshot_pipeline_name,
        catalog=uc_catalog_name,
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path=snapshot_runner_path
                )
            )
        ],
        configuration=snapshot_pipeline_config,
        development=True,
        serverless=True
    )
    snapshot_pipeline_id = created_snap.pipeline_id
    print(f"Snapshot pipeline created: {snapshot_pipeline_id}")

with open(snapshot_pipeline_id_file, "w") as fh:
    fh.write(snapshot_pipeline_id)
print(
    f"Snapshot pipeline ID saved to: "
    f"{snapshot_pipeline_id_file}"
)

run_pipeline_and_wait(
    w, snapshot_pipeline_id, label="snapshot initial load"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.6 Validate Initial Snapshots

# COMMAND ----------

# DBTITLE 1,Bronze snap_products (Snapshot SCD Type 2)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".snap_products LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze snap_stores (Snapshot SCD Type 1)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".snap_stores LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver snap_products
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".snap_products LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver snap_stores
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".snap_stores LIMIT 20"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.7 Load Next Snapshot (Version 2)
# MAGIC
# MAGIC Simulate arrival of a new snapshot:
# MAGIC - Products: some items renamed (e.g., `shorts` → `shorts_v2`)
# MAGIC - Stores: only 2 of 4 stores remain (SCD1 will delete missing)

# COMMAND ----------

dbutils.fs.cp(
    f"{snapshot_data_path}/incremental_snapshots/"
    "stores/LOAD_2.csv",
    f"{snapshot_data_path}/stores/LOAD_2.csv",
    True,
)
print("Copied stores LOAD_2.csv")

df2 = spark.read.format("csv").option(
    "header", "true"
).load(
    f"{snapshot_data_path}/incremental_snapshots/"
    "products/LOAD_2.csv"
)
df2.write.format("delta").mode("overwrite").saveAsTable(
    f"{snap_products_source_db}.{snap_products_source_table}"
)
print("Updated source_products_delta with LOAD_2 data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.8 Re-run the Snapshot Pipeline (Snapshot V2)

# COMMAND ----------

with open(snapshot_pipeline_id_file, "r") as fh:
    snapshot_pipeline_id = fh.read().strip()
run_pipeline_and_wait(
    w, snapshot_pipeline_id, label="snapshot V2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.9 Validate Snapshot V2 Changes

# COMMAND ----------

# DBTITLE 1,Bronze snap_products — SCD Type 2 History
display(spark.sql(f"""
  SELECT product_id, name, price,
         __START_AT, __END_AT
  FROM {uc_catalog_name}.{bronze_schema}.snap_products
  ORDER BY product_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Bronze snap_stores — SCD Type 1 (latest only)
display(spark.sql(f"""
  SELECT *
  FROM {uc_catalog_name}.{bronze_schema}.snap_stores
  LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Silver snap_products (current rows only via where_clause)
display(spark.sql(f"""
  SELECT *
  FROM {uc_catalog_name}.{silver_schema}.snap_products
  LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Silver snap_stores
display(spark.sql(f"""
  SELECT *
  FROM {uc_catalog_name}.{silver_schema}.snap_stores
  LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 10: Pipeline Sink — Write to External Delta
# MAGIC
# MAGIC **Pipeline Sink** writes pipeline output to **external destinations**
# MAGIC (delta, kafka) using `dp.create_sink` + `dp.append_flow`.
# MAGIC This stage demonstrates writing IoT events to an external
# MAGIC delta table location.
# MAGIC
# MAGIC ```
# MAGIC Source (CSV via Autoloader)
# MAGIC   │
# MAGIC   ├─── Bronze Table (iot_events)
# MAGIC   │         │
# MAGIC   │         ├── DQE (expect_or_drop / quarantine)
# MAGIC   │         │
# MAGIC   │         └── Sink: dp.create_sink(format="delta")
# MAGIC   │                   writes to external Volume path
# MAGIC   │
# MAGIC   └─── Quarantine Table (iot_events_quarantine)
# MAGIC ```
# MAGIC
# MAGIC See: [kafka-sink-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/kafka-sink-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 DLT Sink Onboarding
# MAGIC
# MAGIC `bronze_sinks` defines external destinations. Each sink has a
# MAGIC format, options, select expression, and optional where clause.

# COMMAND ----------

iot_sink_path = (
    f"/Volumes/{uc_catalog_name}/{uc_schema_name}"
    "/config/data/sink/iot_events"
)

iot_sink_feed = {
    "data_flow_id": "400",
    "data_flow_group": "SINK",
    "source_system": "IoT",
    "source_format": "cloudFiles",
    "source_details": {
        "source_path_prod": f"{data_path}/iot_events",
        "source_schema_path": (
            f"{ddl_path}/iot_events.ddl"
        ),
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "iot_events",
    "bronze_reader_options": {
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true",
    },
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/iot_events_bronze_dqe.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "iot_events_quarantine",
    "bronze_sinks": [
        {
            "name": "iot_events_delta_sink",
            "format": "delta",
            "options": {
                "path": iot_sink_path,
            },
            "select_exp": [
                "device_id", "device_name",
                "temp", "humidity",
                "battery_level", "timestamp",
            ],
            "where_clause": "device_id IS NOT NULL",
        },
    ],
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.append(iot_sink_feed)

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print(
    f"  Added: iot_events (400) with delta sink at "
    f"{iot_sink_path}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_bronze_dataflow_spec()
print("Onboarding updated with IoT events + delta sink!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Create & Start Sink Pipeline
# MAGIC
# MAGIC The sink pipeline runs under the **SINK** data-flow group with
# MAGIC its own runner notebook (no snapshot callback needed).

# COMMAND ----------

sink_pipeline_name = (
    f"sdp_meta_demo_sink_{uc_schema_name}"
)
sink_pipeline_id_file = (
    f"{uc_volume_path}/sink_pipeline_id.txt"
)

sink_pipeline_config = {
    "layer": "bronze",
    "bronze.group": "SINK",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}"
        ".bronze_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

existing_sink = [
    p for p in w.pipelines.list_pipelines()
    if p.name == sink_pipeline_name
]
if existing_sink:
    sink_pipeline_id = existing_sink[0].pipeline_id
    print(
        f"Reusing existing sink pipeline: "
        f"{sink_pipeline_id}"
    )
else:
    created_sink = w.pipelines.create(
        name=sink_pipeline_name,
        catalog=uc_catalog_name,
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path=runner_notebook_path
                )
            )
        ],
        configuration=sink_pipeline_config,
        development=True,
    )
    sink_pipeline_id = created_sink.pipeline_id
    print(f"Sink pipeline created: {sink_pipeline_id}")

with open(sink_pipeline_id_file, "w") as fh:
    fh.write(sink_pipeline_id)
print(
    f"Sink pipeline ID saved to: {sink_pipeline_id_file}"
)

run_pipeline_and_wait(
    w, sink_pipeline_id, label="IoT sink"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Validate DLT Sink Results

# COMMAND ----------

# DBTITLE 1,Bronze IoT Events
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".iot_events LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,External Delta Sink — IoT Events
display(
    spark.read.format("delta").load(iot_sink_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 Final Data Flow Summary
# MAGIC
# MAGIC Complete view across all features demonstrated.

# COMMAND ----------

# DBTITLE 1,Complete Pipeline Data Flow Summary
all_tables_final = [
    ("customers", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("transactions", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("products", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("stores", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("orders", "Append Flow + CDC", bronze_schema,
     silver_schema),
    ("snap_products", "Snapshot SCD2", bronze_schema,
     silver_schema),
    ("snap_stores", "Snapshot SCD1", bronze_schema,
     silver_schema),
    ("iot_events", "CloudFiles + Sink", bronze_schema,
     None),
]

summary_rows = []
for table, feature, b_schema, s_schema in all_tables_final:
    bronze_fqn = (
        f"{uc_catalog_name}.{b_schema}.{table}"
    )
    try:
        bronze_count = spark.sql(
            f"SELECT count(*) FROM {bronze_fqn}"
        ).first()[0]
    except Exception:
        bronze_count = 0

    quarantine_fqn = (
        f"{uc_catalog_name}.{b_schema}"
        f".{table}_quarantine"
    )
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0

    silver_count = 0
    if s_schema:
        silver_fqn = (
            f"{uc_catalog_name}.{s_schema}.{table}"
        )
        try:
            silver_count = spark.sql(
                f"SELECT count(*) FROM {silver_fqn}"
            ).first()[0]
        except Exception:
            silver_count = 0

    summary_rows.append(Row(
        Table=table,
        Feature=feature,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Feature | How It Was Used |
# MAGIC |---------|----------------|
# MAGIC | **Metadata-driven onboarding** | JSON config → DataflowSpec → generic pipeline |
# MAGIC | **CloudFiles (Autoloader)** | CSV files ingested with schema enforcement |
# MAGIC | **Data quality** | `expect_or_drop` and `expect_or_quarantine` |
# MAGIC | **Quarantine tables** | Bad records routed to separate tables |
# MAGIC | **CDC (SCD Type 2)** | `apply_changes` with keys, sequence_by |
# MAGIC | **Liquid clustering** | `cluster_by_auto` for automatic optimization |
# MAGIC | **Silver transformations** | Column selection and expressions via JSON |
# MAGIC | **Adding new feeds** | Products & Stores added — no pipeline changes |
# MAGIC | **Incremental processing** | CDC data (I/U/D) processed automatically |
# MAGIC | **Append Flow** | Multi-source → same target via `dp.append_flow` |
# MAGIC | **File metadata** | `_metadata.file_name`, `_metadata.file_path` |
# MAGIC | **Apply Changes From Snapshot** | Snapshot-based SCD Type 1 & 2 |
# MAGIC | **Pipeline Sink** | Write to external delta table via `dp.create_sink` |
# MAGIC
# MAGIC ### Learn More
# MAGIC - [Full Documentation](https://databrickslabs.github.io/dlt-meta/)
# MAGIC - [Getting Started](https://databrickslabs.github.io/dlt-meta/getting_started/)
# MAGIC - [Onboarding Template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/onboarding.template)
# MAGIC - [Source Code](https://github.com/databrickslabs/dlt-meta/tree/main/src/databricks/labs/sdp_meta)
# MAGIC - [Append Flows](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template)
# MAGIC - [Apply Changes from Snapshot](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/snapshot-onboarding.template)
# MAGIC - [DLT Sink](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/kafka-sink-onboarding.template)
# MAGIC - [DABs](https://github.com/databrickslabs/dlt-meta/tree/main/demo/dabs)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment and run the cells below to clean up all demo resources.

# COMMAND ----------

# -- Delete pipelines --
# for pid_file in [
#     pipeline_id_file,
#     snapshot_pipeline_id_file,
#     sink_pipeline_id_file,
# ]:
#     try:
#         with open(pid_file, "r") as fh:
#             pid = fh.read().strip()
#         w.pipelines.delete(pipeline_id=pid)
#         print(f"Deleted pipeline: {pid}")
#     except Exception as e:
#         print(f"Skipping {pid_file}: {e}")
#
# -- Delete runner notebooks --
# for nb_path in [runner_notebook_path, snapshot_runner_path]:
#     try:
#         w.workspace.delete(nb_path)
#         print(f"Deleted notebook: {nb_path}")
#     except Exception as e:
#         print(f"Skipping {nb_path}: {e}")
#
# -- Drop schemas and catalog --
# spark.sql(f"DROP SCHEMA IF EXISTS {uc_catalog_name}.{bronze_schema} CASCADE")
# spark.sql(f"DROP SCHEMA IF EXISTS {uc_catalog_name}.{silver_schema} CASCADE")
# spark.sql(f"DROP SCHEMA IF EXISTS {uc_catalog_name}.{pipeline_target_schema} CASCADE")
# spark.sql(f"DROP SCHEMA IF EXISTS {uc_catalog_name}.{uc_schema_name} CASCADE")
# spark.sql(f"DROP CATALOG IF EXISTS {uc_catalog_name} CASCADE")
# print(f"Cleaned up all demo resources for: {uc_catalog_name}")
