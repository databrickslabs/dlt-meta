---
title: "Tech Summit DEMO"
date: 2021-08-04T14:25:26-04:00
weight: 22
draft: false
---

### Databricks Tech Summit FY2024 DEMO

This demo launches 100+ auto-generated tables in a single bronze and silver Lakeflow Spark Declarative Pipeline using sdp-meta. Data is generated as CSV files and ingested via Autoloader.

---

### Prerequisites

1. **Command prompt** – Terminal or PowerShell

2. **Databricks CLI** – Install and authenticate:
   - [Install Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:
    
    ```commandline
    databricks auth login --profile DEFAULT
    ```

3. **Python packages**:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk

    # Development requirements
    pip install flake8==6.0 delta-spark==3.0.0 pytest>=7.0.0 coverage>=7.0.0 pyspark==3.5.5
    ```

4. **Clone sdp-meta**:
    ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    cd sdp-meta
    ```

5. **Set environment**:
    ```commandline
    export PYTHONPATH=$(pwd)
    ```

---

### Run the Demo

6. **Launch the demo**:
    ```commandline
    python demo/launch_techsummit_demo.py --uc_catalog_name=<catalog> --source=cloudfiles --profile=DEFAULT [--table_count=100] [--table_column_count=5] [--table_data_rows_count=10]
    ```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `uc_catalog_name` | Unity Catalog name (required for initial setup; omit when using `--run_id`) |
| `source` | Must be `cloudfiles` for this demo (omit when using `--run_id`) |
| `profile` | Databricks CLI profile; prompts for host/token if omitted |
| `table_count` | Number of tables (default 100) |
| `table_column_count` | Columns per table (default 5) |
| `table_data_rows_count` | Rows per table (default 10) |
| `run_id` | Resume an existing demo run in incremental mode; presence implies incremental |

**Loading incremental data** (after the initial setup run):
```commandline
python demo/launch_techsummit_demo.py --profile=DEFAULT --run_id=<run_id_from_setup>
```
Alternatively, click **Run now** on the `sdp-meta-techsummit-demo-incremental-<run_id>` job in the Databricks Jobs UI — no CLI needed.

---

### Monitoring Row Counts Per Run

Use `demo/check_run_summary.py` to print a tabular summary of rows generated, processed by bronze, and processed by silver for each setup and incremental run.

```commandline
python demo/check_run_summary.py --profile=DEFAULT --run_id=<run_id_from_setup>
```

**Example output:**

```
Date/Time (UTC)         Type           Status      New CSVs   Generated   Bronze   Silver
─────────────────────────────────────────────────────────────────────────────────────────
2025-03-01 10:00:00     setup          SUCCESS            1          10       10       10
2025-03-01 10:30:00     incremental    SUCCESS            1          10       10       10
2025-03-01 11:00:00     incremental    SUCCESS            1          10       10       10
```

- **New CSVs** — number of CSV files written to the UC Volume in this run's time window
- **Generated** — `New CSVs × table_data_rows_count` (derived from the job's task parameters; no per-file SQL query needed)
- **Bronze / Silver** — `numOutputRows` from `DESCRIBE HISTORY … STREAMING UPDATE` for `table_1`

![tech_summit_demo.png](/images/tech_summit_demo.png)

### What Happens When You Run the Command

**On your laptop (synchronous):**

1. **UC resources created** – Unity Catalog schemas (`sdp_meta_dataflowspecs_demo_*`, `sdp_meta_bronze_demo_*`, `sdp_meta_silver_demo_*`) and a volume are created in your catalog.
2. **Demo files uploaded to UC Volume** – DDL, templates, and config files from `demo/resources` and `demo/conf` are uploaded to the volume.
3. **Notebooks uploaded to Workspace** – `data_generator.py` and `init_dlt_meta_pipeline.py` are uploaded to `/Users/<you>/sdp_meta_techsummit_demo/<run_id>/runners/`.
4. **sdp_meta wheel uploaded** – The `sdp_meta` Python wheel is uploaded to the UC Volume for use by pipeline tasks.
5. **Bronze and silver pipelines created** – Two Lakeflow Declarative Pipelines are created in your workspace.
6. **Job created and started** – A job is created with four tasks and `run_now` is triggered. The job URL opens in your browser.

**When the job runs on Databricks (asynchronous):**

1. **Data generated** – The `data_generator.py` notebook runs on Databricks and uses [dbldatagen](https://github.com/databrickslabs/dbldatagen) to create CSV files (`table_1`, `table_2`, …) in the UC Volume, along with `onboarding.json`, `silver_transformations.json`, and DQE configs.
2. **Metadata onboarded** – The `sdp_meta onboard` step loads metadata into dataflowspec tables from the generated onboarding file.
3. **Bronze pipeline runs** – The bronze pipeline ingests the CSV files via Autoloader into bronze Delta tables.
4. **Silver pipeline runs** – The silver pipeline applies transformations from the metadata and writes to silver tables.

---

### Data Generation Design

The demo uses [dbldatagen's `clone()`](https://databrickslabs.github.io/dbldatagen/public_docs/generating_cdc_data.html) pattern: a base `DataGenerator` spec is defined once, then `clone().build()` is called per table. This avoids repeating column definitions and aligns with [CDC / multi-table patterns](https://databrickslabs.github.io/dbldatagen/public_docs/generating_cdc_data.html). Data is written to CSV and read by the bronze pipeline via Autoloader ([DLT docs](https://databrickslabs.github.io/dbldatagen/public_docs/using_delta_live_tables.html)).
