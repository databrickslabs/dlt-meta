 # [SDP-META](https://github.com/databrickslabs/dlt-meta) DEMOs
 1. [Interactive Demo (Notebook)](#interactive-demo-notebook): **Start here.** A fully self-contained Databricks notebook covering all SDP-META features end-to-end — no CLI required.
 2. [DAIS 2023 DEMO](#dais-2023-demo): Showcases SDP-META's capabilities of creating Bronze and Silver pipelines with initial and incremental mode automatically.
 3. [Databricks Techsummit Demo](#databricks-tech-summit-fy2024-demo): 100s of data sources ingestion in bronze and silver pipelines automatically.
 4. [Append FLOW Autoloader Demo](#append-flow-autoloader-file-metadata-demo): Write to same target from multiple sources using [dp.append_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) and adding [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
 5. [Append FLOW Eventhub Demo](#append-flow-eventhub-demo): Write to same target from multiple sources using [dp.append_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) and adding [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
 6. [Silver Fanout Demo](#silver-fanout-demo): This demo showcases the implementation of fanout architecture in the silver layer.
 7. [Apply Changes From Snapshot Demo](#apply-changes-from-snapshot-demo): This demo showcases the implementation of ingesting from snapshots in bronze layer
 8. [Lakeflow Spark Declarative Pipelines Sink Demo](#lakeflow-declarative-pipelines-sink-demo): This demo showcases the implementation of write to external sinks like delta and kafka
 9. [DAB Demo](#dab-demo): End-to-end walkthrough of the `databricks labs sdp-meta bundle-*` CLI — scaffold a Declarative Automation Bundle, append flows, validate, deploy, and run onboarding + Lakeflow Spark Declarative Pipelines from one driver script. See [`DAB_README.md`](../DAB_README.md) for the full CLI / template / recipe reference.


# Interactive Demo (Notebook)

**Recommended starting point** — a single Databricks notebook that walks through all SDP-META features
end-to-end with no CLI setup required.

**Notebook:** [`demo/SDP_META_INTERACTIVE_DEMO.py`](SDP_META_INTERACTIVE_DEMO.py)

## What It Covers

| Stage | Feature |
|-------|---------|
| 1 | Setup — UC catalog, schemas, volume, config files, synthetic data |
| 2 | Onboarding — JSON → DataflowSpec tables (`bronze_dataflowspec`, `silver_dataflowspec`) |
| 3 | Pipeline creation and first run (fully automated via Databricks SDK) |
| 4 | Validate initial Bronze + Silver tables, quarantine tables, SCD Type 2 history |
| 5 | Add new feeds (Products & Stores) without modifying the pipeline |
| 6 | Incremental CDC load (Insert / Update / Delete) |
| 7 | Validate incremental results — `__START_AT` / `__END_AT` history |
| 8 | Append Flow — multi-source ingestion with file metadata columns |
| 9 | Apply Changes From Snapshot — SCD Type 1 & 2 from CSV/Delta snapshots |
| 10 | DLT Sink — write Bronze output to an external Delta table |

## Features Demonstrated

- Metadata-driven onboarding (JSON → DataflowSpec → generic pipeline)
- CloudFiles (Autoloader) ingestion with schema enforcement
- Data quality rules: `expect_or_drop` and `expect_or_quarantine`
- Quarantine tables for bad records
- CDC with `apply_changes` (SCD Type 2)
- Liquid clustering (`cluster_by_auto`)
- Silver transformations via JSON (column selection, expressions)
- Adding new feeds without pipeline code changes
- `dp.append_flow` — multiple sources → same target table 
- `_metadata.file_name` / `_metadata.file_path` file metadata columns
- `apply_changes_from_snapshot` — snapshot-based SCD Type 1 & 2
- `dp.create_sink` — write to external Delta destinations

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A UC catalog you have `CREATE` privileges on

## Steps

1. Import the notebook into your Databricks workspace:
   - In the sidebar click **Workspace** → **Import**
   - Upload `demo/SDP_META_INTERACTIVE_DEMO.py` or paste the GitHub raw URL

2. Open the notebook and fill in the widgets at the top:

   | Widget | Default | Description |
   |--------|---------|-------------|
   | `git_branch` | `main` | Branch to install SDP-META from |
   | `uc_catalog_name` | `sdp_meta_demo` | UC catalog for the demo |
   | `uc_schema_name` | `retail_data` | Schema within the catalog |
   | `data_source` | `dbdatagen` | `dbdatagen` (synthetic) or `github` (download from repo) |

3. Click **Run All**. The notebook:
   - Installs SDP-META and (if selected) `dbldatagen` via `%pip install`
   - Creates all UC resources, config files, and demo data automatically
   - Creates and starts the Lakeflow Spark Declarative Pipeline via the Databricks SDK
   - Blocks and polls until each pipeline run completes before moving to the next stage
   - Prints live pipeline state updates and the pipeline URL for each run

> No manual pipeline UI interactions required — the notebook is fully automated end-to-end.

## Data Source Options

| Option | Description |
|--------|-------------|
| `dbdatagen` | Generates synthetic retail data using [dbldatagen](https://github.com/databrickslabs/dbldatagen). No internet access required after install. |
| `github` | Downloads sample CSV data directly from the [dlt-meta repo](https://github.com/databrickslabs/dlt-meta/tree/main/demo/resources). Requires outbound internet access from the cluster. |

## Cleanup

Uncomment and run the cleanup cell at the bottom of the notebook to drop all schemas and the catalog created during the demo.

---

# DAIS 2023 DEMO
## [DAIS 2023 Session Recording](https://www.youtube.com/watch?v=WYv5haxLlfA)
This Demo launches Bronze and Silver pipelines with following activities:
- Customer and Transactions feeds for initial load
- Adds new feeds Product and Stores to existing Bronze and Silver Lakeflow Declarative pipeline with metadata changes.
- Runs Bronze and Silver pipeline for incremental load for CDC events

### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. Clone sdp-meta:
    ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

5. ```commandline
    cd sdp-meta
    ```

6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```

    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```

7. ```commandline
    python demo/launch_dais_demo.py --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
    ```
    - uc_catalog_name : Unity catalog name
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token.

    ![dais_demo.png](../docs/static/images/dais_demo.png)

# Databricks Tech Summit FY2024 DEMO:
This demo will launch auto generated tables(100s) inside single bronze and silver pipeline using sdp-meta.

1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

5. ```commandline
    cd sdp-meta
    ```

6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```

    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```

7. ```commandline
    python demo/launch_techsummit_demo.py --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
    ```
    - uc_catalog_name : Unity catalog name
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

    ![tech_summit_demo.png](../docs/static/images/tech_summit_demo.png)


# Append Flow Autoloader file metadata demo:
This demo will perform following tasks:
- Read from different source paths using autoloader and write to same target using append_flow API
- Read from different delta tables and write to same silver table using append_flow API
- Add file_name and file_path to target bronze table for autoloader source using [File metadata column](https://docs.databricks.com/en/ingestion/file-metadata-column.html)

1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

5. ```commandline
    cd sdp-meta
    ```

6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```

    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```

7. ```commandline
    python demo/launch_af_cloudfiles_demo.py --uc_catalog_name=<<uc catalog name>> --source=cloudfiles --profile=<<DEFAULT>>
    ```
    - uc_catalog_name : Unity Catalog name
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

![af_am_demo.png](../docs/static/images/af_am_demo.png)

# Append Flow Eventhub demo:
- Read from different eventhub topics and write to same target tables using append_flow API

### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

5. ```commandline
    cd sdp-meta
    ```
6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```
    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```
6. Eventhub
- Needs eventhub instance running
- Need two eventhub topics first for main feed (eventhub_name) and second for append flow feed (eventhub_name_append_flow)
- Create databricks secrets scope for eventhub keys
    - ```
            commandline databricks secrets create-scope eventhubs_sdp_meta_creds
        ```
    - ```commandline
            databricks secrets put-secret --json '{
                "scope": "eventhubs_sdp_meta_creds",
                "key": "RootManageSharedAccessKey",
                "string_value": "<<value>>"
                }'
        ```
- Create databricks secrets to store producer and consumer keys using the scope created in step 2

- Following are the mandatory arguments for running EventHubs demo
    - uc_catalog_name : unity catalog name e.g. ravi_sdp_meta_uc
    - eventhub_namespace: Eventhub namespace e.g. sdp_meta
    - eventhub_name : Primary Eventhubname e.g. sdp_meta_demo
    - eventhub_name_append_flow: Secondary eventhub name for appendflow feed e.g. sdp_meta_demo_af
    - eventhub_producer_accesskey_name: Producer databricks access keyname e.g. RootManageSharedAccessKey
    - eventhub_consumer_accesskey_name: Consumer databricks access keyname e.g. RootManageSharedAccessKey
    - eventhub_secrets_scope_name: Databricks secret scope name e.g. eventhubs_sdp_meta_creds
    - eventhub_port: Eventhub port

7. ```commandline
    python3 demo/launch_af_eventhub_demo.py --uc_catalog_name=<<uc catalog name>> --eventhub_name=sdp_meta_demo --eventhub_name_append_flow=sdp_meta_demo_af --eventhub_secrets_scope_name=sdp_meta_eventhub_creds --eventhub_namespace=sdp_meta --eventhub_port=9093 --eventhub_producer_accesskey_name=RootManageSharedAccessKey --eventhub_consumer_accesskey_name=RootManageSharedAccessKey --eventhub_accesskey_secret_name=RootManageSharedAccessKey --profile=<<DEFAULT>>
    ```

  ![af_eh_demo.png](../docs/static/images/af_eh_demo.png)


# Silver Fanout Demo
- This demo will showcase the onboarding process for the silver fanout pattern.
    - Run the onboarding process for the bronze cars table, which contains data from various countries.
    - Run the onboarding process for the silver tables, which have a `where_clause` based on the country condition specified in [`demo/conf/json/silver_transformations_cars.json`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/silver_transformations_cars.json) (or its YAML sibling [`demo/conf/yml/silver_transformations_cars.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations_cars.yml)).
    - Run the Bronze pipeline which will produce cars table.
    - Run Silver pipeline, fanning out from the bronze cars table to country-specific tables such as cars_usa, cars_uk, cars_germany, and cars_japan.

### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

5. ```commandline
    cd sdp-meta
    ```
6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```
    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```

6. Run the command 
    ```commandline
    python demo/launch_silver_fanout_demo.py --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
    ```

    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token.

    a. Databricks Workspace URL:
       Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com. To get your workspace URL, see Workspace instance names, URLs, and IDs.

    b. Token:
        - In your Databricks workspace, click your Databricks username in the top bar, and then select User Settings from the drop down.

        - On the Access tokens tab, click Generate new token.

        - (Optional) Enter a comment that helps you to identify this token in the future, and change the token’s default lifetime of 90 days. To create a token with no lifetime (not recommended), leave the Lifetime (days) box empty (blank).

        - Click Generate.

        - Copy the displayed token

        - Paste to command prompt

    ![silver_fanout_workflow.png](../docs/static/images/silver_fanout_workflow.png)
    
    ![silver_fanout_dlt.png](../docs/static/images/silver_fanout_dlt.png)

# Apply Changes From Snapshot Demo
  - This demo will perform following steps
    - Showcase onboarding process for apply changes from snapshot pattern([snapshot-onboarding.template](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/snapshot-onboarding.template))
    - Run onboarding for the bronze stores and products tables, which contains data snapshot data in csv files.
    - Create source delta table for products
    - Run Bronze Pipeline to load initial snapshot for stores(LOAD_1.csv) and products delta table
    - Run Silver Pipeline to ingest bronze data using apply_changes_from_snapshot API
    - Upload incremental snapshot LOAD_2.csv version=2 for stores and load products delta table for next snapshot
    - Run Bronze Pipeline to load incremental snapshot (LOAD_2.csv). Products is scd_type=2 so updated records will expired and added new records with version_number. Stores is scd_type=1 so in case records missing for scd_type=1 will be deleted.
    - Run Silver Pipeline to ingest bronze data using apply_changes_from_snapshot API
    -  Upload incremental snapshot LOAD_3.csv version=2 for stores and load products delta table for next snapshot
    - Run Bronze Pipeline to load incremental snapshot (LOAD_2.csv). Products is scd_type=2 so updated records will expired and added new records with version_number. Stores is scd_type=1 so in case records missing for scd_type=1 will be deleted.
    - Run Silver Pipeline to ingest bronze data using apply_changes_from_snapshot API
### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git 
    ```

5. ```commandline
    cd sdp-meta
    ```
6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```
    ```commandline
    export PYTHONPATH=$sdp_meta_home

6. Run the command 
    ```commandline
    python demo/launch_acfs_demo.py --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
    ```
    ![acfs.png](../docs/static/images/acfs.png)

# Lakeflow Spark Declarative Pipelines Sink Demo
  - This demo will perform following steps
    - Showcase onboarding process for dlt writing to external sink pattern
    - Run onboarding for the bronze iot events.
    - Publish test events to kafka topic
    - Run Bronze Lakeflow Spark Declarative Pipelines which will read from kafka source topic and write to
        - events delta table into uc
        - create quarantine table as per data quality expectations
        - writes to external kafka topics
        - writes to external dbfs location as external delta sink
### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. Install Python package requirements:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   pip install delta-spark==3.0.0 pyspark==3.5.5
   ```

4. ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git 
    ```

5. ```commandline
    cd sdp-meta
    ```
6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```
    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```

6. Optional: if you are using secrets for kafka. Create databricks secrets scope for source and sink kafka using below command
     ```commandline 
    databricks secrets create-scope <<name>>
     ```
     ```commandline
    databricks secrets put-secret --json '{
        "scope": "<<name>>",
        "key": "<<keyname>>",
        "string_value": "<<value>>"
        }'
     ```

7. Run the command 
    ```commandline
    python demo/launch_dlt_sink_demo.py --uc_catalog_name=<<uc_catalog_name>> --source=kafka --kafka_source_topic=<<kafka source topic name>>>> --kafka_sink_topic=<<kafka sink topic name>> --kafka_source_servers_secrets_scope_name=<<kafka source servers secret name>> --kafka_source_servers_secrets_scope_key=<<kafka source server secret scope key name>> --kafka_sink_servers_secret_scope_name=<<kafka sink server secret scope key name>> --kafka_sink_servers_secret_scope_key=<<kafka sink servers secret scope key name>> --profile=<<DEFAULT>>
    ```
    ![dlt_demo_sink.png](../docs/static/images/dlt_demo_sink.png)
    ![dlt_delta_sink.png](../docs/static/images/dlt_delta_sink.png)
    ![dlt_kafka_sink.png](../docs/static/images/dlt_kafka_sink.png)


# DAB Demo

## Overview

End-to-end demo for the new `databricks labs sdp-meta bundle-*` CLI commands. One driver script (`demo/launch_dab_template_demo.py`) exercises every stage of the bundle lifecycle against a UC catalog you own:

| Stage | Command | What it does |
| --- | --- | --- |
| 1 | `databricks labs sdp-meta bundle-init` | Scaffold a fresh bundle from the packaged template (onboarding job + Lakeflow Spark Declarative Pipelines + `variables.yml` + recipes). |
| 2 | `databricks labs sdp-meta bundle-prepare-wheel` | Build the local sdp-meta wheel and upload it to a UC volume. The resulting `/Volumes/...` path is auto-pinned into `resources/variables.yml` as `sdp_meta_dependency`. |
| 3 | `databricks labs sdp-meta bundle-add-flow` | Bulk-append flow entries from a CSV (the demo supplies one per scenario under `demo/dab_template_demo/flows/`). |
| 4 | `python recipes/from_*.py` | Run the rendered recipe (one of `from_uc.py`, `from_volume.py`, `from_topics.py`, `from_inventory.py`) to programmatically generate flows from real workspace state. |
| 5 | `databricks labs sdp-meta bundle-validate` | Run `databricks bundle validate` plus sdp-meta-specific sanity checks (layer/topology consistency, `wheel_source` vs `sdp_meta_dependency`, unedited `<your-...>` placeholders, dangling `dataflow_group` references). |
| 6 | `databricks bundle deploy` + `bundle run onboarding` + `bundle run pipelines` | Deploy to the workspace, write the `bronze_dataflowspec` / `silver_dataflowspec` rows, and run the LDP pipelines end-to-end. |

> For the full CLI reference (every prompt, every variable, every recipe, the full flag surface, and how to extend the runner notebook for snapshot / CDC / custom transforms), see [`DAB_README.md`](../DAB_README.md) at the repo root. This demo section is the *runnable* walkthrough; `DAB_README.md` is the *reference*.

The demo supports six scenarios via `--scenario`:

| Scenario | Source | Pipeline mode |
| --- | --- | --- |
| `cloudfiles` | UC volume CSVs (Customers / Transactions / Products / Stores) | `split` (separate bronze + silver LDP pipelines) |
| `cloudfiles_combined` | Same data, same recipe | `combined` (bronze + silver in **one** LDP pipeline) |
| `kafka` | Kafka topic list at `demo/dab_template_demo/topics/kafka_topics.txt` | `split` |
| `eventhub` | Event Hub namespace + topic list | `split` |
| `delta` | Existing UC delta tables | `split` |
| `all` | Runs all of the above sequentially into separate `demo_runs/<scenario>/` dirs | varies |

### Prerequisites

- A Databricks workspace with Unity Catalog enabled, and `CREATE SCHEMA` + `CREATE VOLUME` on the target catalog.
- [`databricks` CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and a profile configured (`databricks auth login --profile <name>`).
- Python 3.10+ with `pip install "PyYAML>=6.0" setuptools databricks-sdk wheel`.

> No PySpark or Delta Spark install is needed — STAGES 1-5 are pure Python and shell out to the `databricks` CLI; STAGE 6 runs the actual workload on Databricks compute.

### Steps

1. **Clone and enter the repo**
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
    cd dlt-meta
    export PYTHONPATH=$(pwd)
    ```

2. **Run the full bundle lifecycle for one scenario** (CloudFiles is the fastest end-to-end path because the demo seeds the UC volume with CSV fixtures for you):
    ```commandline
    python demo/launch_dab_template_demo.py \
        --scenario cloudfiles \
        --uc-catalog-name <your_catalog_name> \
        --uc-schema   sdp_meta_dab_demo_cf \
        --uc-volume   sdp_meta_wheels \
        --apply-prepare-wheel \
        --apply-recipe \
        --apply-deploy \
        --profile <your_profile>
    ```
    Without `--apply-prepare-wheel` / `--apply-recipe` / `--apply-deploy` the demo runs in dry-run mode (no workspace access required) — useful for inspecting the rendered bundle locally before committing to a deploy. Bundles are written to `demo_runs/<scenario>/`.

    > If your network can't reach `pypi.org`, add `--pip-index-url https://pypi.internal.example.com/simple` (or set `$PIP_INDEX_URL`) so `bundle-prepare-wheel` builds against your internal mirror.

3. **Or run all scenarios sequentially:**
    ```commandline
    python demo/launch_dab_template_demo.py \
        --scenario all \
        --uc-catalog-name <your_catalog_name> \
        --apply-prepare-wheel --apply-recipe --apply-deploy \
        --profile <your_profile>
    ```

4. **Combined-pipeline variant** (bronze + silver in a single LDP pipeline rather than two):
    ```commandline
    python demo/launch_dab_template_demo.py \
        --scenario cloudfiles_combined \
        --uc-catalog-name <your_catalog_name> \
        --uc-schema sdp_meta_dab_demo_cf_combined \
        --uc-volume sdp_meta_wheels \
        --apply-prepare-wheel --apply-recipe --apply-deploy \
        --profile <your_profile>
    ```

5. **Inspect the scaffolded bundle.** After the demo finishes, the rendered bundle lives at `demo_runs/<scenario>/<bundle_name>/`. Open `databricks.yml`, `resources/variables.yml`, `conf/onboarding.yml`, and `notebooks/init_sdp_meta_pipeline.py` to see what got generated. To re-run just the deploy/run portion against an edited bundle, use the standard CLI:
    ```commandline
    cd demo_runs/cloudfiles/<bundle_name>
    databricks labs sdp-meta bundle-validate
    databricks bundle deploy   --target dev --profile <your_profile>
    databricks bundle run onboarding --target dev --profile <your_profile>
    databricks bundle run pipelines  --target dev --profile <your_profile>
    ```
