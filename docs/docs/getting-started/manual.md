---
id: manual
title: Manual Job Setup
sidebar_position: 4
---

# Manual Job Setup

Configure the onboarding job and Declarative Pipeline manually through the Databricks UI or a notebook.

## Onboarding job

### Option 1: Python Wheel job (Databricks UI)

1. In the Databricks sidebar, click **Workflows** then **Create Job**.
2. Set **Type** to **Python wheel**, **Package name** to `databricks_labs_sdp_meta`, and **Entry point** to `run`.
3. Under **Dependent Libraries**, add a PyPI library: `databricks-labs-sdp-meta`.
4. In **Parameters**, select **Keyword arguments → JSON** and paste the following (adjust for your environment):

```json
{
  "onboard_layer": "bronze_silver",
  "database": "<catalog>.<schema>",
  "onboarding_file_path": "/Volumes/<catalog>/<schema>/<volume>/sdp-meta/conf/onboarding.json",
  "silver_dataflowspec_table": "silver_dataflowspec",
  "bronze_dataflowspec_table": "bronze_dataflowspec",
  "import_author": "your-name",
  "version": "v1",
  "uc_enabled": "True",
  "overwrite": "True",
  "env": "dev"
}
```

:::note
The `database` field uses the format `<catalog_name>.<schema_name>` when Unity Catalog is enabled.
:::

5. Click **Save task**, then **Run now**.
6. After the run succeeds, verify the DataflowSpec tables exist in your schema.

### Option 2: Databricks Notebook

1. Create a new notebook and install the package:

```python
%pip install databricks-labs-sdp-meta
```

2. Run the onboarding:

```python
onboarding_params_map = {
    "database": "<catalog>.<schema>",
    "onboarding_file_path": "/Volumes/<catalog>/<schema>/<volume>/sdp-meta/conf/onboarding.json",
    "bronze_dataflowspec_table": "bronze_dataflowspec",
    "silver_dataflowspec_table": "silver_dataflowspec",
    "overwrite": "True",
    "env": "dev",
    "version": "v1",
    "import_author": "your-name"
}

from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()
```

## Creating the Lakeflow Spark Declarative Pipeline

### Create a pipeline notebook

1. Create a new Python notebook with these cells:

```python
%pip install databricks-labs-sdp-meta
```

```python
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

### Create a Bronze pipeline

1. In the sidebar, click **Workflows → Lakeflow Spark Declarative Pipelines → Create Pipeline**.
2. Select the notebook above and add the following **Configuration** parameters:

| Key | Value |
|---|---|
| `layer` | `bronze` |
| `bronze.dataflowspecTable` | `<catalog>.<schema>.bronze_dataflowspec` |
| `bronze.group` | `<your-group-name>` |

3. Set the **Target schema** and click **Create**, then **Start**.

### Create a Silver pipeline

1. Create another pipeline with the same notebook and add:

| Key | Value |
|---|---|
| `layer` | `silver` |
| `silver.dataflowspecTable` | `<catalog>.<schema>.silver_dataflowspec` |
| `silver.group` | `<your-group-name>` |

2. Start after the Bronze pipeline has completed at least one successful run.

### Combined Bronze-Silver pipeline

Set `layer=bronze_silver` and provide both sets of configuration parameters:

| Key | Value |
|---|---|
| `layer` | `bronze_silver` |
| `bronze.dataflowspecTable` | `<catalog>.<schema>.bronze_dataflowspec` |
| `bronze.group` | `<your-group-name>` |
| `silver.dataflowspecTable` | `<catalog>.<schema>.silver_dataflowspec` |
| `silver.group` | `<your-group-name>` |

:::tip
The group name in the pipeline configuration must match the `data_flow_group` value in your onboarding file.
:::
