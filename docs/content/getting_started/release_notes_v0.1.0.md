---
title: "Release Notes v0.0.11"
date: 2026-02-13
weight: 55
---

# SDP-META v0.0.11 Release Notes

**Release Date**: February 2026
**Previous Version**: v0.0.10 (dlt-meta)

> **Important**: Starting with v0.0.11, **DLT-META** has been renamed to **SDP-META** (Spark Declarative Pipelines META). This release includes backward compatibility support for existing users.

---

## What's New

### 1. Project Renamed: DLT-META → SDP-META

The project has been renamed to align with the Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables) rebranding. The new package follows the **Databricks Labs namespace convention** (`databricks.labs.sdp_meta`).

| Component | Before (v0.0.10) | After (v0.0.11) |
| --- | --- | --- |
| PyPI Package | `dlt-meta` | `databricks-labs-sdp-meta` |
| Python Import | `from src.dataflow_pipeline import ...` | `from databricks.labs.sdp_meta.dataflow_pipeline import ...` |
| CLI Command | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Source Layout | `src/dataflow_pipeline.py` (flat) | `src/databricks/labs/sdp_meta/dataflow_pipeline.py` (namespace) |
| PythonWheelTask `package_name` | `dlt_meta` | `databricks_labs_sdp_meta` |
| Pipeline Spark Config Key | `dlt_meta_whl` | `sdp_meta_whl` |
| Runner Notebook | `init_dlt_meta_pipeline.py` | `init_sdp_meta_pipeline.py` |
| Config Key (onboarding) | `dlt_meta_schema` | `sdp_meta_schema` |

### 2. New Feature: `cluster_by_auto` Support

Automatic liquid clustering is now supported via the `cluster_by_auto` property in onboarding configuration. When set to `true`, Databricks automatically determines the optimal clustering columns.

**Supported in:**
- Bronze tables (`bronze_cluster_by_auto`)
- Bronze quarantine tables (`bronze_quarantine_table_cluster_by_auto`)
- Silver tables (`silver_cluster_by_auto`)

**Onboarding configuration example:**

```json
{
    "bronze_cluster_by_auto": true,
    "bronze_quarantine_table_cluster_by_auto": "true",
    "silver_cluster_by_auto": true
}
```

Both boolean (`true`) and string (`"true"`) values are accepted. This can be used alongside or instead of the explicit `cluster_by` column list.

### 3. Backward Compatibility Package

A `dlt-meta` compatibility wrapper package is published alongside the new `databricks-labs-sdp-meta` package. Existing users who run `pip install dlt-meta` will transparently receive `databricks-labs-sdp-meta` as a dependency.

---

## Backward Compatibility: What Works Without Changes

### ✅ `pip install dlt-meta` (still works)

The `dlt-meta` v0.0.11 PyPI package is a thin wrapper that depends on `databricks-labs-sdp-meta>=0.0.11`. Installing it pulls in the new package automatically.

```bash
# This still works — installs databricks-labs-sdp-meta transparently
pip install dlt-meta
```

### ✅ `from dlt_meta import ...` (still works, with deprecation warning)

Old imports via the `dlt_meta` module continue to work. All symbols are re-exported from `databricks.labs.sdp_meta` with `DeprecationWarning`:

```python
# Still works — emits deprecation warning
from dlt_meta.dataflow_pipeline import DataflowPipeline
from dlt_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
from dlt_meta.cli import DLTMeta  # Aliased to SDPMeta
```

### ✅ `databricks labs dlt-meta onboard/deploy` (still works, with deprecation banner)

The old CLI commands forward to the new implementation:

```
============================================================
DEPRECATION NOTICE: 'dlt-meta' CLI is deprecated.
Please use 'databricks labs sdp-meta' instead.
============================================================
```

### ✅ Config key `dlt_meta_schema` (still works, with logged warning)

Legacy configuration keys in `onboarding_job_details.json` are still accepted:

```json
{ "dlt_meta_schema": "my_schema" }
```

A warning is logged recommending the new key `sdp_meta_schema`.

### ✅ Existing onboarding JSON/YAML files

All onboarding configuration files work without modification. Field names like `bronze_cluster_by`, `silver_cdc_apply_changes`, etc. are unchanged.

### ✅ `DataflowPipeline.invoke_dlt_pipeline()` API

The `DataflowPipeline` class and its `invoke_dlt_pipeline()` static method have the same signature and behavior as before. No changes to pipeline logic.

---

## Breaking Changes

### ⚠️ `from src.dataflow_pipeline import ...` — NO LONGER WORKS

**This is the most significant breaking change.** The old flat `src/` package layout has been replaced with a proper Databricks Labs namespace package.

**Old code (v0.0.10):**

```python
# Databricks notebook source
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl

# COMMAND ----------
layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

**New code (v0.0.11):**

```python
# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl

# COMMAND ----------
layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

**Why this can't be shimmed**: The `from src.*` import was never a proper package import — it relied on the Git repository's `src/` folder being on the Python path. The new package uses `setuptools` namespace packages (`databricks.labs.sdp_meta`), which is incompatible with treating `src/` as an importable package.

### ⚠️ Pipeline Spark Config Key Changed

If your Lakeflow Spark Declarative Pipeline configuration passes the wheel path via Spark config, update the key:

| Before | After |
| --- | --- |
| `"dlt_meta_whl": "/path/to/whl"` | `"sdp_meta_whl": "/path/to/whl"` |

### ⚠️ PythonWheelTask `package_name` Changed

If you have Databricks Jobs using `PythonWheelTask` for onboarding:

| Before | After |
| --- | --- |
| `package_name: "dlt_meta"` | `package_name: "databricks_labs_sdp_meta"` |

---

## Migration Guide for Existing Customers

### Scenario 1: Using CLI-deployed pipelines (`databricks labs dlt-meta deploy`)

**No action required.** When you re-deploy using `databricks labs sdp-meta deploy`, the runner notebook is regenerated with the correct imports automatically.

### Scenario 2: Custom runner notebooks (most common)

Update your runner notebook — only 2 lines change:

```diff
  # Databricks notebook source
- dlt_meta_whl = spark.conf.get("dlt_meta_whl")
+ sdp_meta_whl = spark.conf.get("sdp_meta_whl")
  %pip install $sdp_meta_whl

  # COMMAND ----------
  layer = spark.conf.get("layer", None)

- from src.dataflow_pipeline import DataflowPipeline
+ from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
  DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

And update your pipeline configuration to pass the key as `sdp_meta_whl`:

```diff
  configuration = {
      "layer": "bronze",
      "bronze.group": "my_group",
-     "dlt_meta_whl": runner_conf.remote_whl_path,
+     "sdp_meta_whl": runner_conf.remote_whl_path,
  }
```

### Scenario 3: Custom runner notebooks with snapshot processing

```diff
  # Databricks notebook source
- dlt_meta_whl = spark.conf.get("dlt_meta_whl")
+ sdp_meta_whl = spark.conf.get("sdp_meta_whl")
  %pip install $sdp_meta_whl

  # COMMAND ----------
  import dlt
- from src.dataflow_spec import BronzeDataflowSpec
+ from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec

  def exist(path):
      # ... snapshot logic ...

  @dlt.table
  def next_snapshot_and_version():
      # ... snapshot logic ...

  # COMMAND ----------
  layer = spark.conf.get("layer", None)
- from src.dataflow_pipeline import DataflowPipeline
+ from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
  DataflowPipeline.invoke_dlt_pipeline(spark, layer, bronze_next_snapshot_and_version=next_snapshot_and_version)
```

### Scenario 4: Custom runner notebooks with custom transforms

```diff
  layer = spark.conf.get("layer", None)
- from src.dataflow_pipeline import DataflowPipeline
+ from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
  DataflowPipeline.invoke_dlt_pipeline(
      spark, layer,
      bronze_custom_transform_func=my_bronze_transform,
      silver_custom_transform_func=my_silver_transform
  )
```

### Scenario 5: Manual onboarding notebooks

```diff
- from src.onboard_dataflowspec import OnboardDataflowspec
+ from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
  OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()
```

### Scenario 6: PyPI install in notebooks

```diff
  # Before
- %pip install dlt-meta
+ %pip install databricks-labs-sdp-meta
```

Note: `%pip install dlt-meta` still works during the transition period (it installs `databricks-labs-sdp-meta` as a dependency), but we recommend updating to the new name.

### Scenario 7: Declarative Automation Bundles (DAB)

Update your DAB job YAML:

```diff
  python_wheel_task:
-   package_name: "dlt_meta"
+   package_name: "databricks_labs_sdp_meta"
    entry_point: "run"
```

And update schema variable names:

```diff
  variables:
-   sdp_meta_schema:
+   sdp_meta_schema:
      default: "my_dataflowspec_schema"
```

### Scenario 8: `onboarding_job_details.json`

Update the schema key (optional — the old key still works):

```diff
  {
-     "dlt_meta_schema": "my_dataflowspec_schema",
+     "sdp_meta_schema": "my_dataflowspec_schema",
  }
```

---

## Not Ready to Migrate?

If you are not ready to update your notebooks and pipelines:

1. **Pin to the previous version**: Continue using `dlt-meta==0.0.10` on PyPI or the corresponding Git tag.
2. **Use the compat package**: `pip install dlt-meta==0.0.11` installs the wrapper that pulls in `sdp-meta` — old `from dlt_meta import ...` imports work, but `from src.dataflow_pipeline import ...` does not.
3. **Migrate at your own pace**: The `dlt-meta` compatibility wrapper will be maintained for the foreseeable future.

---

## Complete Import Reference

| Old Import (v0.0.10) | New Import (v0.0.11) |
| --- | --- |
| `from src.dataflow_pipeline import DataflowPipeline` | `from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline` |
| `from src.dataflow_spec import BronzeDataflowSpec` | `from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec` |
| `from src.dataflow_spec import SilverDataflowSpec` | `from databricks.labs.sdp_meta.dataflow_spec import SilverDataflowSpec` |
| `from src.onboard_dataflowspec import OnboardDataflowspec` | `from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec` |
| `from src.pipeline_readers import PipelineReaders` | `from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders` |
| `from src.pipeline_writers import AppendFlowWriter` | `from databricks.labs.sdp_meta.pipeline_writers import AppendFlowWriter` |
| `from dlt_meta.cli import DLTMeta` | `from databricks.labs.sdp_meta.cli import SDPMeta` |

---

## Deprecation Timeline

| Phase | Status | Details |
| --- | --- | --- |
| **v0.0.11** (current) | Active | Both `dlt-meta` and `databricks-labs-sdp-meta` packages work. Old `from dlt_meta` imports work with warnings. Old `from src.*` imports **do not work**. |
| **Next minor release** | Planned | `dlt-meta` compat package maintained but no new features. Deprecation warnings escalated. |
| **Future major release** | Planned | `dlt-meta` package frozen at final version. Advance notice will be provided. |

---

## Questions?

- **Migration guide**: [SDP-META Renaming](../sdp_meta_renaming/)
- **Documentation**: [https://databrickslabs.github.io/sdp-meta/](https://databrickslabs.github.io/sdp-meta/)
- **Issues**: [https://github.com/databrickslabs/sdp-meta/issues](https://github.com/databrickslabs/sdp-meta/issues)
