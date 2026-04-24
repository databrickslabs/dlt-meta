---
title: "Migration Guide: DLT-META to SDP-META"
date: 2025-01-01
weight: 50
---

# Migration Guide: DLT-META to SDP-META

## Why the Rename?

The project has been renamed from **DLT-META** (Delta Live Tables META) to **SDP-META** (Spark Declarative Pipelines META) to align with the current Databricks product terminology:

- **Old**: Delta Live Tables (DLT) — deprecated terminology
- **New**: Lakeflow Spark Declarative Pipelines / Spark Declarative Pipelines (SDP)

The rename aligns with the current Databricks product terminology for Lakeflow Spark Declarative Pipelines.

## What Changed

| Component | Old (DLT-META) | New (SDP-META) |
|-----------|----------------|----------------|
| PyPI package | `dlt-meta` | `databricks-labs-sdp-meta` |
| CLI commands | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Python imports | `from dlt_meta import ...` | `from databricks.labs.sdp_meta import ...` |
| Main class | `DLTMeta` | `SDPMeta` |
| Labs install | `databricks labs install dlt-meta` | `databricks labs install sdp-meta` |

## What Did NOT Change

- **Configuration files**: Your existing onboarding JSON/YAML files work without modification.
- **Dataflow spec schema**: Field names (`sourceDetails`, `targetDetails`, etc.) are unchanged.
- **Pipeline behavior**: All bronze/silver pipeline functionality is identical.
- **API methods**: `DataflowPipeline.invoke_dlt_pipeline()` and all other method signatures are unchanged.

## Migration Steps

### 1. Update Installation

Replace:
```bash
databricks labs install dlt-meta
```

With:
```bash
databricks labs install sdp-meta
```

Or via pip:
```bash
# Old
pip install dlt-meta

# New
pip install databricks-labs-sdp-meta
```

### 2. Update CLI Commands

Replace:
```bash
databricks labs dlt-meta onboard
databricks labs dlt-meta deploy
```

With:
```bash
databricks labs sdp-meta onboard
databricks labs sdp-meta deploy
```

### 3. Update Python Imports

```python
# Old imports (deprecated but still work)
from dlt_meta.cli import DLTMeta
from dlt_meta import DataflowPipeline

# New imports (recommended)
from databricks.labs.sdp_meta.cli import SDPMeta
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
```

### 4. Update Runner Notebooks

If you have custom runner notebooks, update the pip install line:

```python
# Old
%pip install dlt-meta==0.0.10

# New
%pip install databricks-labs-sdp-meta==0.0.11
```

The import in the notebook remains the same:
```python
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

## Backward Compatibility

The old `dlt-meta` package is maintained as a **compatibility wrapper** that:

- Re-exports all symbols from `databricks.labs.sdp_meta`
- Emits `DeprecationWarning` on import
- Forwards all CLI commands to `sdp-meta`

This means existing code using `dlt-meta` will continue to work, but you will see deprecation warnings encouraging migration.

### Class Aliases

The following aliases are maintained for backward compatibility:

| Old Name | New Name | Location |
|----------|----------|----------|
| `DLTMeta` | `SDPMeta` | `databricks.labs.sdp_meta.cli` |
| `DLT_META_RUNNER_NOTEBOOK` | `SDP_META_RUNNER_NOTEBOOK` | `databricks.labs.sdp_meta.cli` |

## Deprecation Timeline

| Phase | Status | Description |
|-------|--------|-------------|
| **Current** | Active | Both `dlt-meta` and `sdp-meta` packages work. Old package shows deprecation warnings. |
| **Future** | Planned | `dlt-meta` compatibility wrapper will be removed. Announcement will be made in advance. |

## Need Help?

- [Getting Started Guide]({{< ref "getting_started" >}})
- [FAQ]({{< ref "faq" >}})
- [File an Issue](https://github.com/databrickslabs/dlt-meta/issues)
