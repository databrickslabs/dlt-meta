---
title: "SDP-META Renaming"
date: 2026-02-10
weight: 50
---

# DLT-META to SDP-META Renaming

Starting with v0.0.11, the project has been renamed from **DLT-META** to **SDP-META** (Spark Declarative Pipelines META). This document describes all naming changes and provides a user guide for both new and existing users.

---

## Package Restructuring

| Component | Before (v0.0.10) | After (v0.0.11) |
| --- | --- | --- |
| Display Name | DLT-META | SDP-META |
| PyPI Package | `dlt-meta` | `databricks-labs-sdp-meta` |
| Python Import | `from src.dataflow_pipeline import ...` | `from databricks.labs.sdp_meta.dataflow_pipeline import ...` |
| CLI Command | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Source Location | `src/` | `src/databricks/labs/sdp_meta/` |
| PythonWheelTask package_name | `dlt_meta` | `databricks_labs_sdp_meta` |

---

## Naming Convention Updates

| Context | Before | After |
| --- | --- | --- |
| CLI Name (`labs.yml`) | `dlt-meta` | `sdp-meta` |
| Python Module | `dlt_meta` | `databricks.labs.sdp_meta` |
| Classes | `DLTMeta` | `SDPMeta` |
| Constants | `DLT_META_RUNNER_NOTEBOOK` | `SDP_META_RUNNER_NOTEBOOK` |
| Schemas | `dlt_meta_dataflowspecs` | `sdp_meta_dataflowspecs` |
| Config Keys | `dlt_meta_schema` | `sdp_meta_schema` |

---

## User Guide: New Users

### Install

```bash
databricks labs install sdp-meta
```

### Onboard

```bash
databricks labs sdp-meta onboard
```

The command will prompt you to provide onboarding details interactively.

### Deploy

```bash
databricks labs sdp-meta deploy
```

The command will prompt you to provide pipeline configuration details.

### Python Imports

```python
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
from databricks.labs.sdp_meta.cli import SDPMeta
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders
from databricks.labs.sdp_meta.pipeline_writers import AppendFlowWriter, DLTSinkWriter
```

---

## User Guide: Existing Users

Existing users of `dlt-meta` can continue using their current setup. A backwards-compatible wrapper package ensures everything keeps working while you plan your migration.

### What Stays the Same

- Existing pipelines continue to run without changes
- Existing configuration files (JSON/YAML) work without modification
- Legacy config keys like `dlt_meta_schema` are still supported (with a logged warning)

### What Changes

Old CLI commands and imports emit deprecation warnings guiding you to the new names.

### CLI (still works)

```bash
# Old command — shows deprecation banner, then runs normally
databricks labs dlt-meta onboard

# Output:
# ============================================================
# DEPRECATION NOTICE: 'dlt-meta' CLI is deprecated.
# Please use 'databricks labs sdp-meta' instead.
# ============================================================
```

### Python Imports (still work)

```python
# Old import — emits DeprecationWarning, then works normally
from dlt_meta.cli import DLTMeta

# DLTMeta is aliased to SDPMeta under the hood
```

### Config Key Compatibility

The framework reads both key formats automatically:

| Config Key | Status |
| --- | --- |
| `sdp_meta_schema` | New (recommended) |
| `dlt_meta_schema` | Legacy (still supported, logs warning) |

### Installation During Transition

```bash
# Still works — pulls databricks-labs-sdp-meta as a dependency
pip install dlt-meta
```

---

## Migration Steps

When you are ready to migrate, follow these steps:

### 1. Update Installation

```bash
# Remove old package
pip uninstall dlt-meta

# Install new package
databricks labs install sdp-meta
```

### 2. Update CLI Commands

```bash
# Before
databricks labs dlt-meta onboard
databricks labs dlt-meta deploy

# After
databricks labs sdp-meta onboard
databricks labs sdp-meta deploy
```

### 3. Update Python Imports

```python
# Before
from dlt_meta.cli import DLTMeta

# After
from databricks.labs.sdp_meta.cli import SDPMeta
```

### 4. Update Config Files (Optional)

Update keys in your `onboarding_job_details.json` when convenient:

```json
// Before
{ "dlt_meta_schema": "my_schema" }

// After
{ "sdp_meta_schema": "my_schema" }
```

This step is optional — legacy keys continue to work.

---

## Backwards Compatibility Details

A `compat/` package provides the bridge between old and new:

| Component | How It Works |
| --- | --- |
| `dlt-meta` PyPI package | v0.0.11 depends on `databricks-labs-sdp-meta>=0.0.11` |
| `from dlt_meta import ...` | Re-exports all symbols from `databricks.labs.sdp_meta` with deprecation warnings |
| `DLTMeta` class | Aliased to `SDPMeta` |
| `DLT_META_RUNNER_NOTEBOOK` | Aliased to `SDP_META_RUNNER_NOTEBOOK` |
| `databricks labs dlt-meta` CLI | Forwards to `databricks labs sdp-meta` with deprecation banner |
| `dlt_meta_schema` config key | Read with a logged warning suggesting `sdp_meta_schema` |

---

## Deprecation Timeline

| Phase | Status | Details |
| --- | --- | --- |
| **v0.0.11** | Active | Both `dlt-meta` and `sdp-meta` packages work. Old package shows deprecation warnings. |
| **Next Release** | Planned | `dlt-meta` compat package maintained but no new features added. |
| **Future** | Planned | `dlt-meta` package removed. Advance notice will be provided. |
