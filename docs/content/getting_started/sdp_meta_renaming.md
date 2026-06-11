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

### Legacy `src.*` Imports (still work in v0.0.11, removed in v0.1.0)

v0.0.10 published the framework as a top-level Python package literally named `src`. Customer notebooks following the v0.0.10 demo guide contain lines like:

```python
from src.dataflow_pipeline import DataflowPipeline
from src.cli import DLTMeta
from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
```

After upgrading to `dlt-meta==0.0.11`, **these continue to work unchanged** — each `src.*` module is registered in `sys.modules` as an alias for the corresponding `databricks.labs.sdp_meta.*` module at interpreter startup (via a `.pth` file shipped in the wheel). On first attribute access, each alias emits a one-time `DeprecationWarning` per process pointing at the canonical replacement.

```text
DeprecationWarning: 'src.dataflow_pipeline' is a v0.0.10 compatibility
alias and will be removed in v0.1.0. Migrate to 'from
databricks.labs.sdp_meta.dataflow_pipeline import …' or 'from
dlt_meta import …'.
```

#### Disabling the `src.*` shim

If you have your own `src/` package on `sys.path` (rare but real for monorepo layouts), set `SDP_META_DISABLE_SRC_ALIAS=1` in the cluster environment **before** any `dlt_meta` import. This skips both the alias registration and the package-level deprecation warning.

```bash
export SDP_META_DISABLE_SRC_ALIAS=1
```

#### Removal in v0.1.0

The `src.*` aliases will be removed in v0.1.0. Migrate before then by rewriting:

```python
# v0.0.10
from src.dataflow_pipeline import DataflowPipeline
from src.cli import DLTMeta

# v0.0.11+ (canonical)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from databricks.labs.sdp_meta.cli import SDPMeta
```

#### What's NOT aliased: `integration_tests.*`

v0.0.10's `setup.py` also published `integration_tests` as a top-level package, but this was an oversight — `integration_tests/` is not a stable API surface. It is **not** aliased in v0.0.11; if you have notebook code that does `from integration_tests.run_integration_tests import …`, inline the relevant code instead.

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
| `from src.* import ...` | `src.*` modules are registered in `sys.modules` as aliases for `databricks.labs.sdp_meta.*` at interpreter startup via a `.pth` file. Removed in v0.1.0. |
| `DLTMeta` class | Aliased to `SDPMeta` (rebound in `cli.py` so `from src.cli import DLTMeta` resolves through the module alias) |
| `DLT_META_RUNNER_NOTEBOOK` | Aliased to `SDP_META_RUNNER_NOTEBOOK` |
| `databricks labs dlt-meta` CLI | Forwards to `databricks labs sdp-meta` with deprecation banner |
| `dlt_meta_schema` config key | Read with a logged warning suggesting `sdp_meta_schema` |
| `SDP_META_DISABLE_SRC_ALIAS=1` env var | Disables the `src.*` shim and suppresses the package-level deprecation warning |

---

## Deprecation Timeline

| Phase | Status | Details |
| --- | --- | --- |
| **v0.0.11** | Active | Both `dlt-meta` and `sdp-meta` packages work. Old package shows deprecation warnings. `src.*` imports work via shim. |
| **v0.0.12 → v0.0.x** | Planned | `dlt-meta` compat package maintained but no new features added. `src.*` shim still active. |
| **v0.1.0** | Planned | `src.*` shim removed. `from src.X import …` returns to raising `ModuleNotFoundError`. |
| **Future** | Planned | `dlt-meta` package removed. Advance notice will be provided. |
