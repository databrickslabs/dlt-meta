# SDP-META Compatibility Package

> **⚠️ DEPRECATED**: This package is a backwards-compatibility wrapper. Please migrate to `databricks-labs-sdp-meta`.

## Overview

This package (`dlt-meta`) is maintained for backwards compatibility with existing v0.0.10 installations. All functionality has been moved to the `databricks-labs-sdp-meta` package.

It supports three independent customer surfaces from v0.0.10:

| Surface | What it covers |
|---|---|
| `from dlt_meta import …` | Flat re-exports of every v0.0.10 public symbol |
| `from dlt_meta.cli import DLTMeta` | Renamed-class compatibility (DLTMeta → SDPMeta) |
| `from src.<module> import …` | Legacy `src.*` import paths used by every v0.0.10 demo notebook |

All three emit a one-time `DeprecationWarning` on first use and are scheduled for removal in v0.2.0.

## Migration Guide

### Installation

Replace:
```bash
pip install dlt-meta
```

With:
```bash
pip install databricks-labs-sdp-meta
```

### CLI Commands

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

### Python Imports

The old imports continue to work with deprecation warnings:

```python
# Old (deprecated, but still works)
from dlt_meta.cli import DLTMeta
from dlt_meta import DataflowPipeline

# Legacy v0.0.10 src.* imports (still work via .pth shim, removed in v0.2.0)
from src.dataflow_pipeline import DataflowPipeline
from src.cli import DLTMeta

# New (recommended)
from databricks.labs.sdp_meta.cli import SDPMeta
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
```

### Configuration Files

Your existing configuration files (JSON/YAML) will continue to work without changes. Field names like `dlt_meta_schema`, `dlt_meta_bronze_schema`, etc. are still supported.

## How the `src.*` shim works

v0.0.10 published the framework as a top-level Python package literally named `src` (via `find_packages(include=["src", ...])` in `setup.py`). v0.1.0 dropped that layout in favour of `databricks.labs.sdp_meta.*`. To keep v0.0.10 customer notebooks running unchanged, this compat package:

1. Ships a `dlt_meta.pth` file at the wheel's `site-packages` root. CPython's `site.py` runs every line beginning with `import` in `*.pth` files at interpreter startup, so the line `import os; os.environ.get('SDP_META_DISABLE_SRC_ALIAS') == '1' or __import__('dlt_meta')` triggers `dlt_meta`'s package init **before** any user code, including before notebook cell 1.
2. `dlt_meta.__init__` registers `src` in `sys.modules` as an alias for itself, and each `src.<sub>` (e.g. `src.dataflow_pipeline`) as a `_LazyAliasModule` proxy for the corresponding `databricks.labs.sdp_meta.<sub>`.
3. The proxy emits a `DeprecationWarning` once per process on first attribute access — `stacklevel` is tuned so the warning surfaces at the customer's notebook line, not somewhere inside this package.

If `pyspark.pipelines` (the Lakeflow SDP runtime) isn't available — e.g. on a legacy DBR runtime — the registration falls back to a stub module whose `__getattr__` raises a clear `Lakeflow SDP runtime` error. This replaces the silent-swallow `ImportError: cannot import name 'DataflowPipeline'` that the old shim produced and points the customer at the actual cause (wrong DBR).

## Disabling the shim

Set `SDP_META_DISABLE_SRC_ALIAS=1` before any `dlt_meta` import to skip the `src.*` aliasing **and** suppress the package-level deprecation warning:

```bash
export SDP_META_DISABLE_SRC_ALIAS=1
```

Use this when you have your own `src/` package on `sys.path` that would conflict with the alias.

## Editable-install caveat for maintainers

`pip install -e ./compat` does **not** install the `.pth` file — `data_files` in setuptools is honoured for wheel installs only. Maintainers running editable installs need to either:

- explicitly `import dlt_meta` in their notebook before any `from src.* import …` line, or
- build and install the wheel: `python -m pip wheel ./compat -w dist/ && pip install dist/dlt_meta-*.whl`.

CI verifies the wheel path; the editable-install path is a dev-loop convenience only.

## Removal Timeline

| Version | Behaviour |
|---|---|
| v0.1.0 | All compat surfaces work; emit `DeprecationWarning`s. |
| v0.1.x | Same. No behaviour change. |
| v0.2.0 | `src.*` shim removed. `compat/dlt_meta.pth` removed. `from src.* import …` returns to raising `ModuleNotFoundError`. |
| Future | `dlt-meta` package removed entirely. Advance notice will be provided. |

## Support

For issues, please file them at: https://github.com/databrickslabs/sdp-meta/issues
