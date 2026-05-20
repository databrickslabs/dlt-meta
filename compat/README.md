# DLT-META Compatibility Package

> **⚠️ DEPRECATED**: This package is a backwards-compatibility wrapper. Please migrate to `databricks-labs-sdp-meta`.

## Overview

This package (`dlt-meta`) is maintained for backwards compatibility with existing installations.
All functionality has been moved to the `databricks-labs-sdp-meta` package.

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

The old imports will continue to work with deprecation warnings:

```python
# Old (deprecated, but still works)
from dlt_meta.cli import DLTMeta

# New (recommended)
from databricks.labs.sdp_meta.cli import SDPMeta
from databricks.labs.sdp_meta import DataflowPipeline
```

### Configuration Files

Your existing configuration files (JSON/YAML) will continue to work without changes.
Field names like `dlt_meta_schema`, `dlt_meta_bronze_schema`, etc. are still supported.

## Deprecation Timeline

- **Current**: Both packages work, `dlt-meta` shows deprecation warnings
- **Future**: `dlt-meta` package will be removed (announcement will be made)

## Support

For issues, please file them at: https://github.com/databrickslabs/sdp-meta/issues
