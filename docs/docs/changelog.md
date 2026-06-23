---
id: changelog
title: Changelog
sidebar_position: 99
---

# Changelog

---

## v0.1.0

### New Features

- **Automatic liquid clustering** (`cluster_by_auto`) for bronze and silver tables. When set to `true`, Databricks automatically determines the optimal clustering columns. Works alongside explicit `cluster_by` to define initial keys followed by automatic optimization. Supported for `bronze_cluster_by_auto`, `bronze_quarantine_table_cluster_by_auto`, and `silver_cluster_by_auto`. ([Issue #238](https://github.com/databrickslabs/dlt-meta/issues/238))
- **MCP Server support** — AI-assisted pipeline scaffolding via the `mcp` CLI command (stdio transport). Enables use with Claude Code and other MCP-compatible clients.
- **DAB template** with `bundle-init --quickstart` zero-prompt fast path for instant bundle scaffolding. Includes `bundle-add-flow`, `bundle-prepare-wheel`, and `bundle-validate` commands.
- **Row filter support** — `where_clause` in silver transformations files for pipeline-time row filtering.

### Breaking Changes

- **PyPI package renamed**: `dlt-meta` → `databricks-labs-sdp-meta`
- **CLI commands renamed**: `databricks labs dlt-meta` → `databricks labs sdp-meta`
- **Python imports changed**: `from dlt_meta import ...` → `from databricks.labs.sdp_meta import ...`
- **Main class renamed**: `DLTMeta` → `SDPMeta`
- **Source layout changed**: flat `src/` → `src/databricks/labs/sdp_meta/` namespace package

### Backward Compatibility

The `dlt-meta` compatibility wrapper package re-exports all symbols and forwards CLI commands. Existing code continues to work with deprecation warnings. `src.*` imports are supported via a `sys.modules` shim (removed in v0.2.0).

See the [Migration guide](./operations/migration) for full details.

---

## v0.0.10

### Breaking Changes

- **DPM mode removed**: Pipelines using the Legacy Publishing Mode (DPM) flag must be migrated to the default publishing mode before upgrading. Follow [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode). This migration is irreversible.
- **Multi-level namespace qualifiers**: Custom schema qualification in table names is no longer supported. Tables must be created without database qualifiers in the pipeline context.
- **`invoke_dlt_pipeline` argument names**: Method arguments now require layer-specific prefixes (`bronze_` or `silver_`). Replace `custom_transform_func` with `bronze_custom_transform_func`, and `next_snapshot_and_version` with `bronze_next_snapshot_and_version`.

### Enhancements

- `apply_changes_from_snapshot` support in the silver layer
- Databricks App UI for onboarding and deploy commands
- Non-Delta sink support (Delta and Kafka sinks) via `bronze_sink` / `silver_sink`
- Quarantine support in the silver layer for data quality rules
- Table comments, column comments, and `cluster_by` support
- Catalog support for `sourceDetails` and `targetDetails`
- DBDemos integration
- YAML support for onboarding files
- Multiple column support for `create_auto_cdc_flow`
- Custom transformations support for Kafka and Delta sources

### Migration

See the [Migration guide](./operations/migration#migrating-from-v0010-specific-breaking-changes).

---

## v0.0.9

### Enhancements

- `apply_changes_from_snapshot` API support in the bronze layer
- `append_flow` API support for the silver layer
- File metadata column support for Autoloader
- Bring-your-own custom transformation support
- UC Volume and serverless support for CLI, integration tests, and demos
- Bronze/silver pipeline chaining into a single Declarative Pipeline
- Liquid clustering support
- Silver fanout demo

---

## v0.0.8

### Enhancements

- `append_flow` API support for bronze and silver layers
- File metadata columns for Autoloader
- Custom transformation support
- Silver fanout demo and unit tests

---

## v0.0.7

### Enhancements

- Fixed mismatched key: `read_dlt_delta()` updated to use `source_database` instead of `database`

---

## v0.0.6

### Enhancements

- Migrated to `create_streaming_table` API from deprecated `create_streaming_live_table`
- Data quality support for the silver layer
- Unity Catalog integration test framework

---

## v0.0.5

### New Features

- Unity Catalog support
- Databricks Labs CLI support with `onboard` and `deploy` commands

---

## v0.0.4

### Bug Fixes

- New `eventhub.accessKeySecretName` option for Event Hubs source

---

## v0.0.3

### Bug Fixes

- Infer datatypes from `sequence_by` for `__START_AT` / `__END_AT` in the `apply_changes` API

---

## v0.0.2

### New Features

- Table properties support for bronze, quarantine, and silver tables
- `track_history_column` support in `apply_changes`
- Delta source support
- Bronze/silver onboarding validation

---

## v0.0.1

Initial release.
