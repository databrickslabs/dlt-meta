---
id: faq
title: FAQ
sidebar_position: 10
---

# Frequently Asked Questions

## General

**Q: What reader types are supported?**

Databricks Autoloader (`cloudFiles`), Delta, Kafka, Event Hubs, and snapshot. Any Spark Structured Streaming reader can also be added by overriding `read_bronze()` in `DataflowPipeline`.

**Q: How many pipelines will SDP-META launch?**

One pipeline per `data_flow_group` value. Rows sharing the same `data_flow_group` run inside the same pipeline.

**Q: Can I run onboarding for bronze only?**

Yes. Set `"onboard_layer": "bronze"`. Both `bronze_dataflowspec_table` and `silver_dataflowspec_table` are required in the config, but only the bronze table gets populated.

**Q: Can I run onboarding for silver only?**

Yes. Set `"onboard_layer": "silver"`. Both table names are still required in the config. The bronze dataflowspec table must already exist and contain rows.

**Q: Do I have to use JSON? Can I use YAML?**

Both are supported. Pass a `.yml` file to `onboard` or reference it in your DAB bundle. All fields are identical between formats.

**Q: Can I write to the same target table from multiple sources?**

Yes — two patterns are supported:

- **Append + merge** — use `bronze_append_flows` to land additional raw sources into the same bronze streaming table, then `bronze_cdc_apply_changes` (or `silver_cdc_apply_changes`) to run a single CDC merge over the combined bronze. Best when sources are heterogeneous and you want them durably landed in bronze before the merge step. This is the pattern shown in the [Multi-Source CDC guide](./guides/multi-source-cdc).
- **Native multi-source CDC** — use `bronze_cdc_apply_changes_flows` (a single CDC group with a `flows: [...]` list and per-flow `source_details` / `source_schema_path`) when sources share a CDC schema and should merge directly through one `create_auto_cdc_flow_from_*` call. The same shape exists at silver as `silver_cdc_apply_changes_flows` for fan-in into a silver table.

The two patterns are mutually exclusive on the same flow row (the framework rejects an onboarding row that sets both `bronze_cdc_apply_changes` and `bronze_cdc_apply_changes_flows`).

**Q: How do I chain multiple silver tables from one bronze table?**

Use [Silver Fanout](./guides/silver-fanout). Run a second onboarding job in append mode (`"overwrite": "false"`).

**Q: How do I add Autoloader file metadata columns (filename, size, etc.) to the bronze table?**

Configure `source_metadata` inside `source_details`. See [Autoloader — File metadata columns](./guides/autoloader#file-metadata-columns).

---

## Migrating from DLT-META

**Q: My pipeline runs in Legacy Publishing Mode (DPM). Can I upgrade to v0.1.0?**

No — not yet. You must migrate to the default publishing mode first, then upgrade. Legacy DPM pipelines with custom schemas will fail at runtime in v0.1.0 with:
```
DLTAnalysisException: Materializing tables in custom schemas is not supported.
```
Follow the Databricks guide to [migrate to the default publishing mode](https://docs.databricks.com/aws/en/ldp/migrate-to-dpm#migrate-to-the-default-publishing-mode) before upgrading. Test in a non-production environment — the migration is one-way.

**Q: My notebooks use `from src.dataflow_pipeline import DataflowPipeline`. Will they break?**

No. A `sys.modules` shim maps all `src.*` imports to `databricks.labs.sdp_meta.*` automatically. Your notebooks keep working without changes. This shim will be removed in v0.2.0 — update imports before then.

**Q: My code uses `import dlt`. Does v0.1.0 still support this?**

The underlying Databricks API is now `pyspark.pipelines` (imported as `dp`). SDP-META handles this internally — you do not write `import dlt` or `import pyspark.pipelines` in your own code. `DataflowPipeline.invoke_dlt_pipeline()` is unchanged from the user's perspective.

**Q: I used `custom_transform_func` in `invoke_dlt_pipeline`. Do I need to change it?**

Yes. The single `custom_transform_func` argument was split into layer-specific arguments in v0.0.10:

```python
# Before (v0.0.9 and earlier — no longer supported)
DataflowPipeline.invoke_dlt_pipeline(spark, layer, custom_transform_func=my_func)

# After (v0.0.10+)
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    bronze_custom_transform_func=my_bronze_func,
    silver_custom_transform_func=my_silver_func,
)
```

**Q: My onboarding file uses `apply_changes`. Do I need to rename it to `create_auto_cdc_flow`?**

No. Field names in the onboarding file (`bronze_cdc_apply_changes`, `silver_cdc_apply_changes`, etc.) are unchanged. The rename from `apply_changes` to `create_auto_cdc_flow` happened in the underlying Databricks API — SDP-META translates automatically.

**Q: I used DBFS paths in my onboarding file (`dbfs:/...`). Do I need to migrate to UC Volumes?**

DBFS paths still work, but the recommended approach for Unity Catalog workspaces is UC Volumes (`/Volumes/catalog/schema/volume/...`). The CLI and DAB workflows default to UC Volumes for all file uploads. Migrate at your own pace — there is no forced cutover.

**Q: What happened to the `dlt_meta_schema` config key?**

It was renamed to `sdp_meta_schema` in v0.1.0. The old key is still accepted with a logged warning. Update it before v0.2.0.

**Q: My pipeline uses `database` as a single schema name (not `catalog.schema`). Does UC mode still work?**

Yes. The `database` field accepts `schema` (one part) or `catalog.schema` (two parts). Three-part names (`catalog.schema.table`) are not valid here — table names go in `bronze_dataflowspec_table` and `silver_dataflowspec_table`.

---

## New Features in v0.1.0

**Q: How do I enable liquid clustering on my tables?**

Add `bronze_cluster_by` (or `silver_cluster_by`) to your onboarding row with a comma-separated list of columns:
```json
{ "bronze_cluster_by": "event_date,customer_id" }
```
To let Databricks choose the clustering columns automatically, set the layer-specific `*_cluster_by_auto` key to `true`:
```json
{ "bronze_cluster_by_auto": true }
```
Both layers have their own field — use `bronze_cluster_by_auto` for the bronze table and `silver_cluster_by_auto` for the silver table. The bare `cluster_by_auto` key is **not** recognized and will be silently ignored. The quarantine-table variant `bronze_quarantine_table_cluster_by_auto` is also supported. See the [Onboarding fields reference](reference/onboarding-fields) for the full list.

**Q: What is `bronze_row_filter` / `silver_row_filter`?**

Row-level security filters applied when reading from the streaming table. Useful for masking sensitive data per-pipeline. Set in the onboarding file:
```json
{ "bronze_row_filter": "region = 'US'" }
```
Quarantine table variants (`bronze_quarantine_row_filter`) are also supported.

**Q: Can I deploy bronze and silver in a single pipeline run?**

Yes. Set `"layer": "bronze_silver"` in the pipeline configuration. This chains bronze into silver in one Lakeflow Spark Declarative Pipeline execution. Alternatively, use `pipeline_mode=combined` in a DAB bundle.

**Q: What is `create_sink` and what formats does it support?**

`create_sink` writes pipeline output to an external destination alongside the managed Delta table. Supported sinks: external Delta table and Kafka. Configure in the onboarding file via `bronze_sinks` or `silver_sinks`.

**Q: Does the silver layer now support quarantine tables?**

Yes. Silver quarantine tables were added in v0.1.0. Set `silver_quarantine_table` and related fields in the onboarding file — rows failing silver expectations are written there instead of being dropped.

**Q: Can I use YAML for onboarding instead of JSON?**

Yes. YAML is fully supported for the onboarding file, DQE rules, and silver transformation files. All field names are identical. Pass a `.yml` file path to `onboard` or reference it in your DAB bundle.

---

## Declarative Automation Bundles (DAB)

**Q: What is the recommended way to deploy SDP-META?**

Use the DAB interface (`bundle-init`, `bundle-add-flow`, `bundle-validate`, `bundle deploy`). It gives you git-tracked pipeline state, `dev`/`prod` targets, and CI/CD support. The interactive `onboard`/`deploy` CLI is for first-touch exploration only.

**Q: What does `bundle-init --quickstart` do?**

Scaffolds a bundle with developer-friendly defaults (Autoloader + bronze_silver + split pipeline mode + PyPI dependency) so you can get a working bundle in one command. Edit `resources/variables.yml` afterwards to point at your real catalog/schema.

**Q: What is `pipeline_mode` and when should I use each?**

- `split` (default) — bronze and silver deploy as two separate Lakeflow Spark Declarative Pipelines. Recommended for most cases: independent scheduling and failure isolation.
- `combined` — bronze and silver run in a single pipeline. Use when you need atomic bronze-to-silver promotion or want a single pipeline UI view.

**Q: What is `bundle-validate` checking beyond `databricks bundle validate`?**

It catches SDP-META-specific authoring mistakes: unedited `<your-...>` or `__SET_ME__` placeholders in `databricks.yml` and onboarding files, mismatched `dataflow_group` references, `pipeline_mode` inconsistencies, and `wheel_source` vs `sdp_meta_dependency` drift.

**Q: How do I generate onboarding entries in bulk?**

Use the recipes inside the scaffolded bundle (`recipes/`):
- `from_uc.py` — from existing Unity Catalog tables
- `from_volume.py` — from CSVs in a UC volume
- `from_topics.py` — from Kafka / Event Hub topic lists
- `from_inventory.py` — from an inventory CSV

---

## Installation & PyPI

**Q: What is the difference between `databricks-labs-sdp-meta` and `dlt-meta` on PyPI?**

`databricks-labs-sdp-meta` is the primary package with all code. `dlt-meta` is an empty compatibility wrapper that declares `databricks-labs-sdp-meta` as a dependency — so `pip install dlt-meta` keeps working for existing users without any notebook changes.

**Q: I was using `pip install dlt-meta`. Do I need to change anything?**

No. `pip install dlt-meta==0.1.0` installs `databricks-labs-sdp-meta` as a dependency automatically. Your existing `from dlt_meta import ...` imports continue to work with a deprecation warning.

**Q: When will the `dlt-meta` compatibility package be removed?**

The compat package will be maintained through v0.1.x with no new features. `src.*` imports (v0.0.10 style) will be removed in v0.2.0. The `dlt-meta` package itself will be removed in a future major version. See the full [deprecation timeline](./operations/migration#deprecation-timeline).

---

## MCP Agent

**Q: What is the SDP-META MCP Agent?**

An MCP (Model Context Protocol) server that exposes SDP-META operations to AI assistants like Claude. It lets you onboard, deploy, and inspect pipelines through natural language.

**Q: How do I install the MCP Agent?**

```bash
pip install databricks-labs-sdp-meta[mcp]
```

See the [MCP Getting Started guide](./getting-started/mcp) for configuration details.

---

## App

**Q: Do I need to run an initial setup before using the SDP-META App?**

Yes. Click the **Setup** button when the app first loads.

**Q: Who can access the SDP-META App?**

Authenticated Databricks workspace users with `CAN_USE` permission on the app. `CAN_MANAGE` is required for administration.

**Q: How does catalog and schema access work in the App?**

The app uses a dedicated Service Principal with `USE CATALOG`, `USE SCHEMA`, and `SELECT` permissions on all Unity Catalog resources used by SDP-META.
