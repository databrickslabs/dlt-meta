# OSS Apache Spark Declarative Pipelines runtime

This template makes a metadata-driven SDP-META pipeline runnable through
the OSS [`spark-pipelines`](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
CLI on Apache Spark 4.1+, alongside the existing Databricks Lakeflow path.

## What works on OSS Spark

- Vanilla Spark file-source ingestion — `json`, `csv`, `parquet`,
  `delta`, plus generic Spark `kafka` (with the matching `--packages`
  jar). All reuse the same readers under
  [`pipeline_readers.py`](../../pipeline_readers.py). The OSS smoke
  fixture ([`tests/resources/oss_onboarding.json`](../../../../../../tests/resources/oss_onboarding.json))
  uses `source_format: "json"` for this reason.
- Bronze + silver layer materialisation as `@dp.table`, including
  partition columns and cluster-by columns.
- `expect_or_drop` data quality expectations — enforced at runtime via
  filter injection by the shim in
  [`oss_pipelines.py`](../../oss_pipelines.py).
- `expect_or_fail` — best-effort, raises at execution time on the first
  violating row via Spark's `raise_error` SQL function. **Semantics
  differ from Lakeflow**: Lakeflow aborts the update before any write;
  the OSS shim raises during streaming execution after some rows may
  already have been emitted to downstream operators. Don't migrate
  Lakeflow specs that depend on the strict pre-write semantics without
  re-validating.
- `append_flow` — works identically to Lakeflow.
- `create_sink` — works identically to Lakeflow for any sink format
  Spark itself supports (Delta, Kafka, custom Python data sources).

## What does NOT work on OSS Spark

- `cloudFiles` (Auto Loader) — **Databricks-proprietary source.** Plain
  Apache Spark 4.1+ has no `cloudFiles` data source; a `spark-pipelines
  run` against an onboarding spec with `source_format: "cloudFiles"`
  fails at source resolution with `DataSource not found`. Use
  `source_format: "json"` / `"csv"` / `"parquet"` / `"delta"` against
  cloud storage paths (the standard Spark file readers stream
  micro-batches incrementally just like Auto Loader, minus the file
  notification / RocksDB optimisations). For workloads that genuinely
  need Auto Loader semantics (e.g. trillions-of-files scale, schema
  inference + evolution, file-notification mode), stay on Lakeflow.
- Event Hubs / Kafka helpers that depend on the Databricks `eventhub`
  connector. Plain Spark `kafka` works with the appropriate `--packages
  org.apache.spark:spark-sql-kafka-0-10_2.13:...` jar.
- `cdcApplyChanges` (`create_auto_cdc_flow`) — Lakeflow extension; relies
  on streaming MERGE / SCD semantics not present in OSS
  `pyspark.pipelines`. The shim raises a `NotImplementedError` with
  a pointer to remove the section from the dataflow spec.
- `applyChangesFromSnapshot` (`create_auto_cdc_from_snapshot_flow`) —
  same reason.
- `expect_all` metrics — OSS SDP has no event-log surface for
  per-expectation metrics; the shim logs registration only.
- `cluster_by_auto` — Lakeflow-only kwarg, the shim drops it on OSS.
- The `databricks labs sdp-meta deploy` / `bundle-deploy` commands —
  those wire a Databricks pipeline via the Databricks SDK and have no
  OSS analogue.

## Per-table paths on OSS

OSS `pyspark.pipelines.table` itself rejects a `path` kwarg, but
SDP-META still honours the per-table paths from the onboarding spec
(`bronze_table_path_<env>` / `silver_table_path_<env>`) on OSS. Before
the OSS planner registers each `dp.table(name="<db>.<table>", ...)`,
`OSSDataflowPipeline` runs:

```sql
CREATE SCHEMA IF NOT EXISTS `<db>`;
CREATE TABLE IF NOT EXISTS `<db>`.`<table>`
USING DELTA LOCATION '<onboarding-spec-path>';
```

so the subsequent SDP materialisation writes into the configured Delta
location. Idempotent on re-runs; warns when an existing registration
points at a different location. Same applies to
`dp.create_streaming_table` (only reachable through paths that already
raise `NotImplementedError` on OSS today, but covered for symmetry).

> **Caveat — silver path binding on first run.** The side channel needs
> a `StructType` to issue a `CREATE TABLE ... USING DELTA LOCATION`
> that succeeds at an empty path. Bronze specs always carry a schema
> (`source_schema_path`), so this works. **Silver** specs derive their
> shape from the bronze output + `silver_transformation_json` and
> don't carry a schema up front, so the first run at an empty
> `silver_table_path_<env>` falls back to a warn-and-continue path —
> the silver table gets created at the pipeline's default location, not
> the configured one. Pre-create the silver Delta table with the
> expected schema, or accept the first-run drift (subsequent runs find
> the Delta log and bind correctly). See
> [`docs/getting_started/oss_spark.md`](../../../../../../docs/content/getting_started/oss_spark.md#caveat--silver-table-path-binding-on-first-run)
> for the long form.

The bronze + silver dataflowspec metadata itself is also Delta-path
based on OSS — `OnboardDataflowspec` writes via
`DataFrameWriter.save(<bronze_dataflowspec_path>)` and the runtime
reads via `bronze.dataflowspecPath` / `silver.dataflowspecPath`. No
catalog / metastore involved on either end of the OSS pipeline.

## Layout

```
spark-pipeline.yml      pipeline spec consumed by `spark-pipelines run`
run_pipeline.py         top-level source file imported into the SDP graph;
                        invokes DataflowPipeline.invoke_pipeline at planning time
onboarding/             onboarding files (JSON or YAML) — same shape as
                        the existing Lakeflow path
```

## Quickstart

1. Install Apache Spark 4.1+ with the SDP extras:

   ```bash
   pip install "pyspark[pipelines]>=4.1.0" databricks-labs-sdp-meta
   ```

2. Copy this template into your project, **stripping the `.tmpl`
   suffix** from each file (the OSS templates are not rendered through a
   template engine — unlike `templates/dab/` — so copy them verbatim and
   rename):

   ```bash
   cp spark-pipeline.yml.tmpl spark-pipeline.yml
   cp run_pipeline.py.tmpl    run_pipeline.py
   ```

   Then edit `spark-pipeline.yml`: replace the `__PIPELINE_NAME__`,
   `__SET_ME__` placeholders (and set `storage`, optionally `catalog` /
   `database`), and seed your onboarding file. `run_pipeline.py` needs no
   edits — `spark-pipeline.yml`'s `libraries.glob` points at it by its
   stripped name.

3. Run:

   ```bash
   spark-pipelines run --spec spark-pipeline.yml
   ```

   Add any `spark-submit` flags for cluster manager / packages as
   needed (`--master`, `--packages io.delta:delta-spark_2.13:...`).

## Forcing a runtime

Detection probes `pyspark.pipelines` for Lakeflow extension symbols.
Override with the `SDP_META_RUNTIME` environment variable:

- `SDP_META_RUNTIME=oss` — force OSS path (useful for local testing on a
  Databricks-installed Python).
- `SDP_META_RUNTIME=databricks` — force Lakeflow path.

## Smoke-testing the OSS code path locally

Before wiring up `spark-pipelines run`, you can verify the OSS code
path end-to-end without a Spark 4.1+ install. From the repo root:

```bash
python scripts/run_oss_demo.py
```

The script onboards a paths-only spec
([`tests/resources/oss_onboarding.json`](../../../../../../tests/resources/oss_onboarding.json)),
writes the bronze and silver dataflowspec data to two Delta paths
under a temp dir, points Spark conf at those paths via
`bronze.dataflowspecPath` / `silver.dataflowspecPath`, and asserts the
recorded `pyspark.pipelines` calls match the OSS contract (no
`expect_*`, no `create_auto_cdc_*`, no Lakeflow-only kwargs, CDC
methods raise). See
[`docs/getting_started/oss_spark`](../../../../../../docs/content/getting_started/oss_spark.md#testing-the-oss-code-path-locally)
for details.

## Pinning the OSS class explicitly

`DataflowPipeline.invoke_pipeline(spark, layer)` auto-dispatches to
[`OSSDataflowPipeline`](../../oss_dataflow_pipeline.py) when the runtime
probe lands on OSS, so the default `run_pipeline.py` template works in
both runtimes. To pin the OSS subclass directly (for example, to fail
fast on a misconfigured Lakeflow runtime instead of silently using
Lakeflow code paths), import it explicitly:

```python
from databricks.labs.sdp_meta import OSSDataflowPipeline
from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils

bronze_specs = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
for spec in bronze_specs:
    OSSDataflowPipeline(spark, spec, view_name=f"{spec.targetDetails['table']}_inputview").run()
```
