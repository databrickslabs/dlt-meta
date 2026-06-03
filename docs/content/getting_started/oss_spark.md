---
title: "OSS Apache Spark Declarative Pipelines"
date: 2026-05-01
weight: 60
---

# Running SDP-META on OSS Apache Spark 4.1+

SDP-META targets the [Spark Declarative Pipelines (SDP) programming
guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
API. Every runtime call goes through `from pyspark import pipelines as
dp`, the public OSS module shipped with Apache Spark 4.1+.

The same code base runs in two modes:

- **Databricks Lakeflow** (default on Databricks). Lakeflow grafts a
  superset of the OSS API onto `pyspark.pipelines`, including DQE
  expectations and AutoCDC flows. SDP-META uses the full superset when
  it's available.
- **OSS Apache Spark 4.1+** (via `spark-pipelines run`). The shim layer
  in `databricks.labs.sdp_meta.oss_pipelines` rewrites the call
  sequence to fit the OSS public API.

## Runtime detection

`oss_pipelines.RUNTIME` is computed once at import time, and
`is_oss()` / `is_databricks()` re-probe on every call so an env-var
flip after import takes effect immediately:

1. Honour the `SDP_META_RUNTIME` env var if set (`databricks` /
   `lakeflow` / `oss` / `oss-spark`).
2. Probe `pyspark.pipelines` for `create_auto_cdc_flow` — the
   narrowest single signal of a Lakeflow runtime (no OSS counterpart,
   and the more recent of the two main Lakeflow extensions). If
   present, treat as Databricks Lakeflow; otherwise OSS.

Probing a single symbol (rather than the AND of `expect_all` and
`create_auto_cdc_flow` used in earlier revisions) avoids drift if either
runtime ships a stub for one symbol but not the other.

Force a runtime explicitly:

```bash
export SDP_META_RUNTIME=oss   # local tests on a Lakeflow-installed Python
export SDP_META_RUNTIME=databricks   # if probing somehow misfires on Lakeflow
```

## Class layout

The runtime split is reflected in the class hierarchy:

- `DataflowPipeline` (base) — Databricks Lakeflow implementation. Uses
  the full superset of `pyspark.pipelines`, including `expect_*`
  decorators, `create_auto_cdc_flow`, `create_auto_cdc_from_snapshot_flow`,
  and Lakeflow-only kwargs (`cluster_by_auto`, `path`).
- `OSSDataflowPipeline` (subclass of `DataflowPipeline`) — OSS Apache
  Spark 4.1+ implementation. Overrides exactly the methods that diverge:
  `_register_table_with_dqe` (DQE constraints inlined into the query
  function via `df.where` / `raise_error` / log-only), `create_streaming_table`
  (Lakeflow-only kwargs filtered out), and `cdc_apply_changes` /
  `apply_changes_from_snapshot` (raise `NotImplementedError`).

`DataflowPipeline(spark, ...)` automatically returns an
`OSSDataflowPipeline` instance when the runtime probe lands on OSS, so
existing call sites keep working without changes. To pin the OSS class
explicitly (for tests, type checks, or to bypass detection), construct
it directly:

```python
from databricks.labs.sdp_meta import OSSDataflowPipeline

dlt = OSSDataflowPipeline(spark, dataflow_spec, view_name)
dlt.run()
```

## Feature parity matrix

| Feature | Lakeflow | OSS Apache Spark 4.1+ |
| --- | --- | --- |
| `@dp.table` (streaming + materialized) | yes | yes |
| `@dp.temporary_view` | yes | yes |
| `@dp.append_flow` | yes | yes |
| `dp.create_streaming_table` | yes (with `cluster_by_auto`, `path`, `expect_*`) | yes (kwargs filtered to OSS-supported set; `path` honoured via external-table side channel) |
| `dp.create_sink` (Delta / Kafka / custom) | yes | yes |
| `expect_or_drop` | native via `dp.expect_all_or_drop` | shim injects `df.where(...)` into the query function |
| `expect_or_fail` | native, **aborts the update before any write** | shim injects a synthetic `raise_error` column — **best-effort, raises during streaming execution after some rows in the same micro-batch may already have been emitted to downstream operators** (see [`expect_or_fail` semantics gap](#expect_or_fail-semantics-gap-on-oss)) |
| `expect_all` (metrics-only) | native, surfaces in event log | shim logs registration; no enforcement |
| Quarantine table (`expect_or_quarantine`) | yes | yes (uses the same shim path) |
| `cdcApplyChanges` (`create_auto_cdc_flow`) | yes | not supported, raises `NotImplementedError` |
| `applyChangesFromSnapshot` | yes | not supported, raises `NotImplementedError` |
| `cloudFiles` source (Auto Loader) | yes | **not supported** — Databricks-proprietary source; `spark-pipelines run` errors at source resolution. Use `json` / `csv` / `parquet` / `delta` instead, or stay on Lakeflow for true Auto Loader semantics |
| `eventhub` source (Databricks Event Hubs connector) | yes | not supported; use plain Spark `kafka` with `--packages spark-sql-kafka-0-10_2.13:...` |
| `cluster_by_auto` table kwarg | yes | dropped with warning |
| `path` table kwarg (filesystem location) | yes (flows through to `dp.table`) | OSS `dp.table` rejects `path`; SDP-META pre-creates an external Delta table at that location with the same name (see [Per-table path on OSS](#per-table-path-on-oss-external-delta-tables)) |
| `databricks labs sdp-meta deploy` | yes | n/a, use `spark-pipelines run` |
| Declarative Automation Bundles | yes | n/a |

### Per-table path on OSS (external Delta tables)

OSS `pyspark.pipelines.table` does not accept a `path` kwarg — that's a
Lakeflow extension. SDP-META still honours the per-table `path` from the
onboarding spec (`bronze_table_path_<env>` / `silver_table_path_<env>`)
on the OSS code path by pre-creating an **external Delta table** at the
requested location with the same fully-qualified name SDP-META then
passes to `dp.table(name=...)`.

Concretely, before the OSS planner sees `dp.table(name="bronze.customers", ...)`,
`OSSDataflowPipeline._register_table_with_dqe` runs:

```sql
CREATE SCHEMA IF NOT EXISTS `bronze`;
CREATE TABLE IF NOT EXISTS `bronze`.`customers`
USING DELTA LOCATION '/path/from/onboarding/spec/bronze/customers';
```

The subsequent `dp.table(name="bronze.customers")` resolves to the
existing external Delta table, so the streaming materialisation lands
at the path the onboarding spec asked for. `dp.create_streaming_table`
on the OSS subclass uses the same side channel.

The helper is **idempotent**: re-running the pipeline against a path
that already has a Delta log is a no-op. If the table is already
registered under the same name at a *different* location, the helper
emits a warning and lets Spark continue writing to the registered
location — drop the table (`DROP TABLE bronze.customers`) or align the
onboarding spec to silence it.

The same mechanism applies to the bronze/silver dataflowspec metadata
itself, except there `OnboardDataflowspec` writes the Delta data via
`DataFrameWriter.save(<path>)` and the runtime reads it via
`<layer>.dataflowspecPath` — no `dp.table` involvement, no side channel
needed (see [Driving the OSS pipeline from filesystem paths](#driving-the-oss-pipeline-from-filesystem-paths)).

#### Caveat — silver-table path binding on first run

The side channel needs a `StructType` to write a
`CREATE TABLE ... USING DELTA LOCATION` that succeeds at an empty
location — a schema-less `CREATE TABLE` over an empty path raises
`DELTA_FAILED_INFER_SCHEMA`.

`OSSDataflowPipeline._oss_struct_schema()` extracts the schema from
`self.schema_json`, which is populated for **bronze** specs (sourced
from `source_schema_path` in the onboarding spec). **Silver** specs
don't carry a schema — the silver shape is derived from the bronze
output plus the `silver_transformation_json` transforms, so there's
nothing to inline into the `CREATE TABLE` DDL up front.

What this means in practice for `silver_table_path_<env>` on OSS:

- **First run** at an empty silver path: `ensure_external_delta_table`
  attempts the schema-less `CREATE TABLE`, Delta refuses with
  `DELTA_FAILED_INFER_SCHEMA`, and the helper degrades to a single
  `WARNING` log line (`could not pre-register external Delta table …`)
  rather than aborting the pipeline. The silver table is then created
  by the pipeline itself at whatever location the OSS planner /
  catalog defaults pick — typically *not* the path the onboarding
  spec asked for.
- **Subsequent runs** against the same silver path now find a Delta
  log there (written by the first run) and the helper happily resolves
  to it.

Workarounds if you need the silver path bound on the first run:

1. Pre-create the silver Delta table at the configured location with
   the expected schema (a one-shot `CREATE TABLE silver.t (…) USING
   DELTA LOCATION '…'`) before the first pipeline run.
2. Run the pipeline once to materialise the silver path, accept the
   first-run warning, and rely on subsequent runs.
3. On Databricks Lakeflow, this caveat does not apply — `path` flows
   through to `dp.table` natively and Lakeflow handles path binding
   without the side channel.

Bronze tables are unaffected — they always have `source_schema_path`
in the onboarding spec, so the schema is always available on the
first run.

### `expect_or_fail` semantics gap on OSS

Lakeflow's `expect_all_or_fail` aborts the **update** before any write
happens — Lakeflow's planner runs an expectation pre-flight, so no
violating row ever lands in the target table and no commit is made.

The OSS shim has no equivalent pre-flight surface, so it raises
**during streaming execution** on the first violating row pulled
through the plan. By the time the exception propagates:

- Some rows from the same micro-batch may already have been emitted to
  downstream operators (`append_flow` consumers, sinks, etc.).
- The failing micro-batch will be retried on resume — Spark's
  structured-streaming engine treats it as a transient task failure and
  re-executes from the same offsets. If the violation is data-driven
  (the same row will violate again), the stream stays in a crash loop
  until the bad row ages out of the source or the checkpoint is bumped
  past it.
- The target table commit for the failing micro-batch is rolled back by
  Spark's atomic-commit semantics, but commits for prior micro-batches
  in the same update are **not** rolled back — they already succeeded.

Don't migrate a Lakeflow spec that depends on the "no violating row
ever reaches the target" guarantee without re-validating against this
OSS behaviour. For workloads where the pre-write guarantee matters,
stay on Lakeflow, or fall back to `expect_or_quarantine` on OSS so
violating rows divert to a side table instead of crashing the stream.

## OSS template

A minimal OSS-runtime template ships under
[`src/databricks/labs/sdp_meta/templates/oss/`](https://github.com/databrickslabs/dlt-meta/tree/main/src/databricks/labs/sdp_meta/templates/oss):

- `spark-pipeline.yml.tmpl` — the OSS pipeline spec consumed by
  `spark-pipelines run --spec spark-pipeline.yml`. Conforms to the
  canonical [SDP pipeline-projects spec](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html#pipeline-projects):
  `libraries` and `storage` are required; `catalog` / `database`
  (`schema` is an accepted alias) are **optional** and left commented
  out because SDP-META always fully qualifies its `dp.table(name=...)`
  calls from the onboarding spec. Pins
  `spark.executorEnv.SDP_META_RUNTIME=oss` so executors agree with the
  driver.
- `run_pipeline.py.tmpl` — the top-level Python source file that the
  OSS planner imports. It calls
  `DataflowPipeline.invoke_pipeline(spark, layer)` at import time so
  the SDP graph builder discovers the `@dp.table` decorators that
  `invoke_pipeline` registers.
- `README.md` — what works / what doesn't on OSS, plus a quickstart.

The Lakeflow path uses the runner notebook under `templates/dab/`. The
Python entry point (`DataflowPipeline.invoke_pipeline(spark, layer)`)
is identical between the two runtimes; only the wrapper differs.

## Invoking `OSSDataflowPipeline` from a notebook

Production OSS Spark Declarative Pipelines is a CLI-driven flow
(`spark-pipelines run --spec spark-pipeline.yml`), so the **canonical**
entry point for `OSSDataflowPipeline` is the `.py` library file
referenced by the YAML spec — that's what
[`templates/oss/run_pipeline.py.tmpl`](https://github.com/databrickslabs/dlt-meta/tree/main/src/databricks/labs/sdp_meta/templates/oss/run_pipeline.py.tmpl)
sets up. Notebooks come into play in three scenarios:

### Scenario 1 — Databricks notebook, force the OSS code path

Useful for parity testing — verify your dataflow spec works on the OSS
subset before you run it on plain Spark. Cell layout mirrors the
existing Lakeflow runner template
([`init_sdp_meta_pipeline.py.tmpl`](https://github.com/databrickslabs/dlt-meta/tree/main/src/databricks/labs/sdp_meta/templates/dab/template/%7B%7B.bundle_name%7D%7D/notebooks/init_sdp_meta_pipeline.py.tmpl)).
Set `SDP_META_RUNTIME=oss` *before* importing `DataflowPipeline` so the
runtime probe lands on OSS:

```python
# Databricks notebook source
import os
os.environ["SDP_META_RUNTIME"] = "oss"

sdp_meta_dependency = spark.conf.get("sdp_meta_dependency")
%pip install $sdp_meta_dependency

# COMMAND ----------

layer = spark.conf.get("layer", "bronze_silver")
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_pipeline(spark, layer)
```

The `__new__` factory in `DataflowPipeline` sees the env var and
dispatches to `OSSDataflowPipeline` automatically — no other code
changes required. AutoCDC paths in the dataflow spec will raise
`NotImplementedError`, which is precisely what you want for parity
testing.

### Scenario 2 — Pin `OSSDataflowPipeline` explicitly

Skips runtime probing entirely. Use this when you want the OSS subset
enforced at *code* level (clearer intent, no env var to forget, fails
fast on a misconfigured Lakeflow runtime instead of silently using the
Lakeflow code path):

```python
%pip install $sdp_meta_dependency

# COMMAND ----------

from databricks.labs.sdp_meta import OSSDataflowPipeline
from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils

layer = spark.conf.get("layer", "bronze")
specs = (
    DataflowSpecUtils.get_bronze_dataflow_spec(spark)
    if layer == "bronze"
    else DataflowSpecUtils.get_silver_dataflow_spec(spark)
)
for spec in specs:
    target_view = (
        f"{spec.targetDetails['database']}_{spec.targetDetails['table']}"
        f"_{layer}_inputview"
    ).lower()
    OSSDataflowPipeline(spark, spec, view_name=target_view).run()
```

### Scenario 3 — Local Jupyter notebook with OSS Apache Spark 4.1+

Important caveat: in production, OSS SDP graph registration is driven
by the `spark-pipelines` CLI planner, which imports `.py` libraries
listed under `libraries.glob` and discovers `@dp.table` decorators at
that point. From a freestanding Jupyter notebook the planner is *not*
running, so calling `@dp.table` outside an active SDP graph build is a
no-op. The supported pattern is to keep the notebook as a *driver*
that shells out to `spark-pipelines run`:

```python
import subprocess, os
os.environ["SDP_META_RUNTIME"] = "oss"
subprocess.run(
    ["spark-pipelines", "run", "--spec", "spark-pipeline.yml"],
    check=True,
)
```

…and let `spark-pipeline.yml` point at the `run_pipeline.py` template
that calls `DataflowPipeline.invoke_pipeline(spark, layer)` (which
auto-dispatches to `OSSDataflowPipeline` because the spec sets
`spark.executorEnv.SDP_META_RUNTIME=oss`).

### Picking a scenario

| Goal | Pattern |
| --- | --- |
| Production OSS run | `spark-pipelines run --spec spark-pipeline.yml` (no notebook) |
| Test the OSS code path on Databricks | Scenario 1 — `SDP_META_RUNTIME=oss` env var |
| Force `OSSDataflowPipeline` explicitly | Scenario 2 — direct `OSSDataflowPipeline(...)` |
| Local OSS Spark in Jupyter | Scenario 3 — notebook drives the CLI |

Scenarios 1 and 2 are interchangeable behaviour-wise on Databricks.
Pick scenario 2 when you want the OSS subset enforced at code level,
scenario 1 when a single notebook should flip between runtimes via
spec configuration.

## Testing the OSS code path locally

You can exercise the full OSS code path on a laptop without a Spark
4.1+ install or a remote cluster. The repo ships a self-running smoke
test under
[`scripts/run_oss_demo.py`](https://github.com/databrickslabs/dlt-meta/tree/main/scripts/run_oss_demo.py)
that:

1. Forces `SDP_META_RUNTIME=oss` and installs an instrumented stub
   for `pyspark.pipelines` (records every `dp.table` /
   `dp.create_streaming_table` / `dp.append_flow` call).
2. Spins up a local `SparkSession` configured for Delta and **no**
   Unity Catalog.
3. Runs `OnboardDataflowspec` against
   [`tests/resources/oss_onboarding.json`](https://github.com/databrickslabs/dlt-meta/tree/main/tests/resources/oss_onboarding.json),
   writing the bronze and silver dataflowspec data to two
   filesystem-addressable Delta paths under a temp directory.
4. Wires those paths into Spark conf as
   `bronze.dataflowspecPath` / `silver.dataflowspecPath` (no
   `dataflowspecTable`), then calls
   `DataflowPipeline.invoke_pipeline(spark, "bronze_silver")`.
5. Asserts the recorded `pyspark.pipelines` calls match the OSS
   contract: no `expect_*`, no `create_auto_cdc_*`, no
   `cluster_by_auto` / `path` kwargs leaking through, and CDC
   methods raise `NotImplementedError`.
6. Asserts that each per-table `path` from the onboarding spec was
   registered as an external Delta table under the matching
   `dp.table(name=...)` (i.e. that the OSS path side channel
   described in [Per-table path on OSS](#per-table-path-on-oss-external-delta-tables)
   actually fired).

Run it from the repo root:

```bash
python scripts/run_oss_demo.py
```

A successful run ends with `OSS demo PASSED.` and a summary of the
recorded `pyspark.pipelines` calls. Useful flags:

```bash
# Keep the temp work dir for inspection (Delta paths + onboarding spec)
SDP_META_OSS_DEMO_KEEP=1 python scripts/run_oss_demo.py

# Print every recorded dp(...) call with its args/kwargs
SDP_META_OSS_DEMO_VERBOSE=1 python scripts/run_oss_demo.py
```

The script auto-detects when a real `pyspark.pipelines` (Spark 4.1+)
is available and prints the `spark-pipelines run` command you'd use
for true end-to-end testing against the OSS planner.

### Driving the OSS pipeline from filesystem paths

Production OSS runs typically don't have Unity Catalog or a Hive
metastore — there's nothing to register tables in. SDP-META supports
this by letting you address the bronze and silver dataflowspec data
**by Delta path** instead of by registered table name.

1. Run onboarding with `*_dataflowspec_path` set so the dataflowspec
   Delta data is written to a known location:

   ```python
   onboarding_params = {
       "onboarding_file_path": "/path/to/oss_onboarding.json",
       "database": "sdp_meta_oss",
       "env": "dev",
       "bronze_dataflowspec_table": "bronze_dataflowspec",
       "bronze_dataflowspec_path": "/data/sdp_meta/bronze",
       "silver_dataflowspec_table": "silver_dataflowspec",
       "silver_dataflowspec_path": "/data/sdp_meta/silver",
       "import_author": "ci",
       "version": "v1",
       "overwrite": "True",
   }
   OnboardDataflowspec(spark, onboarding_params, uc_enabled=False).onboard_dataflow_specs()
   ```

2. Point the runtime at those paths via Spark conf and invoke the
   pipeline as usual:

   ```python
   spark.conf.set("layer", "bronze_silver")
   spark.conf.set("bronze.dataflowspecPath", "/data/sdp_meta/bronze")
   spark.conf.set("silver.dataflowspecPath", "/data/sdp_meta/silver")
   DataflowPipeline.invoke_pipeline(spark, "bronze_silver")
   ```

`<layer>.dataflowspecPath` takes precedence over
`<layer>.dataflowspecTable` when both are set. At least one must be
set for each layer the pipeline addresses.

## Working around the AutoCDC gap

`create_auto_cdc_flow` and `create_auto_cdc_from_snapshot_flow` are
Databricks-only. To run an SDP-META pipeline that has
`cdcApplyChanges` set on OSS Spark, either:

1. Switch to Lakeflow for the CDC dataflows, and run the rest on OSS.
2. Remove the `cdcApplyChanges` / `applyChangesFromSnapshot` block
   from the dataflow spec and model SCD history yourself with a Spark
   `MERGE INTO` job triggered after each pipeline update, or model SCD
   Type 2 history as append + windowed view on top of the streaming
   table.

A first-class OSS implementation of `create_auto_cdc_flow` would
require streaming MERGE primitives that OSS `pyspark.pipelines` does
not currently expose. There's no community proposal upstream to add
them as of Spark 4.1.

## Backwards compatibility

The new public names are additive. The legacy names are unchanged:

| New name | Legacy name (still canonical) |
| --- | --- |
| `DataflowPipeline.invoke_pipeline` | `DataflowPipeline.invoke_dlt_pipeline` |
| `DataflowPipeline.run` | `DataflowPipeline.run_dlt` |
| `SinkWriter` (in `pipeline_writers`) | `DLTSinkWriter` |

Every demo notebook, integration test, example, and Declarative
Automation Bundle template that ships in this repo continues to call
the legacy names. Existing customer pipelines do not need to change.
