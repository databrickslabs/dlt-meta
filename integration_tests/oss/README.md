# SDP-META OSS Spark integration test suite

End-to-end **flow** tests for the OSS Apache Spark code path of
SDP-META — `onboarding → bronze pipeline → silver pipeline → row-count
validation`, run locally with no Databricks workspace dependency. The
shape mirrors the existing Databricks integration tests under
`integration_tests/` (driven by `run_integration_tests.py`); this suite
is the OSS equivalent driven by `run_oss_integration_tests.py`.

## What's tested

| Scenario     | Source format              | Pipeline reader exercised                          | Notes                                                  |
|--------------|----------------------------|----------------------------------------------------|--------------------------------------------------------|
| `json`       | JSON files                 | `PipelineReaders.read_dlt_file_source` (json)      | Uses the committed `tests/resources/oss_onboarding.json` |
| `csv`        | CSV files                  | `PipelineReaders.read_dlt_file_source` (csv)       | Source data derived from JSON test data                |
| `parquet`    | Parquet files              | `PipelineReaders.read_dlt_file_source` (parquet)   | Source data derived from JSON test data                |
| `delta`      | Delta tables (registered)  | `PipelineReaders.read_dlt_delta` (catalog lookup)  | Asserts bronze row count equals source row count       |
| `dqe`        | JSON files + `expect_or_drop` | DQE inlining via `oss_pipelines.wrap_dqe`       | Asserts exact post-filter row count                    |
| `cdc_raises` | JSON files + `cdcApplyChanges` | CDC dispatch raise path                       | Asserts `NotImplementedError("create_auto_cdc_flow")`  |

Every flow scenario exercises the full `OSSDataflowPipeline` code path:

- Runtime detection (`is_oss()`, `SDP_META_RUNTIME` override)
- Spec resolution by `<layer>.dataflowspecPath` (no UC, no metastore)
- Side-channel external Delta pre-create via
  `oss_pipelines.ensure_external_delta_table` so the write lands at
  the per-table `bronze_table_path_dev` / `silver_table_path_dev`
- Kwarg filtering via `oss_pipelines.filter_table_kwargs` (drops
  Lakeflow-only kwargs OSS `dp.table` rejects)
- DQE expectation inlining via `oss_pipelines.wrap_dqe` (since OSS
  `dp.table` has no `expect_all` parameter)
- Per-layer view materialization (`dp.temporary_view`) and table
  registration (`dp.table`)

## How the executor works (and why Spark 4.1+ isn't required)

The suite runs against **plain Spark 3.5+ with delta-spark** — not
Spark 4.1+ with `pyspark[pipelines]`. We achieve end-to-end row-count
validation without the pipelines runtime by:

1. Installing a recorder (`integration_tests/oss/conftest.py::_DPRecorder`)
   into `sys.modules["pyspark.pipelines"]` at conftest import time
   (before any sdp-meta import).
2. Letting SDP-META's `OSSDataflowPipeline._register_table_with_dqe`
   wire the bronze / silver pipeline against the recorder — every
   `dp.temporary_view` and `dp.table(qf, name=..., ...)` call is
   captured as a `(api, args, kwargs)` tuple.
3. Replaying the captured registrations against real Spark + Delta
   via `integration_tests/oss/_executor.py::FakeOSSPipelineExecutor`:
   pass-1 materialises every `temporary_view` qf as a session-local
   view; pass-2 executes each `table` qf and writes the result to
   the side-channelled external Delta location via
   `writeStream.trigger(availableNow=True).toTable(name)`.

The `OSSDataflowPipeline` code path under test is unchanged from what
runs against real OSS Spark `pyspark.pipelines`. Only the execution
step is local-Spark-shaped instead of `spark-pipelines run`-shaped.

## Prerequisites

- Python 3.11+ with the dev requirements installed (`pip install -e .`
  plus `pytest`).
- `pyspark` 3.5+ (any version pulled in by the project install will
  work; the suite explicitly does NOT require `pyspark[pipelines]`).
- `io.delta:delta-spark_2.12:3.0.0` — the Spark session config pulls
  it from Maven Central via `spark.jars.packages`; no manual install
  needed. Internet access is required for the first run only.

## Running

### Pytest harness

```bash
cd <repo-root>
PYTHONPATH=. python -m pytest integration_tests/oss/ -v
```

Run a single scenario:

```bash
PYTHONPATH=. python -m pytest integration_tests/oss/test_oss_dqe_flow.py -v
```

### Standalone runner (CSV output, mirrors `run_integration_tests.py`)

```bash
# All scenarios in sequence
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=all

# Single scenario
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=json
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=delta
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=dqe
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=cdc_raises

# Persistent workdir for debugging
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py \
    --source=all \
    --workdir=/tmp/sdp_meta_oss_it \
    --keep_artifacts
```

The runner writes a result CSV (default
`integration_test_output_<run_id>.csv` in CWD) with one row per
validation. Status is `PASS` / `FAIL`; exit code is `0` if every
validation passed, `1` otherwise.

Example CSV row:

```
run_id,scenario,layer,table,metric,expected,actual,status,note
8842ce4d83d5,dqe,bronze,bronze.customers,dqe_filtered_count,10,10,PASS,source=17 rows
```

## Debugging failed runs — keeping artifacts

Same contract as the Databricks runner. By default the runner cleans up its workdir on exit (success or failure). To preserve the workdir for inspection, either pass `--keep_artifacts` on the CLI or export `SDP_META_KEEP_ARTIFACTS=1` for the shell session:

```bash
# Per-invocation
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py \
    --source=dqe --workdir=/tmp/sdp_meta_oss_it --keep_artifacts

# Or, for an iterating debug session
export SDP_META_KEEP_ARTIFACTS=1
PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=all
```

Truthy values for `SDP_META_KEEP_ARTIFACTS` (case-insensitive): `1`, `true`, `yes`, `on`. Anything else — including unset — runs the default cleanup.

Two cases the runner preserves the workdir even without `--keep_artifacts`:

1. `--workdir=<path>` was passed (caller-supplied directory, never owned by the runner).
2. The run crashed before the cleanup `finally` block — in practice rare, but check `/var/folders/.../sdp_meta_oss_it_*` if you suspect orphaned directories.

Inspecting a preserved workdir:

```
<workdir>/
├── _warehouse/                              # local Spark warehouse (Hive)
├── _sources/<fmt>/{customers,transactions}/ # per-format materialised source data
├── <scenario>/
│   ├── spec/{bronze,silver}/                # Delta-backed dataflowspec
│   ├── bronze/{customers,transactions}/     # bronze target Delta tables
│   ├── silver/{customers,transactions}/     # silver target Delta tables
│   └── _checkpoints/                        # streaming checkpoints (one per table)
└── *_onboarding.json                        # rendered onboarding spec per scenario
```

To inspect a Delta table from the preserved workdir:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.read.format("delta").load("/tmp/sdp_meta_oss_it/dqe/bronze/customers").show()
```

The pytest harness uses pytest's per-test `tmp_path` instead of `SDP_META_KEEP_ARTIFACTS`. Run pytest with `--basetemp=/tmp/sdp_meta_oss_pytest` to pin the per-test directories at a known location and inspect them after a failure.

## Files in this directory

- `conftest.py` — pytest fixtures: Spark session, `_DPRecorder`
  installation, per-test catalog isolation, flow driver.
- `_executor.py` — `FakeOSSPipelineExecutor`: replays recorded
  `dp.table` / `dp.temporary_view` registrations against real Spark +
  Delta.
- `_fixtures.py` — per-format source-data + onboarding-template
  generators (used by CSV / Parquet / Delta scenarios).
- `test_oss_json_flow.py` — JSON full-flow + path-binding tests.
- `test_oss_csv_flow.py` — CSV full-flow + source-format dispatch test.
- `test_oss_delta_flow.py` — Delta-source full-flow + bronze
  identity-row-count test.
- `test_oss_dqe_flow.py` — DQE `expect_or_drop` end-to-end + bronze
  predicate-violation check.
- `test_oss_cdc_raises.py` — CDC `NotImplementedError` end-to-end with
  Lakeflow-API-name message assertion.

## What this suite intentionally does NOT cover

- **CloudFiles (`source_format=cloudFiles`)** — Auto Loader is
  Lakeflow-only by design.
- **Kafka / EventHub** — require running brokers; out of scope for a
  local integration suite.
- **Snapshot (`source_format=snapshot`)** — Lakeflow-only
  (`create_auto_cdc_from_snapshot_flow`). The `cdc_raises` scenario
  pins the analogous raise path for `cdcApplyChanges`; the snapshot
  variant raises via the same code path with a different API name in
  the message.
- **`SDP_META_OSS_REGISTER_STRICT`** — the strict-mode behavior is
  covered by unit tests in `tests/test_oss_pipelines.py`; not
  reproduced as a flow scenario here.

For coverage of those Databricks-specific paths, use the existing
`integration_tests/run_integration_tests.py` against a real
workspace.
