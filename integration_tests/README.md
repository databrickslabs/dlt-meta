#### Run Integration Tests
1. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

2. Clone sdp-meta:
    ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

3. Navigate to project directory:
    ```commandline
    cd sdp-meta
    ```

4. Create Python virtual environment:
    ```commandline
    python -m venv .venv
    ```

5. Activate virtual environment:
    ```commandline
    source .venv/bin/activate
    ```

6. Install required packages:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk
    
    # Development requirements
    pip install delta-spark==3.0.0 pyspark==3.5.5 pytest>=7.0.0 coverage>=7.0.0
    
    # Integration test requirements
    pip install "typer[all]==0.6.1"
    ```

7. Set environment variables:
    ```commandline
    sdp_meta_home=$(pwd)
    export PYTHONPATH=$sdp_meta_home
    ```

9. Run integration test against cloudfile or eventhub or kafka using below options. To use the Databricks profile configured using CLI then pass ```--profile <profile-name>``` to below command otherwise provide workspace url and token in command line. You will also need to provide a Unity Catalog catalog for which the schemas, tables, and files will be created in.

    By default the runner uses the **JSON** onboarding spec. To run the same test against the **YAML** onboarding spec instead, add ```--onboarding_file_format=yaml``` to any of the commands below. Either format produces equivalent test runs — the runner reads the YAML template, substitutes runtime placeholders, writes the rendered `onboarding.yml`, and feeds it through `OnboardDataflowspec` (which converts to a driver-local temp JSON before handing to Spark). See the **Onboarding file format (JSON or YAML)** section at the bottom of this README for full details.

    - 9a. Run the command for  **cloudfiles**
        ```commandline
        python integration_tests/run_integration_tests.py  --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```

      Same test, **YAML onboarding**:
        ```commandline
        python integration_tests/run_integration_tests.py --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --onboarding_file_format=yaml --profile=<<DEFAULT>>
        ```

    - 9b. Run the command for **eventhub**
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>> --source=eventhub --dltmeta_sink1=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer --eventhub_consumer_accesskey_name=consumer  --eventhub_name_append_flow=test_append_flow --eventhub_accesskey_secret_name=test_secret_name --profile=<<DEFAULT>>
        ```
    Prerequisites for eventhub integration tests:
    1. Running eventhub instance
    2. Create databricks secrets scope for eventhub keys:
       ```commandline
       databricks secrets create-scope eventhubs_creds
       ```
    3. Create databricks secrets to store producer and consumer keys using the scope created in step 2

    Required arguments for EventHubs integration test:
    1. `--eventhub_name` : Your eventhub topic
    2. `--eventhub_namespace` : Eventhub namespace
    3. `--eventhub_port` : Eventhub port
    4. `--eventhub_secrets_scope_name` : Databricks secret scope name
    5. `--eventhub_producer_accesskey_name` : Eventhub producer access key name
    6. `--eventhub_consumer_accesskey_name` : Eventhub access key name


    - 9c. Run the command for **kafka**
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>>  --source=kafka --kafka_source_topic=sdp-meta-integration-test --kafka_sink_topic=sdp-meta_inttest_topic --kafka_source_broker=host:9092 --profile=<<DEFAULT>>
        ```
    Optional secret configuration:
    ```commandline
    --kafka_source_servers_secrets_scope_name=<<scope_name>> --kafka_source_servers_secrets_scope_key=<<scope_key>>
    --kafka_sink_servers_secret_scope_name=<<scope_name>> --kafka_sink_servers_secret_scope_key=<<scope_key>>
    ```

    Prerequisites for kafka integration tests:
    1. Running kafka instance

    Required arguments for kafka integration test:
    1. `--kafka_topic` : Your kafka topic name
    2. `--kafka_broker` : Kafka broker address
    
    - 9d. Run the command for **snapshot**
        ```commandline
        python integration_tests/run_integration_tests.py --source=snapshot --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```

    > **Tip:** any of the four sources (`cloudfiles`, `eventhub`, `kafka`, `snapshot`) accepts ```--onboarding_file_format=yaml``` to run the same test against the YAML onboarding spec.


10. Once finished integration output file will be copied locally to
```integration-test-output_<run_id>.txt```

11. Output of a successful run should have the following in the file
```
,0
0,Completed Bronze Lakeflow Spark Declarative Pipeline.
1,Completed Silver Lakeflow Spark Declarative Pipeline.
2,Validating Lakeflow Spark Declarative Pipeline Bronze and Silver Table Counts...
3,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions.
4,Expected: 10002 Actual: 10002. Passed!
5,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions_quarantine.
6,Expected: 7 Actual: 7. Passed!
7,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.customers.
8,Expected: 98928 Actual: 98928. Passed!
9,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.customers_quarantine.
10,Expected: 1077 Actual: 1077. Passed!
11,Validating Counts for Table silver_7d1d3ccc9e144a85b07c23110ea50133.transactions.
12,Expected: 8759 Actual: 8759. Passed!
13,Validating Counts for Table silver_7d1d3ccc9e144a85b07c23110ea50133.customers.
14,Expected: 87256 Actual: 87256. Passed!
```

---

## OSS Apache Spark integration tests

The runner above (`run_integration_tests.py`) drives the **Databricks Lakeflow** code path and requires a workspace + UC catalog. SDP-META also has an **OSS Apache Spark** code path (`OSSDataflowPipeline`, the `pyspark.pipelines` runtime probe, the `ensure_external_delta_table` side-channel, DQE inlining via `wrap_dqe`, etc.), and that path has its own end-to-end flow integration suite under [`integration_tests/oss/`](oss/README.md).

The OSS suite mirrors the Databricks flow shape (`onboarding → bronze pipeline → silver pipeline → row-count validation`) but runs entirely locally — **no Databricks workspace, no UC catalog, no notebooks**, just local Spark 3.5+ with `delta-spark`.

Two equivalent entry points:

- **Standalone runner** (mirrors `run_integration_tests.py` CLI shape, produces a result CSV):
    ```commandline
    # All scenarios in sequence
    PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=all

    # Single scenario
    PYTHONPATH=. python integration_tests/run_oss_integration_tests.py --source=json
    ```

- **Pytest harness** (same coverage, native pytest reporting):
    ```commandline
    PYTHONPATH=. python -m pytest integration_tests/oss/ -v
    ```

Both entry points cover the same scenarios: `json`, `csv`, `parquet`, `delta`, `dqe` (DQE `expect_or_drop` row filtering), and `cdc_raises` (CDC raises `NotImplementedError` because `create_auto_cdc_flow` is Lakeflow-only). The standalone runner additionally writes `integration_test_output_<run_id>.csv` in the same shape as the Databricks runner's output.

See [`integration_tests/oss/README.md`](oss/README.md) for the full scenario matrix, the `FakeOSSPipelineExecutor` design that lets the suite validate row counts on plain Spark 3.5 (without requiring Spark 4.1+ `pyspark[pipelines]`), and what's intentionally out of scope (cloudFiles, Kafka, EventHub, snapshot — all Lakeflow-only, covered by the Databricks runner above).

`SDP_META_KEEP_ARTIFACTS=1` and `--keep_artifacts` work identically on the OSS runner — see the next section.

---

## Debugging failed runs — keeping artifacts

By default, the runner's `finally` block calls `clean_up()` after every run (success **or** failure), which drops the bronze/silver schemas, volumes, tables, and the workflow created for the test. This is great for CI, but painful when you're trying to inspect what a failed run actually wrote.

Set `SDP_META_KEEP_ARTIFACTS` before invoking the runner to skip cleanup:

```commandline
SDP_META_KEEP_ARTIFACTS=1 python integration_tests/run_integration_tests.py \
    --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
```

Accepted truthy values (case-insensitive): `1`, `true`, `yes`, `on`. Anything else — including unset — runs the default cleanup.

When the env var is honored you'll see this in the runner's stdout:

```
SDP_META_KEEP_ARTIFACTS is set; skipping clean_up. Run integration_tests/cleanup_script.py to remove artifacts when you're done debugging.
```

Once you're done inspecting:

```commandline
python integration_tests/cleanup_script.py
```

…tears down the leftover schemas, volumes, and tables in the catalog. (Edit the script's catalog/profile constants at the top if your run used non-default values.)

**Tip:** `export SDP_META_KEEP_ARTIFACTS=1` for the whole shell session if you're iterating on a flaky test — every subsequent run will leave its artifacts in place, and a single `cleanup_script.py` invocation at the end clears them all together.

---

## Onboarding file format (JSON or YAML)

The integration test runner can drive every supported source (`cloudfiles`, `eventhub`, `kafka`, `snapshot`) with either a JSON or a YAML onboarding spec. The format is selected per-run with a single CLI flag:

```commandline
--onboarding_file_format=json   # default
--onboarding_file_format=yaml   # also accepts 'yml'
```

### Where the conf files live

Templates and reference configs are organized into format-specific subdirectories so each format has its own self-contained tree:

```
integration_tests/conf/
├── json/
│   ├── cloudfiles-onboarding.template
│   ├── cloudfiles-onboarding_A2.template
│   ├── eventhub-onboarding.template
│   ├── kafka-onboarding.template
│   ├── snapshot-onboarding.template
│   ├── silver_transformations.json
│   ├── silver_transformations_snapshot.json
│   └── dqe/
│       ├── customers/{bronze,silver}_data_quality_expectations.json
│       ├── iot/{bronze,silver}_data_quality_expectations.json
│       └── transactions/{bronze,silver}_data_quality_expectations.json
└── yml/
    ├── cloudfiles-onboarding.template.yml
    ├── cloudfiles-onboarding_A2.template.yml
    ├── eventhub-onboarding.template.yml
    ├── kafka-onboarding.template.yml
    ├── snapshot-onboarding.template.yml
    ├── silver_transformations.yml
    ├── silver_transformations_snapshot.yml
    └── dqe/
        ├── customers/{bronze,silver}_data_quality_expectations.yml
        ├── iot/{bronze,silver}_data_quality_expectations.yml
        └── transactions/{bronze,silver}_data_quality_expectations.yml
```

The two trees are kept structurally equivalent: each YAML template references its `/yml/` siblings for `silver_transformation_*` and `*_data_quality_expectations_*` paths, and each JSON template references its `/json/` siblings. Pick a format and stay in it.

### How the path translation works

The dataclass defaults all point at the `/json/` tree. When the runner is started with `--onboarding_file_format=yaml`, `SDPMetaRunnerConf.__post_init__` rewrites every relevant path through a single helper, `_to_yaml_variant`:

| Default (JSON mode) | Becomes (YAML mode) |
|---|---|
| `integration_tests/conf/json/cloudfiles-onboarding.template` | `integration_tests/conf/yml/cloudfiles-onboarding.template.yml` |
| `integration_tests/conf/json/eventhub-onboarding.template`   | `integration_tests/conf/yml/eventhub-onboarding.template.yml`   |
| `integration_tests/conf/json/kafka-onboarding.template`      | `integration_tests/conf/yml/kafka-onboarding.template.yml`      |
| `integration_tests/conf/json/snapshot-onboarding.template`   | `integration_tests/conf/yml/snapshot-onboarding.template.yml`   |
| `integration_tests/conf/json/onboarding.json` *(generated)*  | `integration_tests/conf/yml/onboarding.yml` *(generated)*       |
| `integration_tests/conf/json/onboarding_A2.json` *(generated)* | `integration_tests/conf/yml/onboarding_A2.yml` *(generated)*  |

The transformation rules are:
1. Swap the path segment `/json/` → `/yml/`.
2. `.template` → `.template.yml`, `.json` → `.yml`. Already-YAML paths (`.yml`/`.yaml`) are returned unchanged.

### Inputs vs outputs

There are two distinct kinds of paths:

- **Templates (committed):** `cloudfiles_template`, `eventhub_template`, `kafka_template`, `snapshot_template`, `cloudfiles_A2_template`. Both `/json/*.template` and `/yml/*.template.yml` siblings exist on disk.
- **Onboarding outputs (generated):** `onboarding_file_path`, `onboarding_A2_file_path`, `onboarding_fanout_file_path`. The runner reads the template, substitutes runtime placeholders (`{uc_volume_path}`, `{uc_catalog_name}`, `{bronze_schema}`, etc.) via `generate_onboarding_file()`, and writes the rendered result to the format-specific output path. These files are gitignored — they only exist after a test run.

### What happens after the file is written

Once the rendered onboarding file is written, it is handed to `OnboardDataflowspec.__get_onboarding_file_dataframe`:

- `.json` path → read directly with `spark.read.option("multiline","true").json(...)`.
- `.yml`/`.yaml` path → `convert_yml_to_json(...)` parses the YAML via Spark IO (so cloud paths like `/Volumes/...`, `dbfs:/...`, `s3://...`, `abfss://...` all work), serializes the parsed structure to a **driver-local** temp JSON file (`tempfile.mkdtemp("sdp_meta_onboarding_")`), and the local file is then handed to Spark's native JSON reader. The original YAML on the (possibly remote) source filesystem is never modified, and no JSON sibling is written next to it.

DQE files and silver-transformation files referenced from inside the onboarding spec follow the same `_load_structured_file` path: both `.json` and `.yml` extensions parse transparently.

### Worked example

Run cloudfiles end-to-end against the YAML spec:

```commandline
python integration_tests/run_integration_tests.py \
    --source=cloudfiles \
    --uc_catalog_name=<<uc catalog name>> \
    --onboarding_file_format=yaml \
    --profile=<<DEFAULT>>
```

What the runner does, in order:

1. `SDPMetaRunnerConf.__post_init__` sees `onboarding_file_format=yaml` and rewrites:
    - `cloudfiles_template` → `integration_tests/conf/yml/cloudfiles-onboarding.template.yml`
    - `cloudfiles_A2_template` → `integration_tests/conf/yml/cloudfiles-onboarding_A2.template.yml`
    - `onboarding_file_path` → `integration_tests/conf/yml/onboarding.yml`
    - `onboarding_A2_file_path` → `integration_tests/conf/yml/onboarding_A2.yml`
2. `generate_onboarding_file` reads the YAML template as text, substitutes placeholders, and writes the rendered YAML to `integration_tests/conf/yml/onboarding.yml`.
3. The rendered onboarding YAML is uploaded to the UC volume alongside its referenced YAML DQE and silver-transformation files.
4. The pipeline runs and validates row counts. The output file `integration-test-output_<run_id>.txt` is identical in shape to the JSON-mode output.

### Troubleshooting

- **`FileNotFoundError: integration_tests/conf/yml/<something>.template.yml`** — the YAML sibling for that source is missing. All five committed templates listed above must be present; the `_to_yaml_variant` helper does not synthesize missing siblings.
- **`Onboarding file format not supported!`** — `onboarding_file_path` does not end in `.json`, `.yml`, or `.yaml`. Check that you didn't override `--onboarding_file_path` with a non-conforming extension.
- **`YAML onboarding file '...' is empty or could not be parsed`** — the YAML template rendered to an empty document or the YAML is malformed after substitution. Inspect the generated `integration_tests/conf/yml/onboarding.yml` from the failed run.