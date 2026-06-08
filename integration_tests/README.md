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

    - 9e. Run the command for **multi_source_cdc** (issue [#294](https://github.com/databrickslabs/dlt-meta/issues/294))
        ```commandline
        python integration_tests/run_integration_tests.py --source=multi_source_cdc --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```

        Same test, **YAML onboarding**:
        ```commandline
        python integration_tests/run_integration_tests.py --source=multi_source_cdc --uc_catalog_name=<<uc catalog name>> --onboarding_file_format=yaml --profile=<<DEFAULT>>
        ```

        End-to-end checks for the multi-source AUTO CDC code path. Seeds three regional bronze CDC tables (`customers_us_cdc`, `customers_eu_cdc`, `customers_apac_cdc`) — each with a different source column shape on purpose (US: `id`/`firstname`/`operation`; EU: `customer_id`/`given_name`/`change_type`; APAC: `cust_id`/`fname`/`op`) — then runs a single silver pipeline that merges all three into one unified `customers` SCD-1 table via `silver_cdc_apply_changes_flows` with per-flow `select_exp` normalization. The validator asserts per-region bronze counts, the silver live-row total, the per-region silver breakdown (proves each per-flow `select_exp` actually ran), and the exact surviving `customer_id` set.

        The workflow is a minimal 3-task fan-in: `setup_sdp_meta_pipeline_spec → sdp-meta-pipeline → validate_results` (no A2 incremental step, no publish-events step). The `sdp-meta-pipeline` task runs **one** combined Lakeflow Spark Declarative Pipeline configured with `layer=bronze_silver` (groups `bronze.group=A1` and `silver.group=A1`), so all three regional bronze CDC tables AND the unified silver multi-source AUTO CDC merge execute inside a single observable DLT flow graph — matching Stage 11 of the interactive demo notebook and the standalone `demo/launch_multi_source_cdc_demo.py`. Seed data lives under [`integration_tests/resources/data/multi_source_cdc/`](resources/data/multi_source_cdc/) and the onboarding template is [`integration_tests/conf/json/multi-source-cdc-onboarding.template`](conf/json/multi-source-cdc-onboarding.template) (YAML sibling: [`conf/yml/multi-source-cdc-onboarding.template.yml`](conf/yml/multi-source-cdc-onboarding.template.yml)).

    > **Tip:** any of the five sources (`cloudfiles`, `eventhub`, `kafka`, `snapshot`, `multi_source_cdc`) accepts ```--onboarding_file_format=yaml``` to run the same test against the YAML onboarding spec.


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

## Onboarding file format (JSON or YAML)

The integration test runner can drive every supported source (`cloudfiles`, `eventhub`, `kafka`, `snapshot`, `multi_source_cdc`) with either a JSON or a YAML onboarding spec. The format is selected per-run with a single CLI flag:

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
│   ├── multi-source-cdc-onboarding.template
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
    ├── multi-source-cdc-onboarding.template.yml
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
| `integration_tests/conf/json/multi-source-cdc-onboarding.template` | `integration_tests/conf/yml/multi-source-cdc-onboarding.template.yml` |
| `integration_tests/conf/json/onboarding.json` *(generated)*  | `integration_tests/conf/yml/onboarding.yml` *(generated)*       |
| `integration_tests/conf/json/onboarding_A2.json` *(generated)* | `integration_tests/conf/yml/onboarding_A2.yml` *(generated)*  |

The transformation rules are:
1. Swap the path segment `/json/` → `/yml/`.
2. `.template` → `.template.yml`, `.json` → `.yml`. Already-YAML paths (`.yml`/`.yaml`) are returned unchanged.

### Inputs vs outputs

There are two distinct kinds of paths:

- **Templates (committed):** `cloudfiles_template`, `eventhub_template`, `kafka_template`, `snapshot_template`, `cloudfiles_A2_template`, `multi_source_cdc_template`. Both `/json/*.template` and `/yml/*.template.yml` siblings exist on disk.
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