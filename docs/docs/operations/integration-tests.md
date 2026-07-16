---
id: integration-tests
title: Integration Tests
sidebar_position: 2
---

# Integration Tests

The SDP-META integration test suite runs end-to-end pipeline tests against a real Databricks workspace, covering cloudFiles, Event Hubs, and Kafka source types.

## Initial setup

**Prerequisites:**

- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/profiles.html) installed and authenticated
- Python 3.10–3.12
- A Databricks workspace with Unity Catalog enabled

```bash
git clone https://github.com/databrickslabs/sdp-meta.git
cd sdp-meta
python -m venv .venv
source .venv/bin/activate
pip install databricks-sdk
export PYTHONPATH=$(pwd)
```

## Running integration tests

Pass `--profile <profile-name>` for a named Databricks CLI profile, or provide `--workspace-url` and `--token` directly.

### cloudFiles

```bash
python integration_tests/run_integration_tests.py \
  --source=cloudfiles \
  --uc_catalog_name=<your_catalog>
```

### Event Hubs

**Prerequisites:** A running Azure Event Hubs instance and Databricks Secrets with producer and consumer keys:

```bash
databricks secrets create-scope eventhubs_creds
databricks secrets put-secret eventhubs_creds producer --string-value "<producer-sas-key>"
databricks secrets put-secret eventhubs_creds consumer --string-value "<consumer-sas-key>"
```

```bash
python integration_tests/run_integration_tests.py \
  --cloud_provider_name=azure \
  --dbr_version=15.3.x-scala2.12 \
  --source=eventhub \
  --uc_catalog_name=<your_catalog> \
  --eventhub_name=iot \
  --eventhub_secrets_scope_name=eventhubs_creds \
  --eventhub_namespace=<your-namespace> \
  --eventhub_port=9093 \
  --eventhub_producer_accesskey_name=producer \
  --eventhub_consumer_accesskey_name=consumer
```

**Required arguments for Event Hubs:**

| Argument | Description |
|---|---|
| `--eventhub_name` | Event Hub topic name |
| `--eventhub_namespace` | Namespace (without `.servicebus.windows.net`) |
| `--eventhub_port` | Always `9093` |
| `--eventhub_secrets_scope_name` | Databricks Secrets scope name |
| `--eventhub_producer_accesskey_name` | Secrets key name for the producer SAS key |
| `--eventhub_consumer_accesskey_name` | Secrets key name for the consumer SAS key |

### Kafka

**Prerequisites:** A running Kafka broker reachable from Databricks and a Kafka topic created.

```bash
python integration_tests/run_integration_tests.py \
  --cloud_provider_name=aws \
  --dbr_version=15.3.x-scala2.12 \
  --source=kafka \
  --uc_catalog_name=<your_catalog> \
  --kafka_topic_name=sdp-meta-integration-test \
  --kafka_broker=host:9092
```

**Required arguments for Kafka:**

| Argument | Description |
|---|---|
| `--kafka_topic_name` | Kafka topic name |
| `--kafka_broker` | Broker address, e.g. `host:9092` |

## Test output

Results are written to `integration-test-output_<run_id>.txt`. A successful run:

```
0,Completed Bronze Lakeflow Spark Declarative Pipeline.
1,Completed Silver Lakeflow Spark Declarative Pipeline.
2,Validating Lakeflow Spark Declarative Pipeline Bronze and Silver Table Counts...
3,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions.
4,Expected: 10002 Actual: 10002. Passed!
5,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions_quarantine.
6,Expected: 7 Actual: 7. Passed!
...
```

## Running unit tests locally

Unit tests do not require a Databricks workspace:

```bash
pip install flake8==6.0 delta-spark==3.0.0 pytest>=7.0.0 coverage>=7.0.0 pyspark==3.5.5
pytest tests/
coverage run -m pytest tests/
coverage report
```

:::warning
Unit tests require Python 3.10–3.12. See [Troubleshooting — Python version issues](./troubleshooting#python-version-issues).
:::
