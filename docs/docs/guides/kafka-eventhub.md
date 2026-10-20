---
id: kafka-eventhub
title: Kafka & Event Hubs
sidebar_position: 2
---

# Kafka & Event Hubs

SDP-META supports streaming ingestion from Apache Kafka and Azure Event Hubs. Event Hubs uses the Kafka protocol endpoint, so both share the same underlying Spark Structured Streaming reader.

## Apache Kafka

### Prerequisites

- A running Kafka broker reachable from your Databricks cluster
- A Kafka topic already created

### Onboarding configuration

```json
{
  "data_flow_id": "1",
  "data_flow_group": "streaming_group",
  "source_format": "kafka",
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/events.ddl",
    "kafka.bootstrap.servers": "broker-host:9092",
    "subscribe": "my-kafka-topic"
  },
  "bronze_catalog_dev": "my_catalog",
  "bronze_database_dev": "streaming_bronze",
  "bronze_table": "events_bronze",
  "bronze_reader_options": {
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
  }
}
```

### Key `source_details` fields for Kafka

| Field | Description |
|---|---|
| `source_schema_path` | Path to the Spark DDL schema file |
| `kafka.bootstrap.servers` | Comma-separated broker addresses |
| `subscribe` | Kafka topic name |
| `kafka.sasl.mechanism` | SASL mechanism, e.g. `PLAIN`, `SCRAM-SHA-256` |
| `kafka.security.protocol` | Security protocol, e.g. `SASL_SSL`, `PLAINTEXT` |

### Running the Kafka demo

```bash
python integration_tests/run_integration_tests.py \
  --cloud_provider_name=aws \
  --dbr_version=15.3.x-scala2.12 \
  --source=kafka \
  --uc_catalog_name=<your_catalog> \
  --kafka_topic_name=sdp-meta-integration-test \
  --kafka_broker=host:9092
```

## Azure Event Hubs

Azure Event Hubs exposes a Kafka-compatible endpoint. SDP-META supports SAS
policies stored in Databricks Secrets and Databricks service credentials.

### Prerequisites

1. An Azure Event Hubs namespace with a topic (Event Hub) and a SAS policy with `Listen` permission
2. The SAS key stored in Databricks Secrets:

```bash
databricks secrets create-scope eventhubs_creds
databricks secrets put-secret eventhubs_creds consumer --string-value "<your-sas-key>"
databricks secrets put-secret eventhubs_creds producer --string-value "<your-sas-key>"
```

### Onboarding configuration

```json
{
  "data_flow_id": "1",
  "data_flow_group": "iot_group",
  "source_format": "eventhub",
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/iot.ddl",
    "eventhub.accessKeyName": "consumer",
    "eventhub.accessKeySecretName": "consumer",
    "eventhub.name": "iot-events",
    "eventhub.secretsScopeName": "eventhubs_creds",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "eventhub.namespace": "my-eventhubs-namespace",
    "eventhub.port": "9093"
  },
  "bronze_catalog_dev": "my_catalog",
  "bronze_database_dev": "iot_bronze",
  "bronze_table": "iot_bronze"
}
```

### Key `source_details` fields for Event Hubs

| Field | Description |
|---|---|
| `source_schema_path` | Path to the Spark DDL schema file |
| `eventhub.accessKeyName` | SAS policy name; required for SAS-policy authentication only |
| `eventhub.accessKeySecretName` | Databricks Secrets key containing the SAS key; SAS-policy authentication only (defaults to `eventhub.accessKeyName`) |
| `eventhub.name` | Event Hub topic name |
| `eventhub.secretsScopeName` | Databricks Secrets scope; required for SAS-policy authentication only |
| `kafka.sasl.mechanism` | Set automatically to `PLAIN` for SAS-policy authentication; omit for service credentials |
| `kafka.security.protocol` | Set automatically to `SASL_SSL` for SAS-policy authentication; omit for service credentials |
| `eventhub.namespace` | Namespace (without `.servicebus.windows.net`) |
| `eventhub.port` | Always `9093` for Event Hubs |

### Service credential authentication

To use an Azure managed identity through a Databricks service credential, set
`databricks.serviceCredential` in `bronze_reader_options` for a source or in
an Event Hubs sink's `options`. Do not configure SAS policy fields (`eventhub.accessKeyName`,
`eventhub.accessKeySecretName`, `eventhub.secretsScopeName`) or
`kafka.sasl.*` / `kafka.security.protocol` options for this mode. SDP-META
rejects mixed authentication configuration. It also requires the Event Hubs
endpoint and topic to be supplied through the Event Hubs fields; do not set
`kafka.bootstrap.servers`, `subscribe`, `subscribePattern`, `assign`, `topic`,
or SASL Kafka connection options alongside a service credential. These
restrictions apply only to service-credential authentication. SAS-policy
sources continue to allow existing reader-option overrides, while SAS-policy
sinks retain the established behavior of deriving their endpoint and topic
from the Event Hubs fields.

```yaml
source_format: eventhub
source_details:
  eventhub.namespace: my-eventhubs-namespace
  eventhub.name: iot-events
  eventhub.port: 9093
bronze_reader_options:
  databricks.serviceCredential: eventhubs-managed-identity
  startingOffsets: earliest
  failOnDataLoss: false
```

When no service credential is configured, SDP-META retains the existing SAS
policy and Databricks Secrets authentication behavior.

### Event Hubs demo output

![Event Hubs demo result](/img/af_eh_demo.png)

### Running the Event Hubs demo

```bash
python integration_tests/run_integration_tests.py \
  --cloud_provider_name=azure \
  --dbr_version=15.3.x-scala2.12 \
  --source=eventhub \
  --uc_catalog_name=<your_catalog> \
  --eventhub_name=iot \
  --eventhub_secrets_scope_name=eventhubs_creds \
  --eventhub_namespace=my-eventhubs-namespace \
  --eventhub_port=9093 \
  --eventhub_producer_accesskey_name=producer \
  --eventhub_consumer_accesskey_name=consumer
```

## Related

- [Onboarding File Fields — eventhub source_details](../reference/onboarding-fields#source_details--eventhub)
- [Onboarding File Fields — kafka source_details](../reference/onboarding-fields#source_details--kafka)
- [Integration Tests](../operations/integration-tests)
