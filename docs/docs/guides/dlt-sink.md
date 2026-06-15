---
id: dlt-sink
title: DLT Sink
sidebar_position: 6
---

# DLT Sink

The DLT Sink feature writes pipeline output to an external Delta table or Kafka topic outside the pipeline's managed storage. SDP-META maps `bronze_sink` and `silver_sink` directly to the Declarative Pipeline Sink API.

![Delta sink](/img/dlt_delta_sink.png)

![Kafka sink](/img/dlt_kafka_sink.png)

![Sink demo overview](/img/dlt_demo_sink.png)

## `bronze_sink` configuration

### Delta sink

```json
{
  "bronze_sink": {
    "name": "bronze_sink",
    "format": "delta",
    "options": {
      "tableName": "my_catalog.my_schema.my_external_table"
    }
  }
}
```

### Kafka sink

```json
{
  "bronze_sink": {
    "name": "bronze_sink",
    "format": "kafka",
    "options": {
      "kafka.bootstrap.servers": "broker-host:9092",
      "subscribe": "my-output-topic"
    }
  }
}
```

## `silver_sink` configuration

Same structure as `bronze_sink`:

```json
{
  "silver_sink": {
    "name": "silver_sink",
    "format": "delta",
    "options": {
      "tableName": "my_catalog.my_schema.my_silver_external_table"
    }
  }
}
```

## Sink object fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Logical name for the sink |
| `format` | string | Yes | `delta` or `kafka` |
| `options` | object | Yes | Format-specific options |

**Delta sink options:**

| Option | Description |
|---|---|
| `tableName` | Fully qualified table name: `catalog.schema.table` |

**Kafka sink options:**

| Option | Description |
|---|---|
| `kafka.bootstrap.servers` | Comma-separated broker addresses |
| `subscribe` | Target Kafka topic name |

## Full example

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "orders_group",
    "source_format": "cloudFiles",
    "source_details": {
      "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/orders.ddl",
      "source_path_dev": "s3://my-bucket/landing/orders/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "orders_bronze",
    "bronze_sink": {
      "name": "bronze_sink",
      "format": "delta",
      "options": {
        "tableName": "external_catalog.archive_schema.orders_archive"
      }
    },
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "orders_silver",
    "silver_transformation_json": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations.json",
    "silver_sink": {
      "name": "silver_sink",
      "format": "kafka",
      "options": {
        "kafka.bootstrap.servers": "broker-host:9092",
        "subscribe": "orders-processed"
      }
    }
  }
]
```

## Related

- [Declarative Pipeline Sink API](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-sink)
- [Onboarding File Fields — `bronze_sink` and `silver_sink`](../reference/onboarding-fields#bronze-layer-fields)
