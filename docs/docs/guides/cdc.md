---
id: cdc
title: CDC with apply_changes
sidebar_position: 3
---

# CDC with apply_changes

Use this pattern when your source system emits a CDC stream with insert, update, and delete events. SDP-META maps `bronze_cdc_apply_changes` and `silver_cdc_apply_changes` directly to the Declarative Pipeline `create_auto_cdc_flow` API.

Supported SCD types:
- **Type 1** (`scd_type: 1`) — overwrite the existing row; no history retained
- **Type 2** (`scd_type: 2`) — retain full row history with validity timestamps

## `bronze_cdc_apply_changes` configuration

```json
{
  "bronze_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "dmsTimestamp",
    "scd_type": "1",
    "apply_as_deletes": "Op = 'D'",
    "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `keys` | array of strings | Yes | Primary key columns |
| `sequence_by` | string | Yes | Column(s) used to order events and determine the most recent |
| `scd_type` | string | Yes | `1` for overwrite or `2` for history |
| `apply_as_deletes` | string | No | SQL expression identifying delete events |
| `except_column_list` | array of strings | No | Columns to exclude from the target table |
| `track_history_column_list` | array of strings | No | (Type 2 only) Columns whose changes trigger a new history row |
| `track_history_except_column_list` | array of strings | No | (Type 2 only) Columns to exclude from history tracking |

## `silver_cdc_apply_changes` configuration

Same structure as `bronze_cdc_apply_changes`:

```json
{
  "silver_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "dmsTimestamp,enqueueTimestamp,sequenceId",
    "scd_type": "2",
    "apply_as_deletes": "Op = 'D'",
    "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
  }
}
```

:::tip
When using multiple `sequence_by` columns, add a tiebreaker column if events can share the same timestamp.
:::

## Full example: customers CDC with SCD Type 2

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "customers_group",
    "source_format": "cloudFiles",
    "source_details": {
      "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers_cdc.ddl",
      "source_path_dev": "s3://my-bucket/cdc/customers/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "customers_cdc",
    "bronze_reader_options": {
      "cloudFiles.format": "json",
      "cloudFiles.inferColumnTypes": "true"
    },
    "bronze_cdc_apply_changes": {
      "keys": ["customer_id"],
      "sequence_by": "dmsTimestamp",
      "scd_type": "1",
      "apply_as_deletes": "Op = 'D'",
      "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
    },
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "customers",
    "silver_transformation_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations.json",
    "silver_cdc_apply_changes": {
      "keys": ["customer_id"],
      "sequence_by": "dmsTimestamp",
      "scd_type": "2",
      "apply_as_deletes": "Op = 'D'",
      "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
    }
  }
]
```

## Multi-source CDC variant

When CDC events arrive from multiple source paths, use `bronze_append_flows` to write all sources to the same bronze table, then configure a single `silver_cdc_apply_changes` to merge into the silver target. See [Multi-Source CDC](./multi-source-cdc).

## Related

- [`create_auto_cdc_flow` API reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
- [Multi-Source CDC](./multi-source-cdc)
- [Onboarding File Fields](../reference/onboarding-fields)
