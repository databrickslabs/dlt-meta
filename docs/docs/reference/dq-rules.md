---
id: dq-rules
title: Data Quality Rules Schema
sidebar_position: 3
---

# Data Quality Rules Schema

Data quality rules are defined in a separate JSON or YAML file and referenced from the onboarding file via `bronze_data_quality_expectations_json` or `silver_data_quality_expectations_json`. Each rule is a named SQL boolean expression mapped directly to Declarative Pipeline constraint annotations.

## Constraint types

| Constraint | Pipeline action | Use when |
|---|---|---|
| `expect` | Log violation, keep the row | Track quality issues without dropping data |
| `expect_or_drop` | Drop the row (routes to quarantine if configured) | Bad rows should not reach the main table |
| `expect_or_fail` | Halt the entire pipeline update | A violated rule indicates a critical upstream data problem |

:::warning
`expect_or_fail` stops all pipeline processing for the current update. Use it only for genuine data contract breaches.
:::

## JSON schema

```json
{
  "expect": {
    "valid_order_amount": "order_amount > 0",
    "valid_status": "status IN ('active', 'pending', 'closed')"
  },
  "expect_or_drop": {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "transaction_id_not_null": "transaction_id IS NOT NULL"
  },
  "expect_or_fail": {
    "date_not_null": "order_date IS NOT NULL"
  }
}
```

## YAML equivalent

```yaml
expect:
  valid_order_amount: "order_amount > 0"
  valid_status: "status IN ('active', 'pending', 'closed')"

expect_or_drop:
  customer_id_not_null: "customer_id IS NOT NULL"
  transaction_id_not_null: "transaction_id IS NOT NULL"

expect_or_fail:
  date_not_null: "order_date IS NOT NULL"
```

## Referencing the rules file

```json
{
  "bronze_data_quality_expectations_json": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders.json"
}
```

For silver:

```json
{
  "silver_data_quality_expectations_json": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders_silver.json"
}
```

## Quarantine behavior

When `expect_or_drop` rules are configured and a quarantine table is defined (`bronze_quarantine_table`, `bronze_database_quarantine_{env}`), rows that fail are written to the quarantine table rather than discarded. The quarantine table has the same schema as the main bronze table plus a `_error` column.

:::tip
Use the quarantine table to inspect and reprocess failed rows.
:::

## Example files in the repository

- JSON examples: [`examples/json/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/examples/json/dqe)
- YAML examples: [`demo/conf/yml/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe)
