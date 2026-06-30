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
| `expect_or_drop` | Drop the failing row silently | Bad rows should not reach the main table and do not need to be inspected |
| `expect_or_quarantine` | Route the failing row to a quarantine table | Bad rows should be preserved for investigation rather than silently dropped |
| `expect_or_fail` | Halt the entire pipeline update | A violated rule indicates a critical upstream data problem |

:::tip
Prefer `expect_or_quarantine` over `expect_or_drop` when you want to inspect failed rows later. The quarantine table has the same schema as the target table plus an `_error` column.
:::

:::warning
`expect_or_fail` stops all pipeline processing for the current update. Use it only for genuine data contract breaches where continuing with bad data would cause irreversible harm.
:::

## JSON schema

```json
{
  "expect": {
    "valid_order_amount": "order_amount > 0",
    "valid_status": "status IN ('active', 'pending', 'closed')"
  },
  "expect_or_drop": {
    "transaction_id_not_null": "transaction_id IS NOT NULL"
  },
  "expect_or_quarantine": {
    "customer_id_not_null": "customer_id IS NOT NULL"
  },
  "expect_or_fail": {
    "date_not_null": "order_date IS NOT NULL"
  }
}
```

Each key within a constraint block is the **rule name** (a unique identifier shown in pipeline metrics). The value is the SQL boolean expression evaluated per row. Do not swap them.

## YAML equivalent

```yaml
expect:
  valid_order_amount: "order_amount > 0"
  valid_status: "status IN ('active', 'pending', 'closed')"

expect_or_drop:
  transaction_id_not_null: "transaction_id IS NOT NULL"

expect_or_quarantine:
  customer_id_not_null: "customer_id IS NOT NULL"

expect_or_fail:
  date_not_null: "order_date IS NOT NULL"
```

## Referencing the rules file

Reference the DQE file from the onboarding entry using the env-suffixed field name. Replace `prod` with your actual environment tag (`dev`, `stag`, etc.):

```json
{
  "bronze_data_quality_expectations_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders.json"
}
```

For silver:

```json
{
  "silver_data_quality_expectations_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders_silver.json"
}
```

## Quarantine behavior

When `expect_or_drop` rules are configured and a quarantine table is defined (`bronze_quarantine_table`, `bronze_database_quarantine_{env}`), rows that fail are written to the quarantine table rather than discarded. The quarantine table has the same schema as the main bronze table plus a `_error` column.

:::tip
Use the quarantine table to inspect and reprocess failed rows.
:::

## Example files in the repository

- JSON examples: [`demo/conf/json/dqe/`](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/json/dqe)
- YAML examples: [`demo/conf/yml/dqe/`](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/yml/dqe)
