---
id: data-quality
title: Data Quality
sidebar_position: 4
---

# Data Quality

SDP-META supports Lakeflow Spark Declarative Pipelines' native Data Quality expectations. Define expectations in your onboarding YAML or JSON file; SDP-META registers and applies them at pipeline startup.

## Actions

| Action | Behavior |
|---|---|
| `warn` (`expect`) | Row passes through; a warning metric is recorded in the pipeline event log. |
| `drop` (`expect_or_drop` / `expect_or_quarantine`) | Row is excluded from the target table. With `expect_or_quarantine`, the row is written to a quarantine table. |
| `fail` (`expect_or_fail`) | The entire pipeline update is halted. |

## Defining expectations

Expectations are defined in `bronze_data_quality_expectations_json` or `silver_data_quality_expectations_json` in the onboarding file:

```json
{
  "expect_or_quarantine": {
    "customer_id IS NOT NULL": "customer_id must not be null",
    "order_amount > 0": "order amount must be positive"
  },
  "expect": {
    "region IS NOT NULL": "region should be populated"
  },
  "expect_or_fail": {
    "event_date IS NOT NULL": "event_date is required and must not be null"
  }
}
```

In YAML onboarding files, use `bronze_data_quality_expectations_json_path` to reference an external JSON file.

## Quarantine table

When any expectation uses `expect_or_quarantine`, SDP-META creates `<target_table>_quarantine` automatically. The quarantine table has the same schema as the target table plus an `_error` column recording which expectation failed. Liquid clustering is supported.

:::tip
Use `expect_or_quarantine` rather than `expect_or_drop` when you need to investigate failed rows. Quarantined data is preserved and queryable.
:::

## Onboarding file fields

| Field | Layer | Description |
|---|---|---|
| `bronze_data_quality_expectations_json` | Bronze | Inline JSON string with expectations |
| `bronze_data_quality_expectations_json_path` | Bronze | Path to a JSON file containing expectations |
| `silver_data_quality_expectations_json` | Silver | Inline JSON string with expectations |
| `silver_data_quality_expectations_json_path` | Silver | Path to a JSON file containing expectations |

:::note
In DAB bundles, the file-path pattern is preferred — expectation files can be reviewed and diff'd in git alongside the onboarding file.
:::

## Monitoring

Lakeflow Spark Declarative Pipelines records expectation metrics in the pipeline event log. Query from the pipeline UI or via SQL on the `event_log` system table.

For the complete expectation schema, see [DQ Rules Reference](../reference/dq-rules.md).
