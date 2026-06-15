---
id: pipeline-chaining
title: Pipeline Chaining
sidebar_position: 3
---

# Pipeline Chaining

Pipeline chaining connects a Bronze pipeline (which writes Delta tables) to a Silver pipeline (which reads those tables as its source). SDP-META supports this through the `layer=bronze_silver` configuration and two deployment modes.

## Split vs combined

When you set `layer=bronze_silver`, the `pipeline_mode` setting controls deployment:

### Split (default)

Two separate Lakeflow Spark Declarative Pipelines: one for Bronze, one for Silver.

- Each pipeline has independent update history, rollback, and cluster configuration.
- Silver waits for Bronze to finish (enforced by the workflow job in DAB deployments).
- Recommended when layers have different SLAs or need independent lifecycle management.

### Combined

One pipeline with `configuration.layer = bronze_silver` that materializes both layers in a single DAG.

- Lower overhead — one pipeline to schedule and monitor.
- Lakeflow Spark Declarative Pipelines manages intra-pipeline ordering automatically.
- Best for development environments or tightly coupled Bronze/Silver workloads.

## Configuring data_flow_group for chaining

Both Bronze and Silver flows must share the same `data_flow_group`. In the onboarding file:

```yaml
# Bronze flow — group G1
- data_flow_id: 1
  data_flow_group: G1
  source_format: cloudFiles
  target_table: orders_raw
  ...

# Silver flow — also group G1, reads from orders_raw
- data_flow_id: 101
  data_flow_group: G1
  source_format: delta
  source_details:
    source_database: bronze_schema
    source_table: orders_raw
  target_table: orders_clean
  ...
```

The pipeline is configured with `bronze.group = G1` and `silver.group = G1`.

:::warning
If the `data_flow_group` in Silver onboarding entries does not match the group name on the Silver pipeline, those flows will not be processed. `bundle-validate` checks for this mismatch.
:::

## Silver fan-out

One Bronze table can feed multiple Silver tables. Define multiple Silver onboarding entries with the same `source_table` but different `target_table` values:

```yaml
- data_flow_id: 101
  data_flow_group: G1
  source_details:
    source_table: orders_raw
  target_table: orders_by_region
  silver_transformation_sql: "SELECT region, COUNT(*) as cnt FROM orders_raw GROUP BY region"

- data_flow_id: 102
  data_flow_group: G1
  source_details:
    source_table: orders_raw
  target_table: orders_by_customer
  silver_transformation_sql: "SELECT customer_id, SUM(amount) as total FROM orders_raw GROUP BY customer_id"
```

## Switching between modes

Change `pipeline_mode` in `resources/variables.yml`, redeploy the bundle, and re-run the onboarding job. The onboarding file does not change.
