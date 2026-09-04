---
id: dataflowspec
title: Dataflowspec
sidebar_position: 2
---

# Dataflowspec

The onboarding file (YAML or JSON) is the configuration you write to define
your pipelines. The Onboard Job writes Spark pipeline metadata into
`bronze_dataflowspec` and `silver_dataflowspec`. Lakeflow Connect blocks are
written separately to `ingestion_dataflowspec`.

Each entry in the onboarding file represents one flow. A flow has a shared identity section, a Bronze section, and an optional Silver section.

## Architecture

SDP-META operates in three phases:

1. **Onboard Job** — reads one onboarding file and writes ingestion, Bronze,
   and Silver metadata to their respective Delta spec tables.
2. **Ingestion deployment** — reads `ingestion_dataflowspec` at deploy time and
   reconciles managed Lakeflow Connect pipelines.
3. **Generic Declarative Pipeline** — reads Bronze/Silver specs at runtime and
   dynamically constructs the Spark pipeline graph.

## Ingestion dataflowspec

Lakeflow Connect metadata has two representations:

1. Authors add an `ingestion` block to the onboarding YAML.
2. The Onboard Job resolves defaults and environment suffixes, expands table
   mappings, and writes one normalized row to `ingestion_dataflowspec`.

The persisted row uses this schema:

| Field | Type | Description |
|---|---|---|
| `dataFlowId` | string | Stable ID copied from `data_flow_id`. |
| `dataFlowGroup` | string | Deployment group copied from `data_flow_group`. |
| `sourceType` | string | Managed connector type, initially `POSTGRESQL`. |
| `connectionName` | string | Unity Catalog connection referenced by the gateway. |
| `connectionSpec` | JSON string | Optional connection-creation metadata; `{}` when reusing an existing connection. |
| `manageConnection` | boolean | Records connection-management intent. Pipeline reconciliation does not create connections; run `lfc-connection` separately. |
| `gatewayDetails` | map | Gateway name, storage catalog/schema, channel, and continuous mode. |
| `sourceConfigurations` | JSON string | Connector-specific settings such as PostgreSQL publication and replication slot. |
| `objects` | JSON string | Expanded source-to-destination table mappings and SCD configuration. |
| `targetDetails` | map | Ingestion pipeline name, destination catalog/schema, and channel. |
| `schedule` | JSON string | Optional Quartz schedule for triggered ingestion. |
| `deploy` | boolean | `false` registers metadata without creating duplicate pipelines. |
| `gatewayPipelineConfiguration` | JSON string | String-valued gateway pipeline configuration. |
| `ingestionPipelineConfiguration` | JSON string | String-valued ingestion pipeline configuration. |
| `gatewayCompute` | JSON string | Optional gateway cluster and DBR overrides. |
| `version` | string | Metadata version used for latest-version selection. |
| `createDate`, `updateDate` | timestamp | Audit timestamps. |
| `createdBy`, `updatedBy` | string | Audit principals. |

Complex payloads are stored as JSON strings so connector-specific API fields
can evolve without changing the Delta table schema.

### Ingestion onboarding example

```yaml
- data_flow_id: "300"
  data_flow_group: postgres_orders
  ingestion:
    source:
      type: POSTGRESQL
      catalog: ordersdb
      schema: public
      connection: orders_connection
      slot:
        publication: databricks_publication
        slot: databricks_slot
    target:
      catalog_prod: main
      schema: orders
    gateway:
      storage_catalog_prod: main
      storage_schema: lfc_staging
      pipeline_configuration:
        pipelines.cdc.snapshot.qbc.maxConnections: "70"
        pipelines.gateway.mapUuidToString: "true"
      compute:
        node_type_id: r8i.2xlarge
        autoscale:
          min_workers: 1
          max_workers: 10
          mode: ENHANCED
    schedule:
      quartz_cron_expression: "0 0/15 * * * ?"
      timezone_id: UTC
    tables:
      - name: customers
        scd: 1
      - name: payments
        scd: 2
```

The normalized `ingestion_dataflowspec` row contains values such as:

```yaml
dataFlowId: "300"
dataFlowGroup: postgres_orders
sourceType: POSTGRESQL
connectionName: orders_connection
connectionSpec: '{}'
manageConnection: false
gatewayDetails:
  name: postgres_orders_gateway
  storageCatalog: main
  storageSchema: lfc_staging
  continuous: "true"
  channel: CURRENT
sourceConfigurations: >-
  [{"catalog":{"source_catalog":"ordersdb","postgres":{"slot_config":
  {"publication_name":"databricks_publication","slot_name":"databricks_slot"}}}}]
objects: >-
  [{"table":{"source_catalog":"ordersdb","source_schema":"public",
  "source_table":"customers","destination_catalog":"main",
  "destination_schema":"orders","destination_table":"customers",
  "table_configuration":{"scd_type":"SCD_TYPE_1"}}}]
targetDetails:
  catalog: main
  schema: orders
  name: postgres_orders_ingestion
  channel: CURRENT
schedule: '{"quartz_cron_expression":"0 0/15 * * * ?","timezone_id":"UTC"}'
deploy: true
gatewayPipelineConfiguration: >-
  {"pipelines.cdc.snapshot.qbc.maxConnections":"70",
  "pipelines.gateway.mapUuidToString":"true"}
ingestionPipelineConfiguration: '{}'
gatewayCompute: >-
  {"node_type_id":"r8i.2xlarge","autoscale":{"min_workers":1,
  "max_workers":10,"mode":"ENHANCED"}}
version: "1"
```

This persisted form is generated and consumed by SDP-META. Do not author or
edit it directly. See
[Managed Ingestion Fields](../reference/ingestion-fields.md) for the complete
onboarding contract.

## Common fields

These fields are required on every standard Bronze/Silver flow entry.
Ingestion rows use an `ingestion` block instead; see
[Managed Ingestion Fields](../reference/ingestion-fields.md).

| Field | Type | Required | Description |
|---|---|---|---|
| `data_flow_id` | string | Yes | Unique identifier for the flow within the file. |
| `data_flow_group` | string | Yes | Group name. A pipeline processes only flows matching its configured group. |
| `source_format` | string | Yes | Source format: `cloudFiles`, `delta`, `eventhub`, `kafka`, or `snapshot`. |
| `source_details` | map | Yes | Source-specific connection properties. Keys vary by `source_format` (e.g. `source_path_<env>`, `source_schema_path`). |
| `source_system` | string | No | Informational label for the source system. |

## Bronze fields

| Field | Type | Required | Description |
|---|---|---|---|
| `bronze_catalog_<env>` | string | UC only | Unity Catalog catalog name for the Bronze target. |
| `bronze_database_<env>` | string | Yes | Schema for the Bronze target table. |
| `bronze_table` | string | Yes | Bronze table name. |
| `bronze_table_comment` | string | No | Table comment. |
| `bronze_table_path_<env>` | string | Non-UC | External path for the Bronze table (required without Unity Catalog). |
| `bronze_reader_options` | map | No | Spark reader options (e.g. `cloudFiles.format`, `header`). |
| `bronze_table_properties` | map | No | Delta table properties. |
| `bronze_partition_columns` | string | No | Comma-separated partition column names. |
| `bronze_cluster_by` | list | No | Liquid clustering columns. |
| `bronze_cluster_by_auto` | boolean | No | Enable auto liquid clustering. |
| `bronze_data_quality_expectations_json_<env>` | string | No | Path to a DQE file (JSON or YAML). |
| `bronze_catalog_quarantine_<env>` | string | No | Catalog for the quarantine table (defaults to bronze catalog). |
| `bronze_database_quarantine_<env>` | string | No | Schema for the quarantine table. Required when DQE has `drop` expectations. |
| `bronze_quarantine_table` | string | No | Quarantine table name. |
| `bronze_quarantine_table_comment` | string | No | Quarantine table comment. |
| `bronze_quarantine_table_path_<env>` | string | No | External path for the quarantine table (non-UC). |
| `bronze_quarantine_table_cluster_by` | list | No | Liquid clustering columns for the quarantine table. |
| `bronze_quarantine_table_cluster_by_auto` | boolean | No | Auto liquid clustering for the quarantine table. |
| `bronze_cdc_apply_changes` | map | No | Single-source CDC config. See [CDC](../guides/cdc.md). |
| `bronze_apply_changes_from_snapshot` | map | No | Snapshot-based CDC config. See [Snapshot CDC](../guides/snapshot.md). |
| `bronze_append_flows` | list | No | Additional append flows. See [Autoloader](../guides/autoloader.md). |
| `bronze_sinks` | list | No | Sink configs (Delta, Kafka, Event Hubs). See [DLT Sink](../guides/dlt-sink.md). |
| `bronze_row_filter` | string | No | `ROW FILTER` clause for the Bronze table (Unity Catalog only). |

## Silver fields

| Field | Type | Required | Description |
|---|---|---|---|
| `silver_catalog_<env>` | string | UC only | Unity Catalog catalog name for the Silver target. |
| `silver_database_<env>` | string | Yes | Schema for the Silver target table. |
| `silver_table` | string | Yes | Silver table name. |
| `silver_table_comment` | string | No | Table comment. |
| `silver_table_path_<env>` | string | Non-UC | External path for the Silver table (required without Unity Catalog). |
| `silver_transformation_json_<env>` | string | Yes* | Path to a transformations file defining `select_exp` and `where_clause`. *Not required when using `silver_cdc_apply_changes_flows`. |
| `silver_data_quality_expectations_json_<env>` | string | No | Path to a DQE file. |
| `silver_cdc_apply_changes` | map | No | Single-source CDC config. See [CDC](../guides/cdc.md). |
| `silver_cdc_apply_changes_flows` | map | No | Multi-source CDC flow group. See [Multi-source CDC](../guides/multi-source-cdc.md). |
| `silver_reader_options` | map | No | Additional Spark reader options for the Silver source. |
| `silver_cluster_by` | list | No | Liquid clustering columns. |
| `silver_cluster_by_auto` | boolean | No | Enable auto liquid clustering. |
| `silver_row_filter` | string | No | `ROW FILTER` clause for the Silver table (Unity Catalog only). |

## Environment suffixes

Fields ending in `_<env>` accept environment-specific values. The Onboard Job is called with an `env` parameter (typically `dev` or `prod`), and it reads the matching field — for example `bronze_database_prod` when `env=prod`. This lets one onboarding file serve multiple environments.

## Example

```yaml
- data_flow_id: '1'
  data_flow_group: retail
  source_format: cloudFiles
  source_details:
    source_path_prod: /Volumes/my_catalog/my_schema/landing/customers
    source_schema_path: /Volumes/my_catalog/my_schema/ddl/customers.ddl
  bronze_catalog_prod: my_catalog
  bronze_database_prod: retail_bronze
  bronze_table: customers
  bronze_reader_options:
    cloudFiles.format: csv
    header: 'true'
  bronze_cluster_by_auto: true
  bronze_data_quality_expectations_json_prod: /Volumes/my_catalog/my_schema/dqe/customers.yml
  bronze_database_quarantine_prod: retail_bronze
  bronze_quarantine_table: customers_quarantine
  silver_catalog_prod: my_catalog
  silver_database_prod: retail_silver
  silver_table: customers
  silver_cdc_apply_changes:
    keys: [customer_id]
    sequence_by: updated_at
    scd_type: '1'
  silver_transformation_json_prod: /Volumes/my_catalog/my_schema/transformations/silver.yml
```

## Source of truth

The `ingestion_dataflowspec`, `bronze_dataflowspec`, and
`silver_dataflowspec` Delta tables are derived artifacts. Always edit the
onboarding file and re-run onboarding; do not edit spec rows directly.
