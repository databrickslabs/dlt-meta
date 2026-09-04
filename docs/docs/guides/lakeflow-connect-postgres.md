---
id: lakeflow-connect-postgres
title: Lakeflow Connect for PostgreSQL
---

# Lakeflow Connect for PostgreSQL

SDP-META models Lakeflow Connect as a deploy-time ingestion layer. It creates
and reconciles the managed gateway and ingestion pipelines; it does not add a
PostgreSQL reader to the Spark Declarative Pipeline runtime.

```text
PostgreSQL -> UC connection -> LFC gateway -> LFC ingestion -> UC landing tables
                                                                    |
                                                           SDP-META Silver
```

The landing tables are the Bronze layer by default. A Silver flow can reference
one with `ingestion_ref`, avoiding an unnecessary second Bronze copy.

## Prerequisites

- Network connectivity from Databricks to PostgreSQL.
- A database user with the Lakeflow Connect grants.
- `wal_level=logical`.
- A publication and logical replication slot created on PostgreSQL.
- Unity Catalog privileges to create/use connections and destination schemas.

SDP-META does not create or delete publications or replication slots. Remove
unused slots explicitly to prevent retained WAL from growing indefinitely.

## Author ingestion metadata

One `ingestion` block represents one source database and can contain many
tables:

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
    tables:
      - name: customers
        scd: 1
      - name: payments
        scd: 2
```

Credential sourcing and rotation remain outside SDP-META. Reference a
pre-existing UC connection, or use the `lfc-connection` command with an existing
Databricks secret scope. `manage_connection` records intent in onboarding
metadata; `deploy --layer=ingestion` does not create or update the connection.
Run `lfc-connection` explicitly. When a managed connection already exists, the
command refuses reuse if its visible type, host, port, or database differs;
update or recreate that connection explicitly.

## Advanced pipeline configuration

Gateway and ingestion pipeline configuration are independent optional
pass-through maps. Values must be strings:

```yaml
    gateway:
      pipeline_configuration:
        pipelines.cdc.snapshot.qbc.maxConnections: "70"
        pipelines.gateway.mapUuidToString: "true"
      compute:
        node_type_id: r8i.2xlarge
        autoscale:
          min_workers: 1
          max_workers: 10
          mode: ENHANCED
        apply_policy_default_values: true
    ingestion_pipeline:
      pipeline_configuration: {}
```

Use these settings only when required by the selected Lakeflow Connect runtime.
Custom DBR versions and private-preview flags are not portable defaults.
Credentials and structural pipeline fields are rejected from configuration
maps.

## Use a landing table in Silver

```yaml
- data_flow_id: "301"
  data_flow_group: postgres_orders
  ingestion_ref:
    data_flow_id: "300"
    table: customers
  silver_database_prod: main.orders_silver
  silver_table: customers
  silver_reader_options:
    readChangeFeed: "true"
  silver_cdc_apply_changes:
    keys: [customer_id]
    sequence_by: _commit_version
    scd_type: "1"
```

Onboarding resolves `ingestion_ref` into the existing Silver source-details
shape. The Silver pipeline continues to use the standard Delta reader, DQE,
transformations, quarantine, and AUTO CDC behavior.

## Safe deployment lifecycle

1. Populate the Databricks secret scope through your existing secret-management
   process.
2. Run `lfc-connection` when SDP-META should create the UC connection, or
   reference a pre-existing connection.
3. Run `onboard` to write `ingestion_dataflowspec` and resolve references.
4. Preview deployment:

   ```bash
   databricks labs sdp-meta deploy \
     --layer=ingestion \
     --ingestion-dataflowspec-table=<catalog.schema.ingestion_dataflowspec> \
     --warehouse-id=<id> \
     --dry-run=true
   ```
5. Apply the reviewed deployment plan.
6. Deploy the existing Silver layer.

Reconciliation creates or updates pipelines by state-backed stable identity.
Missing metadata does not delete a live pipeline unless `--prune=true` is
explicitly used, and pruning is limited to pipeline IDs recorded in the
ownership table. Use `deploy: false` for metadata-only records. Same-name
pipelines without ownership state are reported as drift and are never adopted.
SDK schedule-job reconciliation is not currently performed.
