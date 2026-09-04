---
id: ingestion-fields
title: Managed Ingestion Fields
---

# Managed ingestion onboarding fields

Lakeflow Connect ingestion uses an `ingestion` block instead of
`source_format`. The block is consumed at onboarding and deployment time; it is
never dispatched to `PipelineReaders`.

## Row fields

- `data_flow_id`: stable ingestion identifier.
- `data_flow_group`: deployment and scheduling group.
- `ingestion.source`: source type, database coordinates, connection, and
  optional managed-secret metadata.
- `ingestion.target`: environment-resolved landing catalog and schema.
- `ingestion.tables`: source tables and optional per-table overrides.
- `ingestion.gateway`: gateway storage, pipeline configuration, and compute
  overrides.
- `ingestion.ingestion_pipeline`: ingestion-pipeline configuration overrides.
- `ingestion.schedule`: optional Quartz schedule and timezone.
- `ingestion.deploy`: set to `false` for metadata-only registration.

## Source fields

- `type`: initially `POSTGRESQL`.
- `catalog`: PostgreSQL database name.
- `schema`: source schema; defaults to `public`.
- `connection`: existing UC connection name.
- `host_<env>`, `port`, and secret-scope key names: persisted as connection
  metadata when `manage_connection: true`. Ingestion deployment does not act on
  these fields automatically. Supply the corresponding values explicitly to
  `lfc-connection` before deployment. SDP-META does not integrate with an
  external secret manager.
- `slot.publication` and `slot.slot`: existing PostgreSQL CDC publication and
  replication slot.

## Target fields

- `catalog_<env>`: Unity Catalog destination catalog.
- `schema`: destination schema.

## Table entries

A table can be a name or a mapping:

```yaml
tables:
  - customers
  - name: payments
    schema: billing
    destination: payments_history
    scd: 2
```

Resolution order is table override, source default, then onboarding default.
Destination names must be unique within an ingestion.

## Pipeline configuration

`gateway.pipeline_configuration` and
`ingestion_pipeline.pipeline_configuration` accept arbitrary string-valued
runtime settings. SDP-META passes these values through without interpreting
them.

Structural fields (`name`, `gateway_definition`, `ingestion_definition`,
destination identity, and pipeline IDs) and credential-shaped keys are not
allowed in pass-through configuration.

`gateway.compute` optionally accepts:

- `dbr_version`
- `node_type_id`
- `autoscale.min_workers`
- `autoscale.max_workers`
- `autoscale.mode`
- `apply_policy_default_values`

## Silver and Bronze references

`ingestion_ref` selects a landing table:

```yaml
ingestion_ref:
  data_flow_id: "300"
  table: customers
```

It is mutually exclusive with explicit Bronze source fields. The referenced
ingestion and table must exist in the current onboarding file or in the latest
persisted ingestion specs.
