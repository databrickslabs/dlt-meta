---
id: index
title: Guides
sidebar_position: 1
---

# Guides

| Scenario | Guide |
|---|---|
| Ingest files landing in S3, ADLS, or GCS using Databricks Autoloader | [Autoloader / Cloud Files](./autoloader) |
| Replicate PostgreSQL tables with managed Lakeflow Connect CDC | [Lakeflow Connect for PostgreSQL](./lakeflow-connect-postgres) |
| Ingest from Kafka or Azure Event Hubs | [Kafka & Event Hubs](./kafka-eventhub) |
| CDC merge — Type 1 or Type 2 SCD from a change-data-capture source | [CDC with apply_changes](./cdc) |
| Full snapshot ingestion — replace-all semantics, not streaming | [Snapshot Ingestion](./snapshot) |
| Fan-out: one bronze table as source for multiple silver tables | [Silver Fanout](./silver-fanout) |
| Write pipeline output to an external Delta table or Kafka topic | [DLT Sink](./dlt-sink) |
| Multiple source paths (e.g. US, EU, APAC) → single target table | [Multi-Source CDC](./multi-source-cdc) |
| Filter rows at pipeline time by column value | [Row Filters](./row-filters) |
