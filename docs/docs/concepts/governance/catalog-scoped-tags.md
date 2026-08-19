---
id: catalog-scoped-tags
title: Catalog-scoped Tags
---

# Catalog-scoped tags

Maintain one `tags.yml` per Unity Catalog per environment.

A file:

- declares exactly one effective catalog;
- may contain tables from multiple schemas in that catalog;
- must not contain tables from another catalog;
- is applied by an independent task or job invocation;
- uses a stable source ID for assignment ownership.

Tags remain outside data-pipeline metadata. Optional integrations may use
external metadata only to discover resolved physical targets when generating a
starter file.

## Why catalog scope

Catalog-scoped files provide:

- a clear Unity Catalog permission boundary;
- independent deployment and failure isolation;
- simpler target resolution;
- smaller reconciliation scope;
- easier ownership and audit reporting;
- no requirement for one service principal to modify every catalog;
- safer regeneration and promotion across environments.

Combining catalogs in one file couples unrelated permissions and makes a partial failure affect multiple governance domains.

## YAML contract

```yaml
version: "1"

source_id: retail-main-tags

defaults:
  catalog: main_prod
  schema: retail_bronze

tables:
  customers:
    table:
      data_domain: customer
    columns:
      email:
        sensitivity: pii

  retail_silver.customers:
    columns:
      email:
        sensitivity: pii

  retail_quarantine.customers:
    table:
      access_tier: restricted
```

### `source_id`

`source_id` is a stable identifier for the configuration owner, independent of the local filename or deployment path.

It participates in assignment contributor identity:

```text
("target", source_id, "catalog.schema.table")
```

The assignment key separately contains the resolved table or column and tag
key. This prevents two files that reference the same table from silently
removing each other's assignments.

### `defaults.catalog`

`defaults.catalog` identifies the file's catalog. It is required unless supplied by a deployment-time `--catalog` override.

Every resolved table must match the effective catalog. Cross-catalog entries fail validation before any metadata read or DDL.

### `defaults.schema`

`defaults.schema` is optional. It supplies the schema for one-part table names.

Use schema-qualified names for other schemas in the same catalog.

## Name resolution

Given:

```yaml
defaults:
  catalog: main_prod
  schema: retail1
```

Resolution is:

```text
customers
  -> main_prod.retail1.customers

retail2.orders
  -> main_prod.retail2.orders

main_prod.shared.reference_codes
  -> main_prod.shared.reference_codes
```

An explicit three-part name is accepted only if its catalog is `main_prod`.

```text
other_catalog.finance.transactions
  -> validation failure
```

CLI `--catalog` and `--schema` may override YAML defaults for environment promotion. Explicit three-part names must still match the effective catalog after overrides.

## Multiple schemas

Tables from multiple schemas in one catalog belong in the same file when they share a governance owner:

```yaml
defaults:
  catalog: catalog1
  schema: retail1

tables:
  customers: {}
  transactions: {}
  retail2.orders: {}
  retail2.products: {}
```

Resolved targets:

```text
catalog1.retail1.customers
catalog1.retail1.transactions
catalog1.retail2.orders
catalog1.retail2.products
```

An empty table node has active reconciliation meaning: it requests removal of stale assignments previously owned by this `source_id`. Generators must not emit active empty nodes as harmless examples.

## Multiple catalogs

Use separate files:

```text
conf/tags/dev/catalog1.tags.yml
conf/tags/dev/catalog2.tags.yml
conf/tags/prod/catalog1.tags.yml
conf/tags/prod/catalog2.tags.yml
```

Example:

`catalog1.tags.yml`:

```yaml
version: "1"
source_id: catalog1-retail-tags
defaults:
  catalog: catalog1
  schema: retail
tables:
  customers:
    table:
      data_domain: customer
  transactions:
    table:
      data_domain: finance
```

`catalog2.tags.yml`:

```yaml
version: "1"
source_id: catalog2-retail-tags
defaults:
  catalog: catalog2
  schema: retail
tables:
  stores:
    table:
      data_domain: store
  products:
    table:
      data_domain: product
```

## Generation from SDP-META

`generate-tags` reads target fields from an environment-resolved SDP-META
onboarding YAML or JSON file. The core reconciliation planner has no
DataflowSpec or onboarding dependency.

Run it once per catalog:

```bash
databricks labs sdp-meta generate-tags \
  --input onboarding.prod.yml \
  --environment prod \
  --catalog catalog1 \
  --output catalog1.tags.generated.yml
```

Generation creates commented target examples and never infers classifications
or sensitive columns. Generate to a staging path, add desired tags, and merge
the reviewed result into the curated catalog file.

## Execution model

Run one apply task per catalog:

```text
pipeline update
      |
      +--> apply catalog1 tags
      |
      +--> apply catalog2 tags
```

Each task:

- receives one catalog-scoped YAML;
- uses a catalog-appropriate service principal;
- writes assignment status keyed by `source_id`;
- fails independently;
- prevents workflow success when required governance application fails.

Example DAB tasks. This is the opt-in *chained* variant, where tagging runs in
the same job as the pipeline; the generated `governance_tagging` resource is
instead a standalone job whose ordering is operator-sequenced. Named-parameter
keys must use hyphens — they are passed to the entry point as `--<key>=<value>`:

```yaml
- task_key: apply_catalog1_tags
  depends_on:
    - task_key: pipeline_update
  python_wheel_task:
    package_name: databricks_labs_sdp_meta
    entry_point: apply_tags
    named_parameters:
      tags-file: /Volumes/catalog1/governance/config/catalog1.tags.yml
      state-table: catalog1.sdp_meta.uc_governance_tag_assignments

- task_key: apply_catalog2_tags
  depends_on:
    - task_key: pipeline_update
  python_wheel_task:
    package_name: databricks_labs_sdp_meta
    entry_point: apply_tags
    named_parameters:
      tags-file: /Volumes/catalog2/governance/config/catalog2.tags.yml
      state-table: catalog2.sdp_meta.uc_governance_tag_assignments
```

## State model

The assignment-state table uses non-keyword column names:

```text
catalog_name
schema_name
table_name
column_name
tag_key
last_applied_value
ownership
contributors
status
error_message
first_observed_at
last_reconciled_at
```

Source IDs are recorded inside the `contributors` JSON column as
`("target", source_id, "catalog.schema.table")` entries; desired values live
only in the reviewed `tags.yml`, never in state.

`pending` ownership intent and verified ownership are distinct states. A pending assignment is reconciled against actual metadata and promoted only after successful verification.

## Pre-mutation checks

Before mutation:

1. validate `source_id`;
2. resolve every target to three parts;
3. verify all targets match the effective catalog;
4. validate table and column tag maps;
5. verify every configured table and column exists;
6. load state for all selected tables regardless of `source_id`, so
   assignments contributed by other sources are visible and protected;
7. detect cross-source assignment conflicts;
8. abort without DDL when conflicts exist.
