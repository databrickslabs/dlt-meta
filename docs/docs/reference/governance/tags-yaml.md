---
id: tags-yaml
title: tags.yml Reference
---

# `tags.yml` reference

Each file is scoped to one Unity Catalog and may contain multiple schemas.

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
      access_tier: restricted
    columns:
      email:
        sensitivity: pii
        semantic_type: email

  retail_silver.customers:
    columns:
      email:
        sensitivity: pii
```

## Root fields

- `version`: configuration schema version; currently `"1"`.
- `source_id`: required stable configuration owner ID unless `--source-id` is
  supplied. Use a different value for each independently managed file.
- `defaults.catalog`: required unless supplied by `--catalog`.
- `defaults.schema`: required for one-part table names unless supplied by
  `--schema`.
- `tables`: map of target names to table and column assignments.

## Target resolution

- `customers` resolves to
  `<defaults.catalog>.<defaults.schema>.customers`.
- `retail_silver.customers` resolves to
  `<defaults.catalog>.retail_silver.customers`.
- `main_prod.retail_silver.customers` is accepted only when its catalog equals
  the effective catalog.

## Assignment nodes

- `table` maps tag keys to string values on the table.
- `columns` maps column names to tag-key/string-value maps.
- An empty target remains in scope and requests cleanup of stale
  SDP-META-owned assignments.
- A target omitted from the file is outside the run scope.

Contributor ownership includes both `source_id` and the resolved target, so
overlapping files cannot remove one another's assignments.

Tag keys and allowed values must already exist when governed tags are used.
