---
id: uc-naming
title: Unity Catalog Naming
sidebar_position: 6
---

# Unity Catalog Naming

SDP-META accepts raw Unity Catalog (UC) object names in configuration and
adds SQL delimiter quoting only where Spark SQL requires it. Use raw names
in new onboarding files, DAB variables, and CLI arguments.

This behavior follows Databricks' [UC names](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names)
and [SQL identifiers](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers)
documentation.

## Supported object names

The rules below apply to UC catalog, schema, table/view, and volume name
segments supplied to SDP-META.

| Input | Supported | Notes |
|---|---:|---|
| `manufacturing` | Yes | A regular SQL identifier. |
| `cdlp-gsc-glb-manufacturing-dev` | Yes | A hyphenated UC name. |
| `9lives` | Yes | Valid as a delimiter-quoted UC name. |
| `data.set` | No, as one segment | A period separates name segments. |
| `data set` | No | UC object names cannot contain spaces. |
| `data/set` | No | UC object names cannot contain `/`. |
| `` `data-set` `` | No | Supply the raw name, `data-set`; SDP-META adds backticks for SQL. |
| ASCII control characters or `DEL` | No | Prohibited by Unity Catalog. |

Each segment can contain at most 255 characters. Databricks stores UC object
names in lowercase.

## How names map to Spark SQL

Use raw names in SDP-META configuration:

```yaml
bronze_catalog_dev: cdlp-gsc-glb-manufacturing-dev
bronze_database_dev: cdlp-gsc-glb-manufacturing-dev.raw-zone
bronze_table: customer-orders
```

At a SQL or Spark table-reference boundary, SDP-META maps that to:

```sql
`cdlp-gsc-glb-manufacturing-dev`.`raw-zone`.`customer-orders`
```

SDK calls and UC Volume paths use the same raw values; they are not SQL
expressions and must not contain backticks:

```text
/Volumes/cdlp-gsc-glb-manufacturing-dev/raw-zone/files/
```

## Legacy delimited configuration

Raw names are the supported configuration format. For upgrade compatibility,
SDP-META also accepts historic SQL-delimited values such as
`` `my-catalog`.`raw-zone` `` and normalizes them to raw names before
validation and persistence. Do not add delimiters to new configuration, SDK
parameters, or `/Volumes/...` paths.

## Multipart names

Fields that accept a database or table reference may use dot-separated
segments, such as `catalog.schema.table`. Each segment must independently
meet the rules above. A dot is therefore never valid inside an individual
catalog, schema, table, or volume name.

## Column and expression fields

This support concerns UC object names. SDP-META keeps column names used in
pipeline expressions, partitioning, clustering, CDC keys, and `sequence_by`
as regular SQL identifiers. Do not use special characters in those fields
unless that specific pipeline API documents support for them.

The interactive demo supports these UC naming rules as well.
