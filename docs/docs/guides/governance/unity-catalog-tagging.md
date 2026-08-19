---
id: unity-catalog-tagging
title: Unity Catalog Tagging
---

# Tagging tool guide

The installable `databricks.labs.sdp_meta.governance.tagging` package provides
reconciliation for any Unity Catalog table:

- `apply-tags`, which validates, plans, applies, and verifies table and column
  tag assignments.

The generator module discovers SDP-META physical targets without coupling the
reconciliation planner to DataflowSpec or onboarding metadata.

`tags.yml` is the only desired-state source for assignments.

## Install

```bash
python -m pip install databricks-labs-sdp-meta
```

## Generate a catalog skeleton

```bash
databricks labs sdp-meta generate-tags \
  --input ./onboarding.yml \
  --output ./tags.yml \
  --environment prod \
  --catalog main_prod \
  --schema retail_bronze \
  --source-id retail-production-tags
```

The generator accepts JSON or YAML. It reads bronze, silver, and quarantine
target metadata but never reads tag attributes or infers classifications.
Run it once per catalog when onboarding contains targets in multiple catalogs.
It refuses to replace an existing output file unless `--overwrite` is supplied
explicitly; generate to a staging path when updating a curated file.

## Author assignments

```yaml
version: "1"
source_id: retail-production-tags
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
```

One-part table names use both defaults. Two-part names select another schema
in the same catalog. Three-part names are accepted only when their catalog
matches the effective catalog.

## Plan and apply

Create a non-mutating plan:

```bash
databricks labs sdp-meta apply-tags \
  --tags-file ./tags.yml \
  --state-table main_prod.sdp_meta.uc_governance_tag_assignments \
  --warehouse-id "$DATABRICKS_WAREHOUSE_ID" \
  --dry-run
```

After review, remove `--dry-run`. With an active Spark session,
`--warehouse-id` is optional.

The command validates all targets and columns before mutation, reads existing
assignments and ownership state, generates deterministic DDL, persists pending
ownership before DDL, verifies assignments by reading them back, and records
the final state.

## Apply from Python

Notebook and application code should call the public `apply_tags()` API rather
than the CLI `main()` entry point:

```python
from databricks.labs.sdp_meta.governance.tagging import apply_tags

status = apply_tags(
    tags_file="/Volumes/main_prod/sdp_meta/governance/tags.yml",
    state_table="main_prod.sdp_meta.uc_governance_tag_assignments",
    dry_run=True,
)
if status != 0:
    raise RuntimeError(f"Tag reconciliation returned status {status}")
```

With an active Spark session, no warehouse ID is required. Outside Spark, pass
`warehouse_id="..."`. Remove `dry_run=True` after reviewing the plan.
Configuration and preflight errors are raised to the Python caller; statuses
`2` and `3` retain the conflict and handled execution-failure meanings
documented below. The CLI entry point maps user-correctable validation errors
to status `1`.

## Interactive demo

The
[Unity Catalog tagging interactive demo](https://github.com/databrickslabs/dlt-meta/tree/main/demo/governance-tagging)
creates four ordinary UC tables, generates `tags.yml` in a managed UC Volume,
applies it through the public Python API, and retains CLI examples for terminal
automation.

## DAB execution ordering

The generated `governance_tagging` resource is an optional standalone job. Its
`apply_tags` task depends on `stage_conf`, but the job does not depend on the
generated `pipelines` job. Run it only after the relevant pipeline run succeeds,
either manually or through external orchestration.

## Newly created tables and restrictive ABAC

There is an interval between a pipeline creating a table and the standalone
governance job applying its object-specific tags. If access must be restrictive
from the moment of creation, use a restrictive baseline tag on the target
schema and design the ABAC policy to honor that inherited baseline. The
governance job can then apply the reviewed table and column tags after the
pipeline succeeds. Do not rely on operator sequencing alone as the security
boundary for newly created tables.

## Ownership rules

- Missing desired assignment: set it and record SDP-META ownership.
- Matching pre-existing assignment: preserve and record external ownership.
- Conflicting external assignment: fail unless reviewed ownership transfer is
  requested.
- Changed SDP-META-owned assignment: update it.
- Stale unchanged SDP-META-owned assignment: remove it.
- Externally modified SDP-META-owned assignment: preserve and report conflict.
- Stale externally owned assignment: forget the observation without removing
  the Unity Catalog tag.

An empty target remains in scope and requests cleanup of SDP-META-owned
assignments. Removing the target from the file places it outside the run scope.

Use `--transfer-ownership` only after reviewing the dry-run conflicts.

## Exit codes

- `0`: successful apply or clean dry-run;
- `1`: invalid configuration or failed preflight;
- `2`: ownership conflict;
- `3`: handled DDL or post-application verification failure.

Unexpected backend or state-persistence exceptions fail the process directly
and are not currently normalized to exit code `3`.

## Required permissions

The runtime identity needs `USE CATALOG`, `USE SCHEMA`, `APPLY TAG`, `ASSIGN`
for governed tags, information-schema read access, and read/write access to the
assignment-state table. It must not receive governed-tag `MANAGE`.
