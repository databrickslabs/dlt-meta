---
id: prerequisites
title: Governance Prerequisites
---

# Governed-tag prerequisites for `tags.yml`

**Reference:** [Governed tags](https://docs.databricks.com/aws/en/admin/governed-tags/)

## Purpose

`tags.yml` declares desired assignments. It does not create governed-tag definitions, allowed values, system tags, or permissions.

Before applying a governance-critical `tags.yml`, administrators must bootstrap the tag taxonomy and grant the tag-applier identity the required Unity Catalog and tag-policy permissions.

## Tag types

### Governed tags

Governed tags are account-level definitions with:

- a case-sensitive key;
- an optional controlled list of allowed values;
- principals allowed to assign the tag;
- principals allowed to manage its definition.

Governed tags must exist before `apply-tags` assigns them. Unity Catalog rejects an undefined or disallowed governed value.

Use governed tags for classifications that require consistency, including:

```text
data_domain
sensitivity
access_tier
retention_class
regulatory_scope
```

### System governed tags

System tags are predefined and maintained by Databricks. Customers cannot create or edit their definitions.

Examples include:

```text
system.certification_status
class.email_address
class.us_ssn
class.credit_card
sap.PersonalData.*
```

The tag-applier identity still needs `ASSIGN` permission to apply or remove an applicable system tag.

Data Classification normally owns `class.*` column assignments. The tag
applier treats those assignments as externally owned unless an explicit design
authorizes otherwise.

### Ordinary tags

Ordinary tags do not require an account-level definition. They are created when assigned to an object.

They provide less control because they do not enforce allowed values or assignment policy. Use them for low-risk metadata or migration, not critical policy-driving classifications.

## Bootstrap sequence

### 1. Define the taxonomy

Governance administrators define:

- tag key;
- purpose;
- applicable object types;
- allowed values;
- owning team;
- permitted assigners;
- ABAC or automation consumers;
- deprecation and migration rules.

Tag metadata is stored as plain text and may be replicated globally. Keys and values must not contain secrets, personal information, credentials, or sensitive free text.

### 2. Create governed definitions

An account or metastore administrator creates each governed tag in the Databricks Governed Tags interface or supported administrative API.

Example definitions:

```yaml
data_domain:
  allowed_values:
    - customer
    - finance
    - product
    - store

sensitivity:
  allowed_values:
    - public
    - internal
    - confidential
    - pii
    - pci

access_tier:
  allowed_values:
    - internal
    - analytics
    - restricted
```

This bootstrap YAML is illustrative documentation, not the assignment `tags.yml` consumed by the applier.

### 3. Grant tag assignment permission

Grant `ASSIGN` on every governed or system tag that the tag-applier service principal may set or remove.

Prefer a dedicated principal:

```text
sdp_tag_applier_sp
```

Do not grant broad governed-tag administration to the runtime identity.

### 4. Grant target permissions

For each catalog-scoped tags file, the runtime identity needs:

- `USE CATALOG` on the catalog;
- `USE SCHEMA` on every referenced schema;
- `APPLY TAG` on target objects or an approved parent scope;
- read access to Unity Catalog information schema;
- read/write access to the assignment-state table.

Grant the narrowest supported scope. A separate job identity may be used for each catalog.

### 5. Create the assignment-state location

Select a protected state table location:

```text
<catalog>.<sdp-meta-schema>.uc_governance_tag_assignments
```

Only governance operators and the tag-applier identity should modify it.

The state table contains:

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

Source IDs are encoded in the `contributors` JSON. Desired values remain in
`tags.yml`; they are not separate state columns.

### 6. Author the catalog-scoped assignment file

After the definitions and permissions exist, governance owners create `tags.yml`:

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

  retail_quarantine.customers_quarantine:
    table:
      access_tier: restricted
```

Every governed value must match the definition exactly, including case.

### 7. Run a dry-run

```text
databricks labs sdp-meta apply-tags \
  --tags-file main_prod.tags.yml \
  --state-table main_prod.sdp_meta.uc_governance_tag_assignments \
  --dry-run
```

Dry-run must:

- resolve all targets;
- validate tag syntax and limits;
- verify every configured table and column exists;
- detect ownership conflicts;
- perform no tag DDL;
- create or modify no state.

### 8. Apply and verify

The executable task:

1. persists pending ownership intent;
2. applies deterministic tag DDL;
3. reads `INFORMATION_SCHEMA.TABLE_TAGS` and `COLUMN_TAGS`;
4. verifies desired assignments;
5. records verified ownership and status;
6. fails the workflow on authorization, allowed-value, DDL, verification, or state errors.

## Recommended ownership boundaries

Assign each tag key to one primary mechanism:

- catalog-scoped `tags.yml` for explicit Git-controlled assignments;
- Data Classification for detected `class.*` column tags;
- Databricks Tag Automations for dynamic table-level rollups, certification, and deprecation;
- SAP integration for `sap.PersonalData.*`.

If another mechanism already created a matching tag, the tag applier records it as
externally owned and does not remove it.

Avoid configuring the tag applier and a Databricks automation to manage the same
key on the same object.

## Configuration requirements

The applier validates:

- UTF-8 key/value length of at most 256 characters;
- case-sensitive keys and values;
- no leading or trailing whitespace;
- Databricks disallowed characters;
- reserved system prefixes without rejecting valid system tags;
- configured column existence;
- one effective catalog per file.

Governance owners must separately ensure tag keys and values contain no secrets
or personal information.

Databricks currently supports up to 1,000 governed tags per account and up to 500 allowed values for each governed tag.

## Failure behavior

Tag application fails when:

- a governed tag definition is missing;
- a value is not allowed;
- the runtime identity lacks `ASSIGN`;
- the runtime identity lacks `APPLY TAG`, `USE CATALOG`, or `USE SCHEMA`;
- a target table or column does not exist;
- actual metadata does not match after DDL;
- ownership state cannot be persisted.

`apply-tags` returns a failure status but does not publish or unpublish objects.
A deployment workflow must consume that status if failed tagging is intended to
block publication or consumer access.

The tool must surface the affected object and tag key while avoiding sensitive tag values in normal logs.

## Readiness checklist

- [ ] Taxonomy keys and values approved
- [ ] Governed tags created at account level
- [ ] System-tag ownership boundaries documented
- [ ] `ASSIGN` granted to the tag-applier identity
- [ ] Unity Catalog target privileges granted
- [ ] Protected assignment-state table configured
- [ ] Catalog-scoped `source_id` selected
- [ ] `tags.yml` reviewed by the governance owner
- [ ] Dry-run succeeds with no conflicts
- [ ] Post-apply information-schema verification enabled
