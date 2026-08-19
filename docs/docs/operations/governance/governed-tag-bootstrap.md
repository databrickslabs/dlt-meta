---
id: governed-tag-bootstrap
title: Governed-tag Bootstrap
---

# Governed-tag bootstrap automation

**Scope:** Account-level governed-tag definitions, tag policies, permissions, and handoff to catalog-scoped assignment. The manifests referenced here are illustrative templates under `examples/governance/automation-templates/`; adapt them to your account tooling.

References:

- [Governed tags](https://docs.databricks.com/aws/en/admin/governed-tags/)
- [Create and manage governed tags](https://docs.databricks.com/aws/en/admin/governed-tags/manage-governed-tags)
- [Manage governed-tag permissions](https://docs.databricks.com/aws/en/admin/governed-tags/manage-permissions)
- [Databricks CLI tag-policy commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/tag-policies-commands)
- [Terraform `databricks_tag_policy`](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/tag%5Fpolicy)
- [Security and governance automation manifests](../../reference/governance/automation-manifests.md)

## Decision

Automate governed-tag creation and permission management as a separate account-level governance bootstrap.

Do not allow the catalog-scoped runtime tag applier to create or modify governed-tag definitions.

```text
Central governance bootstrap
        |
        +-- Create/update governed-tag policies
        +-- Define allowed values
        +-- Grant CREATE/MANAGE/ASSIGN
        +-- Optionally deploy ABAC policies
        |
        v
Catalog deployment
        |
        +-- Run data pipeline
        +-- Apply explicit tags from catalog tags.yml
        +-- Verify table and column assignments
```

## Responsibility boundaries

### Governance bootstrap identity

The bootstrap identity may receive:

- account-level `CREATE` for governed tags;
- `MANAGE` on governed-tag definitions;
- permission-management access;
- catalog policy-administration privileges where ABAC is deployed.

It runs infrequently through a centrally controlled Terraform or governance deployment.

### Runtime tag-applier identity

The runtime identity receives only:

- `ASSIGN` on approved governed or system tags;
- `USE CATALOG`;
- `USE SCHEMA`;
- `APPLY TAG`;
- information-schema read access;
- assignment-state table read/write access.

It cannot create, edit, or delete governed-tag definitions.

## Configuration layers

Keep account definitions separate from catalog assignments.

### Account-level bootstrap manifest

```yaml
version: "1"

governed_tags:
  data_domain:
    description: Business domain represented by the data
    allowed_values:
      - customer
      - finance
      - product
      - store

  sensitivity:
    description: Data sensitivity classification
    allowed_values:
      - public
      - internal
      - confidential
      - pii
      - pci

  access_tier:
    description: Broad access classification
    allowed_values:
      - internal
      - analytics
      - restricted

permissions:
  assign:
    - principal: sdp_tag_applier_sp
      tags:
        - data_domain
        - sensitivity
        - access_tier

  manage:
    - principal: governance_admins
      tags:
        - data_domain
        - sensitivity
        - access_tier
```

This manifest is an input to the governance bootstrap, not to `apply-tags`.

### Catalog assignment file

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

  retail_quarantine.customers:
    table:
      access_tier: restricted
```

The assignment file may use only definitions and values already authorized by the bootstrap.

## Recommended implementation: Terraform

Use Terraform as the production source of truth for governed-tag policies:

```hcl
resource "databricks_tag_policy" "data_domain" {
  tag_key     = "data_domain"
  description = "Business domain represented by the data"

  values = [
    { name = "customer" },
    { name = "finance" },
    { name = "product" },
    { name = "store" },
  ]
}

resource "databricks_tag_policy" "sensitivity" {
  tag_key     = "sensitivity"
  description = "Data sensitivity classification"

  values = [
    { name = "public" },
    { name = "internal" },
    { name = "confidential" },
    { name = "pii" },
    { name = "pci" },
  ]
}
```

Use the Databricks provider's account access-control resources to grant `ASSIGN` and `MANAGE`.

Terraform is preferred because it provides:

- reviewable plans;
- drift detection;
- import of existing policies;
- controlled updates;
- explicit deletion review;
- auditable account-level ownership.

## CLI bootstrap option

For development or controlled migration:

```bash
databricks tag-policies create-tag-policy data_domain \
  --json '{
    "description": "Business domain represented by the data",
    "allowed_values": ["customer", "finance", "product", "store"]
  }'

databricks tag-policies create-tag-policy sensitivity \
  --json '{
    "description": "Data sensitivity classification",
    "allowed_values": ["public", "internal", "confidential", "pii", "pci"]
  }'
```

The CLI also supports get, list, update, and delete operations.

Permission grants use governed-tag permission APIs or Terraform; tag-policy creation alone does not grant the runtime identity `ASSIGN`.

## SDK bootstrap option

A controlled administration service may use the Databricks SDK:

```python
from databricks.sdk import WorkspaceClient

workspace = WorkspaceClient()

existing = {
    policy.tag_key: policy
    for policy in workspace.tag_policies.list_tag_policies()
}
```

The service then creates missing policies and updates changed descriptions or allowed values.

SDK-based bootstrap must produce a dry-run plan before mutation and use the Account Access Control Proxy API for permissions.

## Idempotent reconciliation

For each governed-tag policy:

1. read the existing definition;
2. compare description and allowed values;
3. report creates, additions, removals, and permission changes;
4. reject destructive changes without explicit approval;
5. create missing definitions;
6. update approved differences;
7. verify the final policy;
8. verify expected `ASSIGN` and `MANAGE` grants.

An unchanged second run produces no mutation.

## Destructive-change controls

Require explicit approval for:

- deleting a governed-tag policy;
- removing an allowed value;
- changing the semantic meaning of an existing key;
- revoking `ASSIGN` from an active runtime principal;
- converting an actively used ordinary tag key into a governed tag.

Creating a governed tag with a key already assigned to objects immediately governs existing assignments with that key. Existing out-of-policy values remain assigned but cannot be reassigned with the same invalid value. Review existing assignments before governing an established key.

## System governed tags

Do not create or modify:

```text
system.*
class.*
sap.*
```

Databricks owns these definitions and allowed values.

The bootstrap may grant `ASSIGN` where supported, but assignment ownership remains with the approved mechanism:

- `system.certification_status`: governance workflow or automation;
- `class.*`: Databricks Data Classification;
- `sap.PersonalData.*`: SAP integration.

The catalog tag applier treats externally created system assignments as externally owned.

## ABAC policy bootstrap

ABAC policies are separate from governed-tag definitions.

If an organization uses governed tags for masks or row filters:

```text
Create governed tags
        |
        v
Create protected policy functions
        |
        v
Create catalog/schema ABAC policies
        |
        v
Grant runtime permissions
        |
        v
Apply tags to objects
```

ABAC policy deployment belongs in the central governance Terraform or bundle. The catalog `apply-tags` runtime must not create policy functions or ABAC policies.

## Deployment topology

```text
Account governance repository
  └── Terraform / bootstrap manifest
        ├── governed-tag definitions
        ├── allowed values
        ├── ASSIGN/MANAGE grants
        └── optional ABAC policies

Data-product deployment repository
  └── catalog-scoped tags.yml
        ├── explicit table assignments
        ├── explicit column assignments
        └── standalone apply-tags job run after the pipeline
```

Bootstrap completes before any catalog tagging job runs.

## Failure behavior

Bootstrap fails when:

- the identity lacks `CREATE` or `MANAGE`;
- an existing definition conflicts with the approved taxonomy;
- destructive changes lack approval;
- permission grants cannot be verified;
- final policies differ from desired state.

Catalog assignment fails when:

- a governed definition is missing;
- a value is not allowed;
- the runtime identity lacks `ASSIGN`;
- the runtime identity lacks Unity Catalog target privileges.

Neither failure is downgraded to a warning.
