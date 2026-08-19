---
id: automation-manifests
title: Governance Automation Manifests
---

# Security and governance automation templates

These templates define the contracts between Security, Governance, and Data
Engineering. They are desired-state inputs for a future Terraform, Databricks
CLI, or Python SDK adapter; they are not direct Databricks API payloads.

## Responsibility boundary

| Owner | Template | Creates or manages |
|---|---|---|
| Security | `security/account-identities.template.yml` | Account groups, service principals, ownership, and group membership |
| Governance | `governance/governed-tags.template.yml` | Governed tag definitions, allowed values, and ASSIGN/MANAGE permissions |
| Governance | `governance/abac-policies.template.yml` | Policy functions, ABAC policies, grants, and policy tests |
| Governance and data owner | `governance/catalog-tags.template.yml` | Approved table and column tag assignments for one catalog |
| Data Engineering | Approved catalog tags file | Runs data pipelines and the tag applier; optionally coordinates separate ABAC tests and publication controls |

Security owns identity and membership. Governance refers to the resulting
principal aliases but must not create or alter identity membership. The
pipeline runtime must not create tag definitions, change allowed values, or
create ABAC policies.

## Required execution order

1. Security validates and applies account identities.
2. Security exports immutable principal IDs and stable aliases.
3. Governance validates governed tag definitions against those aliases.
4. Governance applies tag definitions and assignment permissions.
5. Governance creates policy functions and applies ABAC policies.
6. Governance and data owners approve a catalog-specific tags file.
7. Data Engineering ensures the target Unity Catalog objects exist, whether
   produced by SDP-META or another system.
8. The tag applier reconciles the approved tags file and verifies assignments.
9. If required by the deployment design, a separate test step evaluates ABAC
   behavior with authorized and unauthorized principals.
10. If fail-closed publication is configured, the deployment workflow publishes
    or grants consumer access only after all required verification succeeds.

Tag definitions must exist before policies reference them. Policies must exist
before tagged objects are expected to receive ABAC enforcement. Steps 9 and 10
are workflow contracts for a future automation adapter; they are not actions
performed by `apply-tags`.

## Environment layout

Copy templates into an environment-owned configuration repository:

```text
governance/
  dev/
    security/account-identities.yml
    account/governed-tags.yml
    catalogs/main_dev/abac-policies.yml
    catalogs/main_dev/tags.yml
  prod/
    security/account-identities.yml
    account/governed-tags.yml
    catalogs/main_prod/abac-policies.yml
    catalogs/main_prod/tags.yml
```

Use one `tags.yml` per catalog per environment. A file can include multiple
schemas in that catalog. Keep account-level tag definitions separate because
their lifecycle and permissions are broader than catalog assignments.

## Recommended automation interface

An implementation should expose the same lifecycle for every manifest:

```text
validate MANIFEST
plan MANIFEST --output plan.json
approve plan.json
apply plan.json
verify MANIFEST --output evidence.json
```

`apply` must consume the reviewed immutable plan rather than recalculating it.
Each plan should contain the manifest hash, target account/workspace, actor,
adapter version, creates, updates, revocations, and deletions.

The SDP-META tag applier is the final catalog-assignment step:

```bash
databricks labs sdp-meta apply-tags \
  --tags-file environments/prod/catalogs/main_prod/tags.yml \
  --state-table main_prod.sdp_meta.uc_governance_tag_assignments \
  --warehouse-id "$DATABRICKS_WAREHOUSE_ID" \
  --dry-run
```

Remove `--dry-run` only after review. The production workflow should retain the
plan and verification evidence as job artifacts.

## Validation gates

Security validation must reject:

- unknown owners or external identity-provider groups;
- direct human membership where enterprise-group mapping is required;
- runtime service principals in governance-administrator groups;
- removal from a protected group without explicit approval;
- display-name ambiguity where immutable principal IDs are available.

Governance validation must reject:

- policy references to undefined tags, values, functions, groups, or columns;
- tag values outside an allowed governed-tag value set;
- overlapping column masks or row filters without an explicit conflict rule;
- catalog scopes that cross the environment boundary;
- runtime identities with governed-tag `MANAGE`;
- ownership of reserved prefixes such as `class.*` or `system.*`;
- publication without successful assignment and ABAC verification.

## Change approval

Routine additive changes can be automated after code-owner review. The
following require explicit Security or Governance approval:

- group deletion, protected membership removal, or privilege expansion;
- governed-tag deletion or removal of an allowed value;
- policy disablement, deletion, scope broadening, or bypass-group addition;
- assignment cleanup that removes a tag used by an active ABAC policy.

Production should use separate identities for security bootstrap, governance
bootstrap, pipeline execution, and tag assignment. No single runtime identity
should have all four permission sets.

## Outputs and handoff

Security automation publishes principal aliases and immutable IDs. Governance
automation consumes those outputs and publishes:

- tag-policy identifiers and effective permissions;
- policy and function identifiers;
- validation and policy-test evidence;
- approved catalog-specific `tags.yml` files.

Data Engineering consumes only the approved `tags.yml` and policy readiness
status. It does not need account-level governance administrator credentials.
