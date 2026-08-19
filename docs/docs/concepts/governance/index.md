---
id: index
title: Unity Catalog Governance
---

# Unity Catalog governance

SDP-META keeps pipeline metadata, tag assignments, and Unity Catalog policy
administration separate. This prevents onboarding from becoming a governance
database and allows the same tag applier to govern any existing Unity Catalog
table.

## End-to-end governance lifecycle

```mermaid
flowchart TD
    subgraph Account governance outside apply-tags
        SA[Security administrator] --> ID[Define groups and service principals]
        GA[Governance administrator] --> GT[Create governed tags and allowed values]
        GA --> AP[Create ABAC masks and row-filter policies]
    end

    subgraph Optional SDP-META data production
        ON[onboarding JSON or YAML] --> OB[SDP-META onboarding]
        OB --> DS[(Bronze and silver DataflowSpec)]
        DS --> LP[Run Lakeflow pipeline]
        LP --> TB[Unity Catalog tables]
    end

    EXT[Any existing Unity Catalog tables] --> TB
    TY[Catalog-scoped tags.yml] --> AT[Run sdp-meta apply-tags]
    TB --> AT
    ST[(Assignment ownership and retry state)] <--> AT

    AT --> VP[Validate configuration and preflight targets]
    VP --> PL[Plan ownership-aware reconciliation]
    PL -->|conflict or failure| FAIL[Return nonzero status without tag DDL]
    PL -->|valid plan| DDL[Apply table and column tag DDL]
    DDL --> TV[Read back TABLE_TAGS and COLUMN_TAGS]
    TV -->|mismatch| FAIL
    TV -->|verified| DONE[Persist applied state and return success]

    GT -. consumed by .-> UC[Unity Catalog]
    AP -. enforced by .-> UC
    ID -. evaluated by .-> UC
    DONE -. optional caller-controlled gate .-> WF[ABAC tests, RBAC changes, publication, and monitoring]
    WF -. if configured .-> UC
```

`onboarding.yml` and `tags.yml` are independent inputs. Onboarding produces
DataflowSpec records; it does not persist tag assignments. `apply-tags` reads
the catalog-scoped YAML directly and uses a Delta state table only for
assignment ownership and retry recovery. Existing Unity Catalog tables do not
need to originate from SDP-META.

## Simplified lifecycle

```text
1. Prepare Unity Catalog prerequisites
   Create governed tags, allowed values, and assignment permissions outside apply-tags

2. Author assignment desired state
   Create one catalog-scoped tags.yml; onboarding metadata remains independent

3. Ensure target objects exist
   Use tables created by SDP-META or any other Unity Catalog data producer

4. Preview reconciliation
   Run apply-tags with --dry-run and resolve ownership conflicts

5. Apply assignments
   Run apply-tags with --tags-file and --state-table

6. Verify assignments
   apply-tags reads back TABLE_TAGS and COLUMN_TAGS and returns success or failure

7. Run optional deployment controls
   The caller may test ABAC behavior, change RBAC, publish objects, or emit alerts

8. Enforce at query time
   Unity Catalog RBAC controls reachability; configured ABAC policies filter and mask

9. Reconcile or monitor again when scheduled
   Scheduling and continuous monitoring are external to apply-tags
```

## Query-time control sequence

```mermaid
flowchart TD
    Q[Query submitted] --> R{RBAC permits access to the object?}
    R -->|no| D[Access denied]
    R -->|yes| RF[Evaluate ABAC row filter]
    RF --> PR[Keep permitted rows]
    PR --> CM[Evaluate ABAC column mask]
    CM --> RV[Return permitted rows and values]
```

RBAC determines whether a principal can reach the catalog object. ABAC then
determines which rows and values that authorized principal can see. These
checks are performed by Unity Catalog. SDP-META assigns tags that separately
provisioned ABAC policies may consume.

## Tag assignment and optional deployment gate

```mermaid
flowchart LR
    TY[Catalog tags.yml] --> VP[Validate and preflight]
    TB[Any existing UC tables] --> VP
    AS[(Assignment ownership state)] --> PL[Plan reconciliation]
    VP --> PL

    PL -->|conflict| FAIL[Return failure]
    PL -->|valid| PI[Persist pending intent]
    PI --> DDL[Apply SET or UNSET TAG DDL]
    DDL --> IV{Assignments verified in UC metadata?}
    IV -->|no| FAIL
    IV -->|yes| FS[Persist applied state and return success]
    FS -. optional deployment workflow .-> EXT[ABAC tests, publication, RBAC, and monitoring]
```

`apply-tags` stops at assignment verification and communicates the result
through its exit status. A deployment workflow may use that status as one input
to a fail-closed publication gate, but the tagging command does not publish
objects, modify consumer RBAC, execute ABAC tests, or start monitoring.

## Implementation boundary

The packaged tagging feature currently implements:

- optional `tags.yml` skeleton generation from SDP-META onboarding metadata;
- catalog-scoped YAML validation and target preflight;
- ownership-aware table and column tag reconciliation;
- read-back verification through `TABLE_TAGS` and `COLUMN_TAGS`;
- retry state and non-destructive handling of externally managed tags;
- public Python `apply_tags()` API plus CLI, wheel-task, and DAB execution
  surfaces.

The following are governance workflow responsibilities, not actions performed
automatically by `apply-tags`:

- creating groups, governed-tag definitions, or ABAC policies;
- executing authorized and unauthorized ABAC behavior tests;
- granting or revoking consumer RBAC;
- moving objects between protected and published schemas;
- continuous schema-classification monitoring.

Those steps must be implemented by the deployment workflow if fail-closed
publication is required.

Key principles:

- `tags.yml` is the only desired-state source for assignments.
- One file governs one catalog and may include multiple schemas.
- Existing externally managed assignments are preserved.
- A deployment workflow may use tagging and policy verification to gate
  publication.
- Account-level security and governance bootstrap remains separate from
  pipeline runtime permissions.

Continue with [catalog-scoped tags](./catalog-scoped-tags.md) and the
[tagging guide](../../guides/governance/unity-catalog-tagging.md). To run the
complete notebook workflow, use the
[interactive governance tagging demo](https://github.com/databrickslabs/dlt-meta/tree/main/demo/governance-tagging).
