# Unity Catalog Tagging Interactive Demo

This demo exercises SDP-META governance tagging against four ordinary Unity
Catalog Delta tables:

- `customers`
- `transactions`
- `products`
- `orders`

Each table contains 100 deterministic rows. The notebook creates and verifies
the resources and can apply tags directly through the SDP-META Python API. An
optional CLI mode prints commands for the customer-facing Labs CLI.

## Files

- `UC_TAGGING_INTERACTIVE_DEMO.py` — Databricks notebook for setup,
  `tags.yml` generation, application, verification, idempotency guidance,
  and cleanup.

## Prerequisites

- An existing Unity Catalog catalog.
- Permission to create a schema, managed Volume, and tables; apply tags; read
  information schema; and create the Delta ownership-state table.
- For CLI mode only: an authenticated Databricks CLI, an SDP-META Labs
  installation, and a running SQL warehouse.

The demo uses ordinary `sdp_meta_demo_*` tags, so governed-tag definitions and
`ASSIGN` permission are not required.

## Run the demo

### 1. Import the notebook

Import `demo/governance-tagging/UC_TAGGING_INTERACTIVE_DEMO.py` into a
Databricks workspace.

Set:

- `install_source`: `git_branch` or `whl_file`
- `git_branch` when installing from GitHub
- `whl_file_path` when installing a wheel from a UC Volume, for example
  `/Volumes/<catalog>/<schema>/<volume>/sdp_meta-<version>-py3-none-any.whl`
- `uc_catalog_name`
- `uc_schema_name`
- `uc_volume_name`: managed UC Volume where the notebook writes `tags.yml`
- `execution_mode`: `api` (default) or `cli`
- `warehouse_id` when using CLI mode

The notebook installs SDP-META and restarts Python before creating any demo
resources. The default catalog/schema are:

```text
sdp_meta.governance_tagging_demo
```

Run the notebook through the setup-validation section. It creates all four
tables and confirms each contains 100 rows.

In `api` mode, the apply cell:

1. creates a managed UC Volume if it does not exist;
2. writes the desired configuration to
   `/Volumes/<catalog>/<schema>/<volume>/tags.yml`;
3. invokes the public `apply_tags(..., dry_run=True)` Python API;
4. invokes `apply_tags(...)` for live application;
5. uses the active Spark session for metadata reads, tag DDL, and Delta state.

### 2. Optional CLI mode

Choose `execution_mode=cli` to exercise the Labs CLI instead of applying from
the notebook. The notebook prints commands to download its generated Volume
file and then preview and apply it using the selected warehouse.

### 3. Preview the CLI plan

From the repository root:

```bash
databricks fs cp \
  dbfs:/Volumes/<catalog>/<schema>/<volume>/tags.yml \
  ./tags.yml \
  --overwrite

databricks labs sdp-meta apply-tags \
  --tags-file ./tags.yml \
  --state-table sdp_meta.governance_tagging_demo.uc_governance_tag_assignments \
  --warehouse-id "$DATABRICKS_WAREHOUSE_ID" \
  --dry-run
```

If needed, select a Databricks CLI profile before running:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile-name>
```

### 4. Apply through the CLI

After reviewing the plan:

```bash
databricks labs sdp-meta apply-tags \
  --tags-file ./tags.yml \
  --state-table sdp_meta.governance_tagging_demo.uc_governance_tag_assignments \
  --warehouse-id "$DATABRICKS_WAREHOUSE_ID"
```

### 5. Verify

Return to the notebook and run the verification sections. They assert:

- eight explicit table tags;
- eight explicit column tags;
- sixteen applied, script-owned state records.

Rerun the API apply cell or the same CLI command to demonstrate idempotency.

### 6. Clean up

Set the notebook `cleanup` widget to `true` and run its final cell. The notebook
drops the demo schema and all contained tables.
