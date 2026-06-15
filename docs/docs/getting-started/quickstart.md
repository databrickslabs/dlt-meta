---
id: quickstart
title: Quickstart
sidebar_position: 1
---

# Quickstart

Get a SDP-META pipeline running using the DAB path with the `--quickstart` flag.

## Steps

### 1. Install

```bash
pip install databricks-labs-sdp-meta
databricks labs install sdp-meta
```

### 2. Authenticate

```bash
databricks auth login --host YOUR_WORKSPACE_URL
```

### 3. Scaffold a bundle

```bash
databricks labs sdp-meta bundle-init --quickstart
```

Creates a bundle directory with job definition, pipeline definition, runner notebook, and a sample onboarding file.

### 4. Edit variables

Open `resources/variables.yml` and set at minimum:

- `uc_catalog_name` — your Unity Catalog catalog name
- `sdp_meta_schema` — schema where DataflowSpec tables will be created
- `bronze_target_schema` — schema for Bronze output tables
- `sdp_meta_dependency` — a PyPI coordinate (`databricks-labs-sdp-meta==0.1.0`) or `/Volumes/...` wheel path

:::warning
`--quickstart` leaves `sdp_meta_dependency` set to `__SET_ME__`. `bundle-validate` rejects this placeholder, so you must set a real value before deploying.
:::

### 5. Validate

```bash
databricks labs sdp-meta bundle-validate
```

### 6. Deploy

```bash
databricks bundle deploy
```

### 7. Run the onboarding job

```bash
databricks bundle run sdp_meta_onboarding_job
```

### 8. Start the pipeline

```bash
databricks bundle run sdp_meta_pipeline
```

For all bundle options — scaffolding modes, split vs combined pipelines, adding flows, CI/CD setup — see [Declarative Automation Bundles](./dabs.md).
