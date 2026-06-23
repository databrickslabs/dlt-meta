---
id: cli
title: Interactive CLI
sidebar_position: 3
---

# Interactive CLI

The SDP-META CLI provides interactive commands to onboard and deploy pipelines without writing bundle YAML.

:::note
For production workloads and team environments, use [Declarative Automation Bundles](./dabs.md). The CLI does not produce a git-trackable artifact.
:::

## Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) v0.213 or later

## Setup

**Step 1 — Authenticate:**

```bash
databricks auth login --host YOUR_WORKSPACE_URL
```

**Step 2 — Install SDP-META:**

```bash
databricks labs install sdp-meta
```

**Step 3 — Run onboarding:**

```bash
databricks labs sdp-meta onboard
```

The command prompts for workspace, catalog, schema, onboarding file path, and more. If you have cloned the SDP-META repository locally, pressing Enter accepts demo defaults from the `demo/` folder.

![SDP-META onboarding CLI](/img/onboardingDLTMeta.gif)

When the onboarding job finishes, it pushes code and data to your workspace, creates an onboarding job, and opens the job URL in your browser.

**Step 4 — Deploy the pipeline:**

```bash
databricks labs sdp-meta deploy
```

![SDP-META deploy CLI](/img/onboardingDLTMeta_2.gif)

![SDP-META bronze+silver deploy](/img/deployingDLTMeta_bronze_silver.gif)

Provide the same schema and group values you used in the onboarding step.

## Deploy command — layer options

| Mode | What deploys |
|---|---|
| `bronze` | A single pipeline for the Bronze layer only |
| `silver` | A single pipeline for the Silver layer only |
| `bronze_silver` | A combined pipeline materializing both layers in one DAG |
