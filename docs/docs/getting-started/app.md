---
id: app
title: Databricks App
sidebar_position: 5
---

# Databricks App

The SDP-META Databricks App is a browser-based GUI for onboarding and managing Lakeflow Spark Declarative Pipelines without using the CLI or editing YAML files.

## Prerequisites

- Python 3.8 or higher
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) (v0.244.0 or later recommended)
- Configured workspace access

## Deploy to Databricks

### Step 1 — Authenticate

```bash
databricks auth login --host YOUR_WORKSPACE_URL
```

### Step 2 — Create the app

```bash
databricks apps create demo-sdp-meta
```

### Step 3 — Sync and deploy

```bash
cd sdp-meta/databricks_app
databricks sync . /Workspace/Users/your.email@company.com/testapp
databricks apps deploy demo-sdp-meta --source-code-path /Workspace/Users/your.email@company.com/testapp
```

The app URL appears in the command output. You can also find it in the Databricks Web UI under **New → App**.

## Run locally

```bash
cd sdp-meta/databricks_app
pip install -r requirements.txt
databricks configure --host YOUR_WORKSPACE_URL --token YOUR_TOKEN
python App.py
```

Access the app at [http://127.0.0.1:5000](http://127.0.0.1:5000).

## Using the app

### Initial setup

Before using any features, click **Setup sdp-meta project environment** to initialize the required environment.

### Onboard a pipeline

![App onboarding UI](/img/app_onboarding.png)

Go to the **UI** tab and fill in the onboarding form (workspace, catalog, schema, onboarding file path, layer, group).

### Deploy a pipeline

![App deploy pipeline UI](/img/app_deploy_pipeline.png)

Use the deploy section of the **UI** tab to create and launch a Lakeflow Spark Declarative Pipeline.

### Run demos

![App demo tab](/img/app_run_demos.png)

The **Demo** tab lists pre-configured example pipelines.

### CLI tab

![App CLI tab](/img/app_cli.png)

The **CLI** tab exposes command-line operations directly in the browser.

## FAQ

**Q: Do I need to run initial setup before using the SDP-META App?**

Yes. Click the **Setup** button to create the required SDP-META environment before using any other features.

**Q: Who can access and use the SDP-META App?**

Authenticated Databricks workspace users with `CAN_USE` permission. `CAN_MANAGE` is required for administration.

**Q: How does catalog and schema access work?**

By default, the app uses a dedicated Service Principal for all data and resource access. The Service Principal needs `USE CATALOG`, `USE SCHEMA`, and `SELECT` on all Unity Catalog resources. An optional On-Behalf-Of (OBO) mode uses individual user permissions instead.
