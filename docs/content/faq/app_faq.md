---
title: "App"
date: 2021-08-04T14:26:55-04:00
weight: 63
draft: false
---

### Initial Setup

**Q1. Do I need to run an initial setup before using the DLT-META App?**

Yes. Before using the DLT-META App, you must click the Setup button to create the required dlt-meta environment. This initializes the app and enables you to onboard or manage Lakeflow Declarative Pipelines.

### Features and Capabilities

**Q2. What are the main features of the DLT-META App?**

The DLT-META App provides several key capabilities:
- Onboard new Lakeflow Declarative Pipeline through an interactive interface
- Deploy and manage pipelines directly in the app
- Run demo flows to explore example pipelines and usage patterns
- Use the command-line interface (CLI) to automate operations

### Access and Permissions

**Q3. Who can access and use the DLT-META App?**

Only authenticated Databricks workspace users with appropriate permissions can access and use the app:
- You need `CAN_USE` permission to run the app
- You need `CAN_MANAGE` permission to administer it
- The app can be shared within your workspace or account
- Every user must log in with their Databricks account credentials

### Resource Access

**Q4. How does catalog and schema access work in the DLT-META App?**

By default, the app uses a dedicated Service Principal (SP) for all data and resource access:
- The Service Principal needs explicit permissions (`USE CATALOG`, `USE SCHEMA`, `SELECT`) on all Unity Catalog resources
- User abilities depend on the Service Principal's access, regardless of URL
- Optional On-Behalf-Of (OBO) mode uses individual user permissions



