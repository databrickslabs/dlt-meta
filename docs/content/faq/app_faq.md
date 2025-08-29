---
title: "App"
date: 2025-08-29T14:50:11-04:00
weight: 63
draft: false
---

**Q.1. Do I need to run an initial setup before using the DLT Meta App ?**
Yes. Before you can use the DLT Meta App, you must click the Setup button to create the required DLT meta environment. This initializes the app and enables you to onboard or manage Delta Live Tables (DLT) pipelines.

**Q. 2. What are the main features of the DLT Meta App ?**
The DLT Meta App provides several key capabilities:

Onboard new DLT pipelines through an interactive interface.
Deploy and manage DLT pipelines directly in the app.
Run DLT meta app demo flows to explore example pipelines and usage patterns.
Use the command-line interface (CLI) to automate onboarding, deployment, and management operations.

**Q. 3. Who can access and use the DLT Meta App ?**
Only authenticated Databricks workspace users with appropriate permissions can access and use the app:

You need CAN_USE permission to run the app and CAN_MANAGE permission to administer it.
The app can be shared within your workspace or account, but not with external users.
Every user must log in with their Databricks account credentials.

**Q. 4. How does catalog and schema access work in the DLT Meta App ?**
By default, the app uses a dedicated Service Principal (SP) identity for all data and resource access:

The SP must have explicit permissions (such as USE CATALOG, USE SCHEMA, SELECT) on all Unity Catalog resources the DLT pipelines reference.
If a user accesses the app—even via the same URL—their abilities depend on the SP’s granted access. If the SP lacks permissions, the app’s functionality fails for all users.
Optionally, the app can be configured to use users’ own permissions via On-Behalf-Of (OBO) mode, but this requires additional setup.

**Q. 5. How should I resolve access errors or permission issues in the app ?**
If you experience errors related to catalog, schema, or table access:

Verify the app’s Service Principal has the required permissions in Unity Catalog.
Confirm the app is attached to the necessary resources, such as warehouses or secrets.
Check if recent administrative or sharing changes have affected your privileges.
Review audit logs for permission denials or configuration changes.
Consult your Databricks workspace administrator if necessary.

**Q. 6. How is sharing, security, and isolation managed for the DLT Meta App ?**
The app operates in a multi-tenant platform, but provides strong isolation between customer accounts and apps.
Each app runs on a dedicated, isolated, serverless compute environment.
Sharing is restricted to specific users, groups, or all account users; every sharing and permission event is audit-logged.
There is no option for public or anonymous access—only Databricks account users can run the app.

**Q. 7. What are best practices for securing and operating the DLT Meta App ?**
Grant only the minimum required catalog and schema permissions to the app’s Service Principal (principle of least privilege).
Regularly review all permission and sharing changes using audit logs.
Allow only trusted application code to run, especially if enabling OBO mode.
Use workspace monitoring tools and collaborate with your Databricks administrator for access adjustments or troubleshooting.
