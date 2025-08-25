---
title: "General"
date: 2021-08-04T14:50:11-04:00
weight: 62
draft: false
---

**Q. What is DLT-META ?**

DLT-META is a solution/framework using Databricks Lakeflow Declarative Pipelines which helps you automate bronze and silver layer pipelines using CI/CD.

**Q. What are the benefits of using DLT-META ?**

- With DLT-META customers needs to only maintain metadata like onboarding.json, data quality rules and silver transformations and framework will take care of execution.
- In case of any input/output or data quality rules or silver transformation logic changes there will be only metadata changes using onboarding interface and no need to re-deploy pipelines.
- If you have 100s or 1000s of tables then DLT-META speeds up overall turn around time to production as customers needs to just produce metadata

**Q. What different types of reader are supported using DLT-META ?**

DLT-META uses Databricks [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html), DELTA, KAFKA, EVENTHUB to read from s3/adls/blog storage.

**Q. Can DLT-META support any other readers?**

DLT-META can support any spark streaming reader. You can override ```read_bronze()``` api inside ```DataflowPipeline.py``` to support any reader

**Q. Who should use this framework ?**

Data Engineering teams, who wants to automate data migrations at scale using CI/CD. 

