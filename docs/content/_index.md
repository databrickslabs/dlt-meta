---
title: "DLT-META"
date: 2021-08-04T14:50:11-04:00
draft: false
---

# DLT-META 
DLT META is a metadata-driven Databricks Delta Live Tables (aka DLT) framework which lets you automate your bronze and silver pipelines.

## Project Overview
With this framework you need to record the source and target metadata in an onboarding json file which acts as the Dataflowspec. A single generice dlt pipeline takes the Dataflowspec and runs your workloads.

### DLT-META components:

#### Metadata Interface 
- Capture input/output metadata in [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json)
- Capture [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe/customers/bronze_data_quality_expectations.json)
- Capture  processing logic as sql in [Silver transformation file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json)

#### Generic DLT pipeline
- Apply appropriate readers based on input metadata
- Apply data quality rules with DLT expectations 
- Apply CDC apply changes if specified in metadata
- Builds DLT graph based on input/output metadata
- Launch DLT pipeline

## High-Level Solution overview:
![High-Level Process Flow](/images/solutions_overview.png)

## How does DLT-META work?
![DLT-META Stages](/images/dlt-meta_stages.png)
- Prepare [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json)
- DLT-META have onboarding API: `OnboardDataflowspec.onboard_dataflow_specs()` which coverts onboarding json file into DataflowSpecs delta table for bronze and silver layer. Dataflowspecs are used by DataflowPipeline which launches DLT for respective bronze and silver layer.
- During onboarding process it creates two metadata tables aka DataflowSpec for bronze and silver layer from onboarding json
![DataflowSpec Data Model](/images/dataflowSpec_model.png)
- This way we know all the details about source and target from dataflowSpec
- Launch Dataflow DLT pipeline 


## How much does it cost ?
DLT-META does not have any **direct cost** associated with it other than the cost to run the Databricks Delta Live Tables 
on your environment.The overall cost will be determined primarily by the [Databricks Delta Live Tables Pricing] (https://databricks.com/product/delta-live-tables-pricing-azure)


## More questions
Refer to the [FAQ]({{%relref "faq/_index.md" %}})

## Getting Started
Refer to the  [Getting Started]({{%relref "getting_started/_index.md" %}}) guide

## Project Support
Please note that all projects in the databrickslabs github account are provided for your 
exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).
They are provided AS-IS and we do not make any guarantees of any kind.
Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.
They will be reviewed as time permits, but there are no formal SLAs for support.


# Contributing 
See our [CONTRIBUTING]({{%relref "contributing/_index.md" %}}) for more details.
