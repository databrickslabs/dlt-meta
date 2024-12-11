---
title: "DLT-META"
date: 2021-08-04T14:50:11-04:00
draft: false
---


## Project Overview
DLT-META is a metadata-driven framework designed to work with Databricks Delta Live Tables (DLT). This framework enables the automation of bronze and silver data pipelines by leveraging metadata recorded in an onboarding JSON file. This file, known as the Dataflowspec, serves as the data flow specification, detailing the source and target metadata required for the pipelines.

In practice, a single generic DLT pipeline reads the Dataflowspec and uses it to orchestrate and run the necessary data processing workloads. This approach streamlines the development and management of data pipelines, allowing for a more efficient and scalable data processing workflow

### DLT-META components:

#### Metadata Interface 
- Capture input/output metadata in [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.template)
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
- [Metadata Preparation](https://databrickslabs.github.io/dlt-meta/getting_started/metadatapreperation/)
- Onboarding Job
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#onboardjob)
    - option#3: [Databricks Notebook](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#option2-databricks-notebook)

- Dataflow DLT Pipeline
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#dataflow-dlt-pipeline)
    - Option#2: [DLT-META MANUAL](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#dataflow-dlt-pipeline)


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
