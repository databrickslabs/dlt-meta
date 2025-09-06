---
title: "DLT-META"
date: 2021-08-04T14:50:11-04:00
draft: false

---


## Project Overview
DLT-META is a metadata-driven framework designed to work with Databricks Lakeflow Declarative Pipelines . This framework enables the automation of bronze and silver data pipelines by leveraging metadata recorded in an onboarding JSON file. This file, known as the Dataflowspec, serves as the data flow specification, detailing the source and target metadata required for the pipelines.

In practice, a single generic pipeline reads the Dataflowspec and uses it to orchestrate and run the necessary data processing workloads. This approach streamlines the development and management of data pipelines, allowing for a more efficient and scalable data processing workflow

### DLT-META components:

#### Metadata Interface 
- Capture input/output metadata in [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.template)
- Capture [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe/customers/bronze_data_quality_expectations.json)
- Capture  processing logic as sql in [Silver transformation file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json)

#### Generic Lakeflow Declarative pipeline
- Apply appropriate readers based on input metadata
- Apply data quality rules with Lakeflow Declarative Pipelines expectations 
- Apply CDC apply changes if specified in metadata
- Builds Lakeflow Declarative Pipelines graph based on input/output metadata
- Launch Lakeflow Declarative Pipelines pipeline

## High-Level Solution overview:
![High-Level Process Flow](/images/solutions_overview.png)

## How does DLT-META work?
![DLT-META Stages](/images/dlt-meta_stages.png)
- [Metadata Preparation](https://databrickslabs.github.io/dlt-meta/getting_started/metadatapreperation/)
- Onboarding Job
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#onboardjob)
    - option#3: [Databricks Notebook](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#option2-databricks-notebook)

- Dataflow Lakeflow Declarative Pipeline
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#dataflow-dlt-pipeline)
    - Option#2: [DLT-META MANUAL](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#dataflow-dlt-pipeline)

## DLT-META DLT Features support
| Features  | DLT-META Support |
| ------------- | ------------- |
| Input data sources  | Autoloader, Delta, Eventhub, Kafka, snapshot  |
| Medallion architecture layers | Bronze, Silver  |
| Custom transformations | Bronze, Silver layer accepts custom functions|
| Data Quality Expecations Support | Bronze, Silver layer |
| Quarantine table support | Bronze layer |
| [create_auto_cdc_flow](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-apply-changes) API support | Bronze, Silver layer | 
| [create_auto_cdc_from_snapshot_flow](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-apply-changes-from-snapshot) API support | Bronze layer|
| [append_flow](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-append-flow) API support | Bronze layer|
| Liquid cluster support | Bronze, Bronze Quarantine, Silver, Silver Quarantine tables|
| [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/) |  ```databricks labs dlt-meta onboard```, ```databricks labs dlt-meta deploy``` |
| Bronze and Silver pipeline chaining | Deploy dlt-meta pipeline with ```layer=bronze_silver``` option using default publishing mode |
| [DLT Sinks](https://docs.databricks.com/aws/en/dlt/dlt-sinks) | Supported formats:external ```delta table```, ```kafka```.Bronze, Silver layers|
## How much does it cost ?
DLT-META does not have any **direct cost** associated with it other than the cost to run the Databricks Lakeflow Declarative Pipelines 
on your environment.The overall cost will be determined primarily by the [Databricks Lakeflow Declarative Pipelines Pricing] (https://www.databricks.com/product/pricing/lakeflow-declarative-pipelines)


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
