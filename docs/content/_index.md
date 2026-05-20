---
title: "SDP-META"
date: 2021-08-04T14:50:11-04:00
draft: false

---


## Project Overview
`SDP-META` is a metadata-driven framework for building automated bronze and silver data pipelines using [Databricks Lakeflow Spark Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines).

The framework leverages metadata recorded in an onboarding JSON file (the Dataflowspec) to automate pipeline creation. A single generic pipeline reads the Dataflowspec and uses it to orchestrate and run the necessary data processing workloads, streamlining development and enabling scalable data processing.


### SDP-META components:

#### Metadata Interface 
- Capture input/output metadata in an onboarding file — JSON ([`examples/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/onboarding.template)) or YAML ([`examples/yml/onboarding.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/onboarding.yml))
- Capture [Data Quality Rules](https://github.com/databrickslabs/sdp-meta/tree/main/examples/json/dqe/customers/bronze_data_quality_expectations.json) (JSON or YAML)
- Capture processing logic as sql in a [Silver transformation file](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/silver_transformations.json) (JSON or YAML)

#### Generic Declarative Pipeline
- Apply appropriate readers based on input metadata
- Apply data quality rules with Lakeflow Spark Declarative Pipelines expectations
- Apply CDC apply changes if specified in metadata
- Builds declarative pipeline graph based on input/output metadata

## High-Level Solution overview:
![High-Level Process Flow](/images/solutions_overview.png)

## How does SDP-META work?
![SDP-META Stages](/images/sdp-meta_stages.png)
- [Metadata Preparation](https://databrickslabs.github.io/sdp-meta/getting_started/metadatapreperation/)
- Onboarding Job
    - Option#1: [SDP-META CLI](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#onboardjob)
    - option#3: [Databricks Notebook](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#option2-databricks-notebook)

- Dataflow Lakeflow Spark Declarative Pipeline
    - Option#1: [SDP-META CLI](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_cli/#dataflow-dlt-pipeline)
    - Option#2: [SDP-META MANUAL](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#dataflow-dlt-pipeline)

## SDP-META Feature Support
| Features  | SDP-META Support |
| ------------- | ------------- |
| Input data sources (Autoloader, Delta, Eventhub, Kafka, snapshot) | Yes |
| Medallion architecture layers (Bronze, Silver)  | Yes |
| Custom transformations (Bronze, Silver) | Yes |
| [append_flow](https://docs.databricks.com/en/delta-live-tables/flows.html#use-append-flow-to-write-to-a-streaming-table-from-multiple-source-streams) API support | Yes |
| Data Quality Expectations | Yes |
| Quarantine table support | Yes |
| [create_auto_cdc_flow](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-apply-changes) API support | Yes |
| [create_auto_cdc_from_snapshot_flow](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-apply-changes-from-snapshot) API support | Yes |
| Liquid cluster support | Yes |
| [create_sink](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-sink) API support | Yes |
| [SDP-META CLI](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_cli/) | Yes |
| Bronze and Silver pipeline chaining | Yes |
| [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) | Yes |
| [SDP-META UI](https://github.com/databrickslabs/sdp-meta/tree/main/lakehouse_app#sdp-meta-lakehouse-app-setup) | Yes |

## How much does it cost?
SDP-META is open source and has no direct cost. The overall cost is determined by [Databricks Lakeflow Spark Declarative Pipelines Pricing](https://www.databricks.com/product/pricing/lakeflow-declarative-pipelines).


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
