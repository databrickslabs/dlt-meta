# DLT-META

<!-- Top bar will be removed from PyPi packaged versions -->
<!-- Dont remove: exclude package -->
[Documentation](https://databrickslabs.github.io/dlt-meta/) |
[Release Notes](CHANGELOG.md) |
[Examples](https://github.com/databrickslabs/dlt-meta/tree/main/examples) 
<!-- Dont remove: end exclude package -->

---
<p align="left">
    <a href="https://databrickslabs.github.io/dlt-meta/">
        <img src="https://img.shields.io/badge/DOCS-PASSING-green?style=for-the-badge" alt="Documentation Status"/>
    </a>
    <a href="https://pypi.org/project/dlt-meta/">
        <img src="https://img.shields.io/badge/PYPI-v%200.0.1-green?style=for-the-badge" alt="Latest Python Release"/>
    </a>
    <a href="https://github.com/databrickslabs/dlt-meta/actions/workflows/onpush.yml">
        <img src="https://img.shields.io/github/workflow/status/databrickslabs/dlt-meta/build/main?style=for-the-badge"
             alt="GitHub Workflow Status (branch)"/>
    </a>
    <a href="https://codecov.io/gh/databrickslabs/dlt-meta">
        <img src="https://img.shields.io/codecov/c/github/databrickslabs/dlt-meta?style=for-the-badge&amp;token=2CxLj3YBam"
             alt="codecov"/>
    </a>
    <a href="https://lgtm.com/projects/g/databrickslabs/dlt-meta/alerts">
        <img src="https://img.shields.io/lgtm/alerts/github/databricks/dlt-meta?style=for-the-badge" alt="lgtm-alerts"/>
    </a>
    <a href="https://lgtm.com/projects/g/databrickslabs/dlt-meta/context:python">
        <img src="https://img.shields.io/lgtm/grade/python/github/databrickslabs/dbx?style=for-the-badge"
             alt="lgtm-code-quality"/>
    </a>
    <a href="https://pypistats.org/packages/dl-meta">
        <img src="https://img.shields.io/pypi/dm/dlt-meta?style=for-the-badge" alt="downloads"/>
    </a>
    <a href="https://github.com/PyCQA/flake8">
        <img src="https://img.shields.io/badge/FLAKE8-FLAKE8-lightgrey?style=for-the-badge"
             alt="We use flake8 for formatting"/>
    </a>
</p>

---

# Project Overview
```DLT-META``` is a metadata-driven framework based on Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) (aka DLT) which lets you automate your bronze and silver data pipelines.

With this framework you need to record the source and target metadata in an onboarding json file which acts as the data flow specification aka Dataflowspec. A single generic ```DLT``` pipeline takes the ```Dataflowspec``` and runs your workloads.

### Components:

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

## High-Level Process Flow:
![DLT-META High-Level Process Flow](./docs/static/images/solutions_overview.png)

## Steps
![DLT-META Stages](./docs/static/images/dlt-meta_stages.png)

## Getting Started
Refer to the [Getting Started](https://databrickslabs.github.io/dlt-meta/getting_started)
### Databricks Labs DLT-META CLI lets you run onboard and deploy in interactive python terminal
#### pre-requisites:
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html)
- Python 3.8.0 +
#### Steps:
- ``` git clone dlt-meta ```
- ``` cd dlt-meta ```
- ``` python -m venv .venv ```
- ```source .venv/bin/activate ```
- ``` pip install databricks-sdk ```
- ```databricks labs dlt-meta onboard``` 
- - Above command will prompt you to provide onboarding details. If you have cloned dlt-meta git repo then accept defaults which will launch config from demo folder.

```     Provide onboarding file path (default: demo/conf/onboarding.template): 
        Provide onboarding files local directory (default: demo/): 
        Provide dbfs path (default: dbfs:/dlt-meta_cli_demo): 
        Provide databricks runtime version (default: 14.2.x-scala2.12): 
        Run onboarding with unity catalog enabled?
        [0] False
        [1] True
        Enter a number between 0 and 1: 1
        Provide unity catalog name: ravi_dlt_meta_uc
        Provide dlt meta schema name (default: dlt_meta_dataflowspecs_203b9da04bdc49f78cdc6c379d1c9ead): 
        Provide dlt meta bronze layer schema name (default: dltmeta_bronze_cf5956873137432294892fbb2dc34fdb): 
        Provide dlt meta silver layer schema name (default: dltmeta_silver_5afa2184543342f98f87b30d92b8c76f): 
        Provide dlt meta layer
        [0] bronze
        [1] bronze_silver
        [2] silver
        Enter a number between 0 and 2: 1
        Provide bronze dataflow spec table name (default: bronze_dataflowspec): 
        Provide silver dataflow spec table name (default: silver_dataflowspec): 
        Overwrite dataflow spec?
        [0] False
        [1] True
        Enter a number between 0 and 1: 1
        Provide dataflow spec version (default: v1): 
        Provide environment name (default: prod): prod
        Provide import author name (default: ravi.gawai): 
        Provide cloud provider name
        [0] aws
        [1] azure
        [2] gcp
        Enter a number between 0 and 2: 0
        Do you want to update ws paths, catalog, schema details to your onboarding file?
        [0] False
        [1] True
```
- Goto your databricks workspace and located onboarding job under: Workflow->Jobs runs
- Once onboarding jobs is finished deploy `bronze` and `silver` DLT using below command
- ```databricks labs dlt-meta deploy```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps
- - Bronze DLT
```
    Deploy DLT-META with unity catalog enabled?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide unity catalog name: ravi_dlt_meta_uc
    Deploy DLT-META with serverless?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide dlt meta layer
    [0] bronze
    [1] silver
    Enter a number between 0 and 1: 0
    Provide dlt meta onboard group: A1  
    Provide dlt_meta dataflowspec schema name: dlt_meta_dataflowspecs_203b9da04bdc49f78cdc6c379d1c9ead
    Provide bronze dataflowspec table name (default: bronze_dataflowspec): 
    Provide dlt meta pipeline name (default: dlt_meta_bronze_pipeline_2aee3eb837f3439899eef61b76b80d53): 
    Provide dlt target schema name: dltmeta_bronze_cf5956873137432294892fbb2dc34fdb
```

- Silver DLT
- - ```databricks labs dlt-meta deploy```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps
```
    Deploy DLT-META with unity catalog enabled?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide unity catalog name: ravi_dlt_meta_uc
    Deploy DLT-META with serverless?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide dlt meta layer
    [0] bronze
    [1] silver
    Enter a number between 0 and 1: 1
    Provide dlt meta onboard group: A1
    Provide dlt_meta dataflowspec schema name: dlt_meta_dataflowspecs_203b9da04bdc49f78cdc6c379d1c9ead
    Provide silver dataflowspec table name (default: silver_dataflowspec): 
    Provide dlt meta pipeline name (default: dlt_meta_silver_pipeline_2147545f9b6b4a8a834f62e873fa1364): 
    Provide dlt target schema name: dltmeta_silver_5afa2184543342f98f87b30d92b8c76f
```
## More questions
Refer to the [FAQ](https://databrickslabs.github.io/dlt-meta/faq)
and DLT-META [documentation](https://databrickslabs.github.io/dlt-meta/)

# Project Support
Please note that all projects released under [`Databricks Labs`](https://www.databricks.com/learn/labs)
 are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements 
(SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket 
relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as issues on the Github Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.