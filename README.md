# DLT-META

<!-- Top bar will be removed from PyPi packaged versions -->
<!-- Dont remove: exclude package -->

[Documentation](https://databrickslabs.github.io/dlt-meta/) |
[Release Notes](CHANGELOG.md) |
[Examples](https://github.com/databrickslabs/dlt-meta/tree/main/examples)

<!-- Dont remove: end exclude package -->

---

[![Documentation](https://img.shields.io/badge/docs-passing-green)](https://databrickslabs.github.io/dlt-meta/) [![PyPI](https://img.shields.io/badge/pypi-v0.0.9-green)](https://pypi.org/project/dlt-meta/) [![Build](https://img.shields.io/github/workflow/status/databrickslabs/dlt-meta/build/main)](https://github.com/databrickslabs/dlt-meta/actions/workflows/onpush.yml) [![Coverage](https://img.shields.io/codecov/c/github/databrickslabs/dlt-meta)](https://codecov.io/gh/databrickslabs/dlt-meta) [![Style](https://img.shields.io/badge/code%20style-flake8-blue)](https://github.com/PyCQA/flake8) [![PyPI Downloads](https://static.pepy.tech/badge/dlt-meta/month)](https://pepy.tech/projects/dlt-meta)

---


# Project Overview
`DLT-META` is a metadata-driven framework designed to work with [Lakeflow Declarative Pipelines](https://www.databricks.com/product/data-engineering/lakeflow-declarative-pipelines). This framework enables the automation of bronze and silver data pipelines by leveraging metadata recorded in an onboarding JSON file. This file, known as the Dataflowspec, serves as the data flow specification, detailing the source and target metadata required for the pipelines.

In practice, a single generic pipeline reads the Dataflowspec and uses it to orchestrate and run the necessary data processing workloads. This approach streamlines the development and management of data pipelines, allowing for a more efficient and scalable data processing workflow

### Components:

#### Metadata Interface

- Capture input/output metadata in [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.template)
- Capture [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe/customers/bronze_data_quality_expectations.json)
- Capture processing logic as sql in [Silver transformation file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json)

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
| [append_flow](https://docs.databricks.com/en/delta-live-tables/flows.html#use-append-flow-to-write-to-a-streaming-table-from-multiple-source-streams) API support | Bronze layer|
| Liquid cluster support | Bronze, Bronze Quarantine, Silver tables|
| [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/) |  ```databricks labs dlt-meta onboard```, ```databricks labs dlt-meta deploy``` |
| Bronze and Silver pipeline chaining | Deploy dlt-meta pipeline with ```layer=bronze_silver``` option using Direct publishing mode |
| [create_sink](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-sink) API support |Supported formats:external ```delta table```, ```kafka```.Bronze, Silver layers|
| [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) | Supported
| [DLT-META UI](https://github.com/databrickslabs/dlt-meta/tree/main/lakehouse_app#dlt-meta-lakehouse-app-setup) | Uses Databricks Lakehouse DLT-META App

## Getting Started

Refer to the [Getting Started](https://databrickslabs.github.io/dlt-meta/getting_started)

### Databricks Labs DLT-META CLI lets you run onboard and deploy in interactive python terminal

#### pre-requisites:

- Python 3.8.0 +

- Databricks CLI v0.213 or later. See [instructions](https://docs.databricks.com/en/dev-tools/cli/tutorial.html)

- Install Databricks CLI on macOS:
- ![macos_install_databricks](docs/static/images/macos_1_databrickslabsmac_installdatabricks.gif)

- Install Databricks CLI on Windows:
- ![windows_install_databricks.png](docs/static/images/windows_install_databricks.png)

Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

```commandline
databricks auth login --host WORKSPACE_HOST
```

    To enable debug logs, simply add `--debug` flag to any command.

### Installing dlt-meta:

- Install dlt-meta via Databricks CLI:

```commandline
    databricks labs install dlt-meta
```

### Onboard using dlt-meta CLI:

If you want to run existing demo files please follow these steps before running onboard command:

1. Clone dlt-meta:
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
    ```

2. Navigate to project directory:
    ```commandline
    cd dlt-meta
    ```

3. Create Python virtual environment:
    ```commandline
    python -m venv .venv
    ```

4. Activate virtual environment:
    ```commandline
    source .venv/bin/activate
    ```

5. Install required packages:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk
    
    # Development requirements
    pip install delta-spark==3.0.0 pyspark==3.5.5 pytest>=7.0.0 coverage>=7.0.0
    
    # Integration test requirements
    pip install "typer[all]==0.6.1"
    ```

6. Set environment variables:
    ```commandline
    dlt_meta_home=$(pwd)
    export PYTHONPATH=$dlt_meta_home
    ```
7. Run onboarding command:
    ```commandline
    databricks labs dlt-meta onboard
    ```
![onboardingDLTMeta.gif](docs/static/images/onboardingDLTMeta.gif)


Above commands will prompt you to provide onboarding details. If you have cloned dlt-meta git repo then accept defaults which will launch config from demo folder.
![onboardingDLTMeta_2.gif](docs/static/images/onboardingDLTMeta_2.gif)


- Goto your databricks workspace and located onboarding job under: Workflow->Jobs runs

### depoly using dlt-meta CLI:

- Once onboarding jobs is finished deploy `bronze` and `silver` DLT using below command
- ```commandline
     databricks labs dlt-meta deploy
  ```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps
- - Bronze DLT

![deployingDLTMeta_bronze.gif](docs/static/images/deployingDLTMeta_bronze.gif)


- Silver DLT
- - ```commandline
       databricks labs dlt-meta deploy
    ```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps

![deployingDLTMeta_silver.gif](docs/static/images/deployingDLTMeta_silver.gif)


## More questions

Refer to the [FAQ](https://databrickslabs.github.io/dlt-meta/faq)
and DLT-META [documentation](https://databrickslabs.github.io/dlt-meta/)

# Project Support

Please note that all projects released under [`Databricks Labs`](https://www.databricks.com/learn/labs)
are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements
(SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket
relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as issues on the Github Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.
