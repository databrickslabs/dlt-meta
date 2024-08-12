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
        <img src="https://img.shields.io/badge/PYPI-v%200.0.9-green?style=for-the-badge" alt="Latest Python Release"/>
    </a>
    <a href="https://github.com/databrickslabs/dlt-meta/actions/workflows/onpush.yml">
        <img src="https://img.shields.io/github/workflow/status/databrickslabs/dlt-meta/build/main?style=for-the-badge"
             alt="GitHub Workflow Status (branch)"/>
    </a>
    <a href="https://codecov.io/gh/databrickslabs/dlt-meta">
        <img src="https://img.shields.io/codecov/c/github/databrickslabs/dlt-meta?style=for-the-badge&amp;token=2CxLj3YBam"
             alt="codecov"/>
    </a>
    <a href="https://pypistats.org/packages/dl-meta">
        <img src="https://img.shields.io/pypi/dm/dlt-meta?style=for-the-badge" alt="downloads"/>
    </a>
    <a href="https://github.com/PyCQA/flake8">
        <img src="https://img.shields.io/badge/FLAKE8-FLAKE8-lightgrey?style=for-the-badge"
             alt="We use flake8 for formatting"/>
    </a>
</p>

[![lines of code](https://tokei.rs/b1/github/databrickslabs/dlt-meta)](<[https://codecov.io/github/databrickslabs/dlt-meta](https://github.com/databrickslabs/dlt-meta)>)

---

# Project Overview
`DLT-META` is a metadata-driven framework designed to work with [Delta Live Tables](https://www.databricks.com/product/delta-live-tables). This framework enables the automation of bronze and silver data pipelines by leveraging metadata recorded in an onboarding JSON file. This file, known as the Dataflowspec, serves as the data flow specification, detailing the source and target metadata required for the pipelines.

In practice, a single generic DLT pipeline reads the Dataflowspec and uses it to orchestrate and run the necessary data processing workloads. This approach streamlines the development and management of data pipelines, allowing for a more efficient and scalable data processing workflow

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

```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
```

```commandline
    cd dlt-meta
```

```commandline
    python -m venv .venv
```

```commandline
    source .venv/bin/activate
```

```commandline
    pip install databricks-sdk
```

```commandline
    dlt_meta_home=$(pwd)
```

```commandline
    export PYTHONPATH=$dlt_meta_home
```
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
