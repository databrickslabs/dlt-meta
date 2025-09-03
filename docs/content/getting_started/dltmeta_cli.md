---
title: "DLT-META CLI"
date: 2021-08-04T14:25:26-04:00
weight: 7
draft: false
---

### Prerequisites:
- Python 3.8.0 +
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html)

### Steps:
1. Install and authenticate Databricks CLI:
    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

2. Install dlt-meta via Databricks CLI:
    ```commandline
    databricks labs install dlt-meta
    ```

3. Clone dlt-meta repository:
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
    ```

4. Navigate to project directory:
    ```commandline
    cd dlt-meta
    ```

5. Create Python virtual environment:
    ```commandline
    python -m venv .venv
    ```

6. Activate virtual environment:
    ```commandline
    source .venv/bin/activate
    ```

7. Install required packages:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk

    # Development requirements
    pip install flake8==6.0 delta-spark==3.0.0 pytest>=7.0.0 coverage>=7.0.0 pyspark==3.5.5

    # Integration test requirements
    pip install "typer[all]==0.6.1"
    ```

8. Set environment variables:
    ```commandline
    dlt_meta_home=$(pwd)
    export PYTHONPATH=$dlt_meta_home
    ```

![onboardingDLTMeta.gif](/images/onboardingDLTMeta.gif)

## OnboardJob 
### Run Onboarding using dlt-meta cli command: 
 ```shell 
    databricks labs dlt-meta onboard
``` 
- The command will prompt you to provide onboarding details. If you have cloned the dlt-meta repository, you can accept the default values which will use the configuration from the demo folder.

![onboardingDLTMeta_2.gif](/images/onboardingDLTMeta_2.gif)

- Above onboard cli command will:
   1. Push code and data to your Databricks workspace
   2. Create an onboarding job
   3. Display a success message: ```Job created successfully. job_id={job_id}, url=https://{databricks workspace url}/jobs/{job_id}```
   4. Job URL will automatically open in your default browser.


- Once onboarding jobs is finished deploy `bronze` and `silver` Lakeflow Declarative Pipeline using below command

## DLT-META Lakeflow Declarative Pipeline: 

#### Deploy ```Bronze``` and ```Silver``` layer into single pipeline
 ```shell 
        databricks labs dlt-meta deploy
   ```
- Above command will prompt you to provide pipeline details. Please provide respective details for schema which you provided in above steps

![deployingDLTMeta_bronze_silver.gif](/images/deployingDLTMeta_bronze_silver.gif)

- Above deploy cli command will:
   1. Deploy Lakeflow Declarative pipeline with dlt-meta configuration like ```layer```, ```group```, ```dataflowSpec table details``` etc to your databricks workspace
   2. Display message: ```dlt-meta pipeline={pipeline_id} created and launched with update_id={pipeline_update_id}, url=https://{databricks workspace url}/#joblist/pipelines/{pipeline_id}```
   3. Pipline URL will automatically open in your defaul browser.


