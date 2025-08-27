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
-  Above command will prompt you to provide onboarding details.
- If you have cloned dlt-meta git repo then accepting defaults will launch config from [demo/conf](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf) folder.
- You can create onboarding files e.g onboarding.json, data quality and silver transformations and put it in conf folder as show in [demo/conf](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf)

![onboardingDLTMeta_2.gif](/images/onboardingDLTMeta_2.gif)

![onboardingDLTMeta.gif](/images/onboardingDLTMeta.gif)

- Once onboarding jobs is finished deploy `bronze` and `silver` DLT using below command

## Dataflow DLT Pipeline: 

#### Deploy Bronze DLT
 ```shell 
        databricks labs dlt-meta deploy
   ```
- Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps

![deployingDLTMeta_bronze.gif](/images/deployingDLTMeta_bronze.gif)

#### Deploy Silver DLT
 ```shell 
        databricks labs dlt-meta deploy
```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps

![deployingDLTMeta_silver.gif](/images/deployingDLTMeta_silver.gif)

- Goto your databricks workspace and located onboarding job under: Workflow->Jobs runs
