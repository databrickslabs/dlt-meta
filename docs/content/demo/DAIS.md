---
title: "DAIS DEMO"
date: 2021-08-04T14:25:26-04:00
weight: 21
draft: false
---

### DAIS 2023 DEMO:
#### [DAIS 2023 Session Recording](https://www.youtube.com/watch?v=WYv5haxLlfA)

This demo showcases DLT-META's capabilities of creating Bronze and Silver Lakeflow Declarative pipeline with initial and incremental mode automatically.
- Customer and Transactions feeds for initial load
- Adds new feeds Product and Stores to existing Bronze and Silver Lakeflow Declarative pipeline with metadata changes.
- Runs Bronze and Silver Lakeflow Declarative Pipeline for incremental load for CDC events

#### Steps to launch DAIS demo in your Databricks workspace:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:
    
    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

3. Install Python package requirements:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk

    # Development requirements
    pip install flake8==6.0 delta-spark==3.0.0 pytest>=7.0.0 coverage>=7.0.0 pyspark==3.5.5
    ```

4. Clone dlt-meta:
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git 
    ```

5. Navigate to project directory:
    ```commandline
    cd dlt-meta
    ```

6. Set python environment variable into terminal
    ```commandline
    dlt_meta_home=$(pwd)
    ```
    ```commandline
    export PYTHONPATH=$dlt_meta_home
    ```

7. Run the command:
    ```commandline 
    python demo/launch_dais_demo.py --uc_catalog_name=<<uc catalog name>> --cloud_provider_name=<<>>
    ```
    - uc_catalog_name : unit catalog name
    - cloud_provider_name : aws or azure or gcp
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token.


    ![dais_demo.png](/images/dais_demo.png)


