---
title: "Tech Summit DEMO"
date: 2021-08-04T14:25:26-04:00
weight: 22
draft: false
---

### Databricks Tech Summit FY2024 DEMO:
This demo will launch auto generated tables(100s) inside single bronze and silver Lakeflow Declarative Pipeline using dlt-meta.

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
    python demo/launch_techsummit_demo.py --uc_catalog_name=<<Unity Catalog name>> --cloud_provider_name=aws
    ```
    - uc_catalog_name : Unity Catalog name
    - cloud_provider_name : aws or azure
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

    ![tech_summit_demo.png](/images/tech_summit_demo.png)
