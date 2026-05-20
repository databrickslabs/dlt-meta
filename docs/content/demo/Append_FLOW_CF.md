---
title: "Append FLOW Autoloader Demo"
date: 2021-08-04T14:25:26-04:00
weight: 23
draft: false
---

### Append FLOW Autoloader Demo:
This demo will perform following tasks:
- Read from different source paths using autoloader and write to same target using [dp.append_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) API
- Read from different delta tables and write to same silver table using append_flow API
- Add file_name and file_path to target bronze table for autoloader source using [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
## Append flow with autoloader

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

4. Clone sdp-meta:
    ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git 
    ```

5. Navigate to project directory:
    ```commandline
    cd sdp-meta
    ```

6. Set python environment variable into terminal
    ```commandline
    sdp_meta_home=$(pwd)
    ```

    ```commandline
    export PYTHONPATH=$sdp_meta_home
    ```

7. Run the command:
    ```commandline
    python demo/launch_af_cloudfiles_demo.py --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --uc_catalog_name=sdp_meta_uc
    ```

- cloud_provider_name : aws or azure or gcp
- db_version : Databricks Runtime Version
- uc_catalog_name: Unity catalog name
- you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

![af_am_demo.png](/images/af_am_demo.png)