---
title: "Tech Summit DEMO"
date: 2021-08-04T14:25:26-04:00
weight: 22
draft: false
---

### Databricks Tech Summit FY2024 DEMO:
This demo will launch auto generated tables(100s) inside single bronze and silver DLT pipeline using dlt-meta.

1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:
    
    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

3. ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git 
    ```

4. ```commandline
    cd dlt-meta
    ```

5. Set python environment variable into terminal
    ```commandline
    dlt_meta_home=$(pwd)
    ```
    ```commandline
    export PYTHONPATH=$dlt_meta_home
    ```

6. Run the command ```python demo/launch_techsummit_demo.py --uc_catalog_name=<<Unity Catalog name>> --cloud_provider_name=aws ```
    - uc_catalog_name : Unity Catalog name
    - cloud_provider_name : aws or azure
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

    ![tech_summit_demo.png](/images/tech_summit_demo.png)
