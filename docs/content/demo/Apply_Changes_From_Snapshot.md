---
title: "Silver Fanout Demo"
date: 2024-10-04T14:25:26-04:00
weight: 26
draft: false
---

### Apply Changes From Snapshot Demo
  - This demo will perform following steps
    - Showcase onboarding process for apply changes from snapshot pattern
    - Run onboarding for the bronze stores and products tables, which contains data snapshot data in csv files.
    - Run Bronze DLT to load initial snapshot (LOAD_1.csv)
    - Upload incremental snapshot LOAD_2.csv version=2 for stores and product
    - Run Bronze DLT to load incremental snapshot (LOAD_2.csv). Stores is scd_type=2 so updated records will expired and added new records with version_number. Products is scd_type=1 so in case records missing for scd_type=1 will be deleted.
    - Upload incremental snapshot LOAD_3.csv version=3 for stores and product
    - Run Bronze DLT to load incremental snapshot (LOAD_3.csv). Stores is scd_type=2 so updated records will expired and added new records with version_number. Products is scd_type=1 so in case records missing for scd_type=1 will be deleted.


### Steps:
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

7. Run the command:
    ```commandline
    python demo/launch_acfs_demo.py --uc_catalog_name=<<uc catalog name>>
    ```
    - uc_catalog_name : Unity catalog name
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token.

    
    ![acfs.png](/images/acfs.png)