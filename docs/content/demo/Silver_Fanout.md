---
title: "Silver Fanout Demo"
date: 2021-08-04T14:25:26-04:00
weight: 25
draft: false
---

### Silver Fanout Demo
  - This demo will perform following steps
    - Showcase onboarding process for silver fanout pattern
    - Run onboarding for the bronze cars table, which contains data from various countries.
    - Run onboarding for the silver tables, which have a `where_clause` based on the country condition in [silver_transformations_cars.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/silver_transformations_cars.json).
    - Run Bronze for cars tables
    - Run onboarding for the silver tables, fanning out from the bronze cars tables to country-specific tables such as cars_usa, cars_uk, cars_germany, and cars_japan.    

### Steps:
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

6. ```commandline
    python demo/launch_silver_fanout_demo.py --uc_catalog_name=<<uc catalog name>> --cloud_provider_name=aws
    ```
    - uc_catalog_name : aws or azure
    - cloud_provider_name : aws or azure
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token.

    - - 6a. Databricks Workspace URL:
    - - Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com. To get your workspace URL, see Workspace instance names, URLs, and IDs.

    - - 6b. Token:
        - In your Databricks workspace, click your Databricks username in the top bar, and then select User Settings from the drop down.

        - On the Access tokens tab, click Generate new token.

        - (Optional) Enter a comment that helps you to identify this token in the future, and change the token’s default lifetime of 90 days. To create a token with no lifetime (not recommended), leave the Lifetime (days) box empty (blank).

        - Click Generate.

        - Copy the displayed token

        - Paste to command prompt

    ![silver_fanout_workflow.png](/images/silver_fanout_workflow.png)
    
    ![silver_fanout_dlt.png](/images/silver_fanout_dlt.png)