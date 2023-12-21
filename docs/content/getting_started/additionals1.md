---
title: "Additionals"
date: 2021-08-04T14:25:26-04:00
weight: 21
draft: false
---
#### [DLT-META](https://github.com/databrickslabs/dlt-meta) DEMO's 
 1. [DAIS 2023 DEMO](#dais-2023-demo): Showcases DLT-META's capabilities of creating Bronze and Silver DLT pipelines with initial and incremental mode automatically.
 2. [Databricks Techsummit Demo](#databricks-tech-summit-fy2024-demo): 100s of data sources ingestion in bronze and silver DLT pipelines automatically.


##### DAIS 2023 DEMO
This Demo launches Bronze and Silver DLT pipleines with following activities:
- Customer and Transactions feeds for initial load
- Adds new feeds Product and Stores to existing Bronze and Silver DLT pipelines with metadata changes.
- Runs Bronze and Silver DLT for incremental load for CDC events

##### Steps:
1. Launch Terminal/Command promt 

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. ```git clone https://github.com/databrickslabs/dlt-meta.git ```

4. ```cd dlt-meta```

5. Set python environment variable into terminal
    ```
        export PYTHONPATH=<<local dlt-meta path>>
    ```

6. Run the command ```python demo/launch_dais_demo.py --username=<<your databricks username>> --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --cloud_provider_name=aws --dbr_version=13.3.x-scala2.12 --dbfs_path=dbfs:/dais-dlt-meta-demo-automated_new```
    - cloud_provider_name : aws or azure or gcp
    - db_version : Databricks Runtime Version
    - dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines
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

##### Databricks Tech Summit FY2024 DEMO:
This demo will launch auto generated tables(100s) inside single bronze and silver DLT pipeline using dlt-meta.

1. Launch Terminal/Command promt 

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

3. ```git clone https://github.com/databrickslabs/dlt-meta.git ```

4. ```cd dlt-meta```

5. Set python environment variable into terminal
    ```
        export PYTHONPATH=<<local dlt-meta path>>
    ```

6. Run the command ```python demo/launch_techsummit_demo.py --username=ravi.gawai@databricks.com --source=cloudfiles --cloud_provider_name=aws --dbr_version=13.3.x-scala2.12 --dbfs_path=dbfs:/techsummit-dlt-meta-demo-automated ```
    - cloud_provider_name : aws or azure or gcp
    - db_version : Databricks Runtime Version
    - dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines
    - you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

    - - 6a. Databricks Workspace URL:
        - Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com. To get your workspace URL, see Workspace instance names, URLs, and IDs.

    - - 6b. Token:
        - In your Databricks workspace, click your Databricks username in the top bar, and then select User Settings from the drop down.

        - On the Access tokens tab, click Generate new token.

        - (Optional) Enter a comment that helps you to identify this token in the future, and change the token’s default lifetime of 90 days. To create a token with no lifetime (not recommended), leave the Lifetime (days) box empty (blank).

        - Click Generate.

        - Copy the displayed token

        - Paste to command prompt