 # [DLT-META](https://github.com/databrickslabs/dlt-meta) DEMO's 
 1. [DAIS 2023 DEMO](#dais-2023-demo): Showcases DLT-META's capabilities of creating Bronze and Silver DLT pipelines with initial and incremental mode automatically.
 2. [Databricks Techsummit Demo](#databricks-tech-summit-fy2024-demo): 100s of data sources ingestion in bronze and silver DLT pipelines automatically.
 3. [Append FLOW Autoloader Demo](#append-flow-autoloader-file-metadata-demo): Write to same target from multiple sources using append_flow and adding file metadata using 
 autoloaders _metadata column
3. [Append FLOW Eventhub Demo](#append-flow-eventhub-demo): Write to same target from multiple sources using append_flow and adding file metadata using 
 autoloaders _metadata column


# DAIS 2023 DEMO 
## [DAIS 2023 Session Recording](https://www.youtube.com/watch?v=WYv5haxLlfA)
This Demo launches Bronze and Silver DLT pipelines with following activities:
- Customer and Transactions feeds for initial load
- Adds new feeds Product and Stores to existing Bronze and Silver DLT pipelines with metadata changes.
- Runs Bronze and Silver DLT for incremental load for CDC events

### Steps:
1. Launch Terminal/Command prompt 

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

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

6. Run the command ```python demo/launch_dais_demo.py --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --dbfs_path=dbfs:/dais-dlt-meta-demo-automated_new```
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

# Databricks Tech Summit FY2024 DEMO:
This demo will launch auto generated tables(100s) inside single bronze and silver DLT pipeline using dlt-meta.

1. Launch Terminal/Command promt 

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

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

6. Run the command 
    ```commandline 
    python demo/launch_techsummit_demo.py --source=cloudfiles --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --dbfs_path=dbfs:/techsummit-dlt-meta-demo-automated 
    ```
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

# Append Flow Autoloader demo:
This demo will 
- Read from different source paths using autoloader and write to same target using append_flow API
- Read from different delta tables and write to same silver table using append_flow API
- Add file_name and file_path to target bronze table for autoloader source
## Append flow with autoloader

```commandline
python demo/launch_af_cloudfiles_demo.py --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --dbfs_path=dbfs:/tmp/DLT-META/demo/ --uc_catalog_name=ravi_dlt_meta_uc
```
- cloud_provider_name : aws or azure or gcp
- db_version : Databricks Runtime Version
- dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines
- uc_catalog_name: Unity catalog name
- you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

# Append Flow Eventhub demo:
- Read from different eventhub topics and write to same target tables using append_flow API
- Prerequisite:
- - Needs eventhub instance running
- - Create databricks secrets scope for eventhub keys
    - - ```
            commandline databricks secrets create-scope eventhubs_creds
        ```
    - - ```commandline 
            databricks secrets put-secret --json '{
                "scope": "eventhubs_creds",
                "key": "RootManageSharedAccessKey",
                "string_value": "<<value>>"
    }' ```
- - Create databricks secrets to store producer and consumer keys using the scope created in step 2 

    - - Following are the mandatory arguments for running EventHubs demo
        1. Provide your eventhub topic : --eventhub_name
        2. Provide eventhub namespace : --eventhub_namespace
        3. Provide eventhub port : --eventhub_port
        4. Provide databricks secret scope name : --eventhub_secrets_scope_name
        5. Provide eventhub producer access key name : --eventhub_producer_accesskey_name
        6. Provide eventhub access key name : --eventhub_consumer_accesskey_name

```commandline 
    python3 demo/launch_af_eventhub_demo.py --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --dbfs_path=dbfs:/tmp/DLT-META/demo/ --uc_catalog_name=ravi_dlt_meta_uc --eventhub_name=dltmeta_int_test --eventhub_name_append_flow=dltmeta_int_test_af --eventhub_secrets_scope_name=dltmeta_eventhub_creds --eventhub_namespace=dltmeta --eventhub_port=9093 --eventhub_producer_accesskey_name=RootManageSharedAccessKey --eventhub_consumer_accesskey_name=RootManageSharedAccessKey --eventhub_accesskey_secret_name=RootManageSharedAccessKey --uc_catalog_name=ravi_dlt_meta_uc
```
