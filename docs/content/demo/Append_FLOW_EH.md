---
title: "Append FLOW Eventhub Demo"
date: 2021-08-04T14:25:26-04:00
weight: 24
draft: false
---

### Append FLOW Autoloader Demo:
- Read from different eventhub topics and write to same target tables using [dlt.append_flow](https://docs.databricks.com/en/delta-live-tables/flows.html#append-flows) API

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
    ```
6. Eventhub
- Needs eventhub instance running
- Need two eventhub topics first for main feed (eventhub_name) and second for append flow feed (eventhub_name_append_flow)
- Create databricks secrets scope for eventhub keys
    - ```
            commandline databricks secrets create-scope eventhubs_dltmeta_creds
        ```
    - ```commandline 
            databricks secrets put-secret --json '{
                "scope": "eventhubs_dltmeta_creds",
                "key": "RootManageSharedAccessKey",
                "string_value": "<<value>>"
                }' 
        ```
- Create databricks secrets to store producer and consumer keys using the scope created in step 2 

- Following are the mandatory arguments for running EventHubs demo
    - cloud_provider_name: Cloud provider name e.g. aws or azure 
    - dbr_version:  Databricks Runtime Version e.g. 15.3.x-scala2.12
    - uc_catalog_name : unity catalog name e.g. dlt_meta_uc
    - dbfs_path: Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines e.g. dbfs:/tmp/DLT-META/demo/ 
    - eventhub_namespace: Eventhub namespace e.g. dltmeta
    - eventhub_name : Primary Eventhubname e.g. dltmeta_demo
    - eventhub_name_append_flow: Secondary eventhub name for appendflow feed e.g. dltmeta_demo_af
    - eventhub_producer_accesskey_name: Producer databricks access keyname e.g. RootManageSharedAccessKey
    - eventhub_consumer_accesskey_name: Consumer databricks access keyname e.g. RootManageSharedAccessKey
    - eventhub_secrets_scope_name: Databricks secret scope name e.g. eventhubs_dltmeta_creds
    - eventhub_port: Eventhub port

7. ```commandline 
    python demo/launch_af_eventhub_demo.py --cloud_provider_name=aws --uc_catalog_name=dlt_meta_uc --eventhub_name=dltmeta_demo --eventhub_name_append_flow=dltmeta_demo_af --eventhub_secrets_scope_name=dltmeta_eventhub_creds --eventhub_namespace=dltmeta --eventhub_port=9093 --eventhub_producer_accesskey_name=RootManageSharedAccessKey --eventhub_consumer_accesskey_name=RootManageSharedAccessKey --eventhub_accesskey_secret_name=RootManageSharedAccessKey
    ```

![af_eh_demo.png](/images/af_eh_demo.png)
