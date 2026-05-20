---
title: "Append FLOW Eventhub Demo"
date: 2021-08-04T14:25:26-04:00
weight: 24
draft: false
---

### Append FLOW Autoloader Demo:
- Read from different eventhub topics and write to same target tables using [dp.append_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) API

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
7. Configure Eventhub
- Needs eventhub instance running
- Need two eventhub topics first for main feed (eventhub_name) and second for append flow feed (eventhub_name_append_flow)
- Create databricks secrets scope for eventhub keys
    - ```
            commandline databricks secrets create-scope eventhubs_sdp_meta_creds
        ```
    - ```commandline 
            databricks secrets put-secret --json '{
                "scope": "eventhubs_sdp_meta_creds",
                "key": "RootManageSharedAccessKey",
                "string_value": "<<value>>"
                }' 
        ```
- Create databricks secrets to store producer and consumer keys using the scope created in step 2 

- Following are the mandatory arguments for running EventHubs demo
    - cloud_provider_name: Cloud provider name e.g. aws or azure 
    - dbr_version:  Databricks Runtime Version e.g. 15.3.x-scala2.12
    - uc_catalog_name : unity catalog name e.g. sdp_meta_uc
    - eventhub_namespace: Eventhub namespace e.g. sdp_meta
    - eventhub_name : Primary Eventhubname e.g. sdp_meta_demo
    - eventhub_name_append_flow: Secondary eventhub name for appendflow feed e.g. sdp_meta_demo_af
    - eventhub_producer_accesskey_name: Producer databricks access keyname e.g. RootManageSharedAccessKey
    - eventhub_consumer_accesskey_name: Consumer databricks access keyname e.g. RootManageSharedAccessKey
    - eventhub_secrets_scope_name: Databricks secret scope name e.g. eventhubs_sdp_meta_creds
    - eventhub_port: Eventhub port

8. Run the command:
    ```commandline 
    python demo/launch_af_eventhub_demo.py --cloud_provider_name=aws --uc_catalog_name=sdp_meta_uc --eventhub_name=sdp_meta_demo --eventhub_name_append_flow=sdp_meta_demo_af --eventhub_secrets_scope_name=sdp_meta_eventhub_creds --eventhub_namespace=sdp_meta --eventhub_port=9093 --eventhub_producer_accesskey_name=RootManageSharedAccessKey --eventhub_consumer_accesskey_name=RootManageSharedAccessKey --eventhub_accesskey_secret_name=RootManageSharedAccessKey
    ```

![af_eh_demo.png](/images/af_eh_demo.png)
