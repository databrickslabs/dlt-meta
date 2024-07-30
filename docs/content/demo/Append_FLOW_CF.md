---
title: "Append FLOW Autoloader Demo"
date: 2021-08-04T14:25:26-04:00
weight: 23
draft: false
---

### Append FLOW Autoloader Demo:
This demo will perform following tasks:
- Read from different source paths using autoloader and write to same target using [dlt.append_flow](https://docs.databricks.com/en/delta-live-tables/flows.html#append-flows) API
- Read from different delta tables and write to same silver table using append_flow API
- Add file_name and file_path to target bronze table for autoloader source using [File metadata column](https://docs.databricks.com/en/ingestion/file-metadata-column.html)
## Append flow with autoloader

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

6. ```commandline
    python demo/launch_af_cloudfiles_demo.py --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --dbfs_path=dbfs:/tmp/DLT-META/demo/ --uc_catalog_name=ravi_dlt_meta_uc
    ```

- cloud_provider_name : aws or azure or gcp
- db_version : Databricks Runtime Version
- dbfs_path : Path on your Databricks workspace where demo will be copied for launching DLT-META Pipelines
- uc_catalog_name: Unity catalog name
- you can provide `--profile=databricks_profile name` in case you already have databricks cli otherwise command prompt will ask host and token

![af_am_demo.png](docs/static/images/af_am_demo.png)