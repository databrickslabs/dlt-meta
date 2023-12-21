---
title: "Run Onboarding"
date: 2021-08-04T14:25:26-04:00
weight: 17
draft: false
---

#### Option#1: Databricks Labs CLI 
##### pre-requisites:
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html)
- Python 3.8.0 +
##### Steps:
1. ``` git clone dlt-meta ```
2. ``` cd dlt-meta ```
3. ``` python -m venv .venv ```
4. ```source .venv/bin/activate ```
5. ``` pip install databricks-sdk ```

##### run dlt-meta cli command: 
 ```shell 
    databricks labs dlt-meta onboard
``` 
-  Above command will prompt you to provide onboarding details.
- If you have cloned dlt-meta git repo then accepting defaults will launch config from [demo/conf](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf) folder.
- You can create onboarding files e.g onboarding.json, data quality and silver transformations and put it in conf folder as show in [demo/conf](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf)

```shell
		Provide onboarding file path (default: demo/conf/onboarding.template): 
        Provide onboarding files local directory (default: demo/): 
        Provide dbfs path (default: dbfs:/dlt-meta_cli_demo): 
        Provide databricks runtime version (default: 14.2.x-scala2.12): 
        Run onboarding with unity catalog enabled?
        [0] False
        [1] True
        Enter a number between 0 and 1: 1
        Provide unity catalog name: uc_catalog_name
        Provide dlt meta schema name (default: dlt_meta_dataflowspecs_203b9): 
        Provide dlt meta bronze layer schema name (default: dltmeta_bronze_cf595): 
        Provide dlt meta silver layer schema name (default: dltmeta_silver_5afa2): 
        Provide dlt meta layer
        [0] bronze
        [1] bronze_silver
        [2] silver
        Enter a number between 0 and 2: 1
        Provide bronze dataflow spec table name (default: bronze_dataflowspec): 
        Provide silver dataflow spec table name (default: silver_dataflowspec): 
        Overwrite dataflow spec?
        [0] False
        [1] True
        Enter a number between 0 and 1: 1
        Provide dataflow spec version (default: v1): 
        Provide environment name (default: prod): prod
        Provide import author name (default: ravi.gawai): 
        Provide cloud provider name
        [0] aws
        [1] azure
        [2] gcp
        Enter a number between 0 and 2: 0
        Do you want to update ws paths, catalog, schema details to your onboarding file?
        [0] False
        [1] True
```

- Goto your databricks workspace and located onboarding job under: Workflow->Jobs runs
