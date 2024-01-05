---
title: "Launch Generic DLT pipeline"
date: 2021-08-04T14:25:26-04:00
weight: 20
draft: false
---
## Option#1: Databricks Labs CLI
##### pre-requisites:
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html)
- Python 3.8.0 +
##### Steps:
```shell 
 git clone dlt-meta 
 cd dlt-meta
 python -m venv .venv 
 source .venv/bin/activate 
 pip install databricks-sdk 
 databricks labs dlt-meta onboard
 ```

- Once onboarding jobs is finished deploy `bronze` and `silver` DLT using below command
#### Deploy Bronze DLT
 ```shell 
        databricks labs dlt-meta deploy
   ```
- Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps
```shell
    Deploy DLT-META with unity catalog enabled?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide unity catalog name: uc_catalog_name
    Deploy DLT-META with serverless?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide dlt meta layer
    [0] bronze
    [1] silver
    Enter a number between 0 and 1: 0
    Provide dlt meta onboard group: A1  
    Provide dlt_meta dataflowspec schema name: dlt_meta_dataflowspecs_203b9
    Provide bronze dataflowspec table name (default: bronze_dataflowspec): 
    Provide dlt meta pipeline name (default: dlt_meta_bronze_pipeline_2aee): 
    Provide dlt target schema name: dltmeta_bronze_cf595
```

#### Deploy Silver DLT
 ```shell 
        databricks labs dlt-meta deploy
```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps
```shell
    Deploy DLT-META with unity catalog enabled?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide unity catalog name: uc_catalog_name
    Deploy DLT-META with serverless?
    [0] False
    [1] True
    Enter a number between 0 and 1: 1
    Provide dlt meta layer
    [0] bronze
    [1] silver
    Enter a number between 0 and 1: 1
    Provide dlt meta onboard group: A1
    Provide dlt_meta dataflowspec schema name: dlt_meta_dataflowspecs_203b9
    Provide silver dataflowspec table name (default: silver_dataflowspec): 
    Provide dlt meta pipeline name (default: dlt_meta_silver_pipeline_21475): 
    Provide dlt target schema name: dltmeta_silver_5afa2
```