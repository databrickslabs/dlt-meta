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
 git clone https://github.com/databrickslabs/dlt-meta.git 
 cd dlt-meta
 python -m venv .venv 
 source .venv/bin/activate 
 pip install databricks-sdk 
 databricks labs dlt-meta onboard
 ```
 
![onboardingDLTMeta.gif](/images/onboardingDLTMeta.gif)

- Once onboarding jobs is finished deploy `bronze` and `silver` DLT using below command
#### Deploy Bronze DLT
 ```shell 
        databricks labs dlt-meta deploy
   ```
- Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps

![deployingDLTMeta_bronze.gif](/images/deployingDLTMeta_bronze.gif)

#### Deploy Silver DLT
 ```shell 
        databricks labs dlt-meta deploy
```
- - Above command will prompt you to provide dlt details. Please provide respective details for schema which you provided in above steps

![deployingDLTMeta_silver.gif](/images/deployingDLTMeta_silver.gif)