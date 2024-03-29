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
1. ``` git clone https://github.com/databrickslabs/dlt-meta.git ```
2. ``` cd dlt-meta ```
3. ``` python -m venv .venv ```
4. ```source .venv/bin/activate ```
5. ``` pip install databricks-sdk ```

![onboardingDLTMeta.gif](docs/static/images/onboardingDLTMeta.gif)

##### run dlt-meta cli command: 
 ```shell 
    databricks labs dlt-meta onboard
``` 
-  Above command will prompt you to provide onboarding details.
- If you have cloned dlt-meta git repo then accepting defaults will launch config from [demo/conf](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf) folder.
- You can create onboarding files e.g onboarding.json, data quality and silver transformations and put it in conf folder as show in [demo/conf](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf)

![onboardingDLTMeta_2.gif](docs/static/images/onboardingDLTMeta_2.gif)

- Goto your databricks workspace and located onboarding job under: Workflow->Jobs runs
