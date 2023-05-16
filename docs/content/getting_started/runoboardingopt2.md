---
title: "Running Onboarding"
date: 2021-08-04T14:25:26-04:00
weight: 18
draft: false
---

#### Option#2: Notebook 
1. Copy below code to databricks notebook cells
```
%pip install dlt-meta
```
```
onboarding_params_map = {
		"database": "dlt_demo",
		"onboarding_file_path": "dbfs:/onboarding_files/users_onboarding.json",
		"bronze_dataflowspec_table": "bronze_dataflowspec_table", 
		"bronze_dataflowspec_path": "dbfs:/onboarding_tables_cdc/bronze",                       
		"silver_dataflowspec_table": "silver_dataflowspec_table",
		"silver_dataflowspec_path": "dbfs:/onboarding_tables_cdc/silver",
		"overwrite": "True",
		"onboard_layer": "bronze_silver",
		"env": "dev",
		"version": "v1",
		"import_author": "Ravi"
} 

from src.onboard_dataflowspec import OnboardDataflowspec
OnboardDataflowspec(spark, onboarding_params_map).onboard_dataflow_specs()

```
2. Specify your onboarding config params in above ```onboarding_params_map```

3. Run notebook cells
