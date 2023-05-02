---
title: "Execution"
date: 2021-08-04T14:26:55-04:00
weight: 10
draft: false
---

**Q. How do I get started ?**

Please refer to the [Getting Started]({{%relref "getting_started/_index.md" %}}) guide

**Q. How do I create metadata DLT-META ?**

DLT-META needs following metadata files:
- [Onboarding File](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json) captures input/output metadata 
- [Data Quality Rules File](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe) captures data quality rules
- [Silver transformation File](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json) captures  processing logic as sql 

**Q. What is DataflowSpecs?**

DLT-META translates input metadata into Delta table as DataflowSpecs


**Q. How many DLT pipelines will be launched using DLT-META?**

DLT-META uses data_flow_group to launch DLT pipelines, so all the tables belongs to same group will be executed under single DLT pipeline. 

**Q. Can we run onboarding for bronze layer only?**

Yes! Remove silver related attributes from onboarding file and call  `onboard_bronze_dataflow_spec()` API from ```OnboardDataflowspec```. Similarly you can run silver layer onboarding separately using `onboard_silver_dataflow_spec()`API from `OnboardDataflowspec` with silver parameters included in `onboarding_params_map`

```
onboarding_params_map = {
                      "onboarding_file_path":onboarding_file_path,
                      "database":bronze_database,
                      "env":"dev", 
                      "bronze_dataflowspec_table":"bronze_dataflowspec_tablename",
                      "bronze_dataflowspec_path": bronze_dataflowspec_path,
                      "overwrite":"True",
                      "version":"v1",
                      "import_author":"Ravi",
}
print(onboarding_params_map)

from src.onboard_dataflowspec import OnboardDataflowspec
OnboardDataflowspec(spark, onboarding_params_map).onboard_bronze_dataflow_spec()

```
