---
title: "Metadata Preparation"
date: 2021-08-04T14:25:26-04:00
weight: 15
draft: false
---

1. Create ```onboarding.json``` metadata file and save to s3/adls/dbfs e.g.[onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json)
2. Create ```silver_transformations.json``` and save to s3/adls/dbfs e.g [Silver transformation file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json)
3. Create data quality rules json and store to s3/adls/dbfs e.g [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe/customers/bronze_data_quality_expectations.json)


### Onboarding File structure
`env` is your environment placeholder e.g `dev`, `prod`, `stag`
| Field | Description |
| :-----------: | :----------- |
| data_flow_id | This is unique identifer for pipeline |
| data_flow_group | This is group identifer for launching multiple pipelines under single DLT |
| source_format | 	Source format e.g `cloudFiles`, `eventhub`, `kafka`, `delta` |
| source_details | This map Type captures all source details for cloudfiles = `source_schema_path`, `source_path_{env}`, `source_database` and for eventhub= `source_schema_path` , `eventhub.accessKeyName`, `eventhub.name` , `eventhub.secretsScopeName` , `kafka.sasl.mechanism`, `kafka.security.protocol`, `eventhub.namespace`, `eventhub.port`. For Source schema file spark DDL schema format parsing is supported <br> In case of custom schema format then write schema parsing function `bronze_schema_mapper(schema_file_path, spark):Schema` and provide to `OnboardDataflowspec` initialization <br> .e.g `onboardDataFlowSpecs = OnboardDataflowspec(spark, dict_obj,bronze_schema_mapper).onboardDataFlowSpecs()`   |         
| bronze_database_{env} | 	Delta lake bronze database name. |
| bronze_table | 	Delta lake bronze table name |
| bronze_reader_options | 	Reader options which can be provided to spark reader <br> e.g multiline=true,header=true in json format |
| bronze_parition_columns | 	Bronze table partition cols list |
| bronze_cdc_apply_changes | 	Bronze cdc apply changes Json |
| bronze_table_path_{env} | 	Bronze table storage path.|
| bronze_table_properties | 	DLT table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| bronze_data_quality_expectations_json | 	Bronze table data quality expectations |
| bronze_database_quarantine_{env} | 	Bronze database for quarantine data which fails expectations. |
| bronze_quarantine_table	Bronze |  Table for quarantine data which fails expectations |
| bronze_quarantine_table_path_{env} | 	Bronze database for quarantine data which fails expectations. |
| bronze_quarantine_table_partitions | 	Bronze quarantine tables partition cols |
| bronze_quarantine_table_properties | 	DLT table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| silver_database_{env} | 	Silver database name. |
| silver_table | 	Silver table name |
| silver_partition_columns | 	Silver table partition columns list |
| silver_cdc_apply_changes | 	Silver cdc apply changes Json |
| silver_table_path_{env} | 	Silver table storage path. |
| silver_table_properties | 	DLT table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false"}` |
| silver_transformation_json | 	Silver table sql transformation json path |


### Data Quality Rules File Structure
| Field | Description |
| :-----------: | :----------- |
| expect | Specify multiple data quality sql for each field when records that fail validation should be included in the target dataset| 
| expect_or_fail  | Specify multiple data quality sql for each field when records that fail validation should halt pipeline execution |
| expect_or_drop  | Specify multiple data quality sql for each field when records that fail validation should be dropped from the target dataset |
| expect_or_quarantine  | Specify multiple data quality sql for each field when records that fails validation will be dropped from main table and inserted into quarantine table specified in dataflowspec  |

### Silver transformation File Structure
| Field | Description |
| :-----------: | :----------- |
| target_table | Specify target table name : Type String | 
| target_partition_cols  | Specify partition columns : Type Array |
| select_exp | Specify SQL expressions : Type Array | 
| where_clause  | Specify filter conditions if you want to prevent certains records from main input : Type Array |