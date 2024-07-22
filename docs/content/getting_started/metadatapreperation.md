---
title: "Metadata Preparation"
date: 2021-08-04T14:25:26-04:00
weight: 6
draft: false
---


### Directory structure
```
conf/
    onboarding.json
    silver_transformations.json
    dqe/
        bronze_data_quality_expectations.json
```

1. Create [onboarding.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/onboarding.template)
2. Create [silver_transformations.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/silver_transformations.json)
3. Create data quality rules json's for each entity e.g. [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/dqe/)

The `onboarding.json` file contains links to [silver_transformations.json](https://github.com/databrickslabs/dlt-meta/blob/3555aaa798881a9cfa65f89599f83d22d245d3c8/demo/conf/onboarding.template#L41C1-L42C1) and data quality expectation files [dqe](https://github.com/databrickslabs/dlt-meta/blob/3555aaa798881a9cfa65f89599f83d22d245d3c8/demo/conf/onboarding.template#L42).

### onboarding.json File structure: Examples( [Autoloader](https://github.com/databrickslabs/dlt-meta/blob/main/examples/cloudfiles-onboarding.template), [Eventhub](https://github.com/databrickslabs/dlt-meta/blob/main/examples/eventhub-onboarding.template), [Kafka](https://github.com/databrickslabs/dlt-meta/blob/main/examples/kafka-onboarding.template) )
`env` is your environment placeholder e.g `dev`, `prod`, `stag`
| Field | Description |
| :-----------: | :----------- |
| data_flow_id | This is unique identifier for pipeline |
| data_flow_group | This is group identifier for launching multiple pipelines under single DLT |
| source_format | Source format e.g `cloudFiles`, `eventhub`, `kafka`, `delta` |
| source_details | This map Type captures all source details for cloudfiles = `source_schema_path`, `source_path_{env}`, `source_database`, `source_metadata` For eventhub= `source_schema_path` , `eventhub.accessKeyName`, `eventhub.accessKeySecretName`, `eventhub.name` , `eventhub.secretsScopeName` , `kafka.sasl.mechanism`, `kafka.security.protocol`, `eventhub.namespace`, `eventhub.port`. For Source schema file spark DDL schema format parsing is supported <br> In case of custom schema format then write schema parsing function `bronze_schema_mapper(schema_file_path, spark):Schema` and provide to `OnboardDataflowspec` initialization <br> .e.g `onboardDataFlowSpecs = OnboardDataflowspec(spark, dict_obj,bronze_schema_mapper).onboardDataFlowSpecs()`   |         
| bronze_database_{env} | Delta lake bronze database name. |
| bronze_table | Delta lake bronze table name |
| bronze_reader_options | Reader options which can be provided to spark reader <br> e.g multiline=true,header=true in json format |
| bronze_parition_columns | Bronze table partition cols list |
| bronze_cdc_apply_changes | Bronze cdc apply changes Json |
| bronze_table_path_{env} | Bronze table storage path.|
| bronze_table_properties | DLT table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| bronze_data_quality_expectations_json | Bronze table data quality expectations |
| bronze_database_quarantine_{env} | Bronze database for quarantine data which fails expectations. |
| bronze_quarantine_table	Bronze | Table for quarantine data which fails expectations |
| bronze_quarantine_table_path_{env} | Bronze database for quarantine data which fails expectations. |
| bronze_quarantine_table_partitions | Bronze quarantine tables partition cols |
| bronze_quarantine_table_properties | DLT table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| bronze_append_flows | Bronze table append flows json. e.g.`"bronze_append_flows":[{"name":"customer_bronze_flow", "create_streaming_table": false,"source_format": "cloudFiles", "source_details": {"source_database": "APP","source_table":"CUSTOMERS", "source_path_dev": "tests/resources/data/customers", "source_schema_path": "tests/resources/schema/customer_schema.ddl"},"reader_options": {"cloudFiles.format": "json","cloudFiles.inferColumnTypes": "true","cloudFiles.rescuedDataColumn": "_rescued_data"},"once": true}]` |
| silver_database_{env} | Silver database name. |
| silver_table | Silver table name |
| silver_partition_columns | Silver table partition columns list |
| silver_cdc_apply_changes | Silver cdc apply changes Json |
| silver_table_path_{env} | Silver table storage path. |
| silver_table_properties | DLT table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false"}` |
| silver_transformation_json | Silver table sql transformation json path |
| silver_data_quality_expectations_json_{env} | Silver table data quality expectations json file path
| silver_append_flows | Silver table append flows json. e.g.`"silver_append_flows":[{"name":"customer_bronze_flow", "create_streaming_table": false,"source_format": "cloudFiles", "source_details": {"source_database": "APP","source_table":"CUSTOMERS", "source_path_dev": "tests/resources/data/customers", "source_schema_path": "tests/resources/schema/customer_schema.ddl"},"reader_options": {"cloudFiles.format": "json","cloudFiles.inferColumnTypes": "true","cloudFiles.rescuedDataColumn": "_rescued_data"},"once": true}]` 


### Data Quality Rules File Structure([Examples](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe))
| Field | Description |
| :-----------: | :----------- |
| expect | Specify multiple data quality sql for each field when records that fail validation should be included in the target dataset| 
| expect_or_fail  | Specify multiple data quality sql for each field when records that fail validation should halt pipeline execution |
| expect_or_drop  | Specify multiple data quality sql for each field when records that fail validation should be dropped from the target dataset |
| expect_or_quarantine  | Specify multiple data quality sql for each field when records that fails validation will be dropped from main table and inserted into quarantine table specified in dataflowspec (only applicable for Bronze layer) |


### Silver transformation File Structure([Example](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json))
| Field | Description |
| :-----------: | :----------- |
| target_table | Specify target table name : Type String | 
| target_partition_cols  | Specify partition columns : Type Array |
| select_exp | Specify SQL expressions : Type Array | 
| where_clause  | Specify filter conditions if you want to prevent certain records from main input : Type Array |
