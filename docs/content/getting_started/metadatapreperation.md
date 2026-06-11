---
title: "Metadata Preparation"
date: 2021-08-04T14:25:26-04:00
weight: 6
draft: false
---


### Directory structure
Repo layout for the bundled demo configs (each format lives in its own subdirectory):

```
demo/conf/
    json/
        onboarding.template
        silver_transformations.json
        dqe/
            customers.json
            ...
    yml/
        onboarding.template.yml
        silver_transformations.yml
        dqe/
            customers.yml
            ...
```

Pick one format and stay in it — both trees are kept in sync. JSON examples:

1. [`demo/conf/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template)
2. [`demo/conf/json/silver_transformations.json`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/silver_transformations.json)
3. [`demo/conf/json/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/json/dqe/)

YAML equivalents:

1. [`demo/conf/yml/onboarding.template.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/onboarding.template.yml)
2. [`demo/conf/yml/silver_transformations.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml)
3. [`demo/conf/yml/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe/)

Each onboarding template references its own format's silver-transformation and DQE files (the JSON template points at `/json/...`; the YAML template points at `/yml/...`).

### Onboarding file structure (JSON or YAML)

Onboarding samples are committed under `examples/json/` and `examples/yml/`. Pick whichever format you prefer — the runner accepts both.

JSON examples: [Autoloader](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/cloudfiles-onboarding.template), [Eventhub](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/eventhub-onboarding.template), [Kafka](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/kafka-onboarding.template).

YAML examples: [Autoloader](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/cloudfiles-onboarding.template.yml), [Eventhub](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/eventhub-onboarding.template.yml).
`env` is your environment placeholder e.g `dev`, `prod`, `stag`
| Field | Description |
| :-----------: | :----------- |
| data_flow_id | This is unique identifier for pipeline |
| data_flow_group | This is group identifier for launching multiple pipelines under single Lakeflow Spark Declarative Pipeline |
| source_format | Source format e.g `cloudFiles`, `eventhub`, `kafka`, `delta`, `snapshot`, `sqlserver` |
| source_details | This map Type captures all source details for cloudfiles = `source_schema_path`, `source_path_{env}`, `source_catalog`, `source_database`, `source_metadata` For eventhub= `source_schema_path` , `eventhub.accessKeyName`, `eventhub.accessKeySecretName`, `eventhub.name` , `eventhub.secretsScopeName` , `kafka.sasl.mechanism`, `kafka.security.protocol`, `eventhub.namespace`, `eventhub.port`. For sqlserver= `connection_name` (Databricks connection name), `table`, optionally `query` for custom SQL queries. For Source schema file spark DDL schema format parsing is supported <br> In case of custom schema format then write schema parsing function `bronze_schema_mapper(schema_file_path, spark):Schema` and provide to `OnboardDataflowspec` initialization <br> e.g `onboardDataFlowSpecs = OnboardDataflowspec(spark, dict_obj,bronze_schema_mapper).onboardDataFlowSpecs()`.<br> For cloudFiles option _metadata columns addtiion there is `source_metadata` tag with attributes: `include_autoloader_metadata_column` flag (`True` or `False` value) will add _metadata column to target bronze dataframe, `autoloader_metadata_col_name` if this provided then will be used to rename _metadata to this value otherwise default is `source_metadata`,`select_metadata_cols:{key:value}` will be used to extract columns from _metadata. key is target dataframe column name and value is expression used to add column from _metadata column. <br> for snapshot= `snapshot_format`, `source_path_{env}` |
| bronze_catalog_{env} | Unity catalog name |         
| bronze_database_{env} | Delta lake bronze database name. |
| bronze_table | Delta lake bronze table name |
| bronze_table_comment | Bronze table comment |
| bronze_reader_options | Reader options which can be provided to spark reader <br> e.g multiline=true,header=true in json format |
| bronze_parition_columns | Bronze table partition cols list |
| bronze_cluster_by | Bronze tables cluster by cols list |
| bronze_cluster_by_auto | Enable automatic liquid clustering on the bronze table. Boolean value (true/false). Can be combined with bronze_cluster_by to define initial clustering keys. See [Automatic liquid clustering](https://docs.databricks.com/aws/en/delta/clustering#auto-liquid) |
| bronze_cdc_apply_changes | Bronze cdc apply changes Json |
| bronze_apply_changes_from_snapshot | Bronze apply changes from snapshot Json e.g. Mandatory fields: keys=["userId"], scd_type=`1` or `2` optional fields: track_history_column_list=`[col1]`, track_history_except_column_list=`[col2]` | 
| bronze_table_path_{env} | Bronze table storage path.|
| bronze_table_properties | Lakeflow Spark Declarative Pipeline table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| bronze_sink | Lakeflow Spark Declarative Pipeline Sink API properties: e.g Delta: `{"name": "bronze_sink","format": "delta","options": {"tableName": "my_catalog.my_schema.my_table"}}`, Kafka:`{"name": "bronze_sink","format": "kafka","options": { "kafka.bootstrap.servers": "host:port","subscribe": "my_topic"}}` |
| bronze_data_quality_expectations_json | Bronze table data quality expectations |
| bronze_catalog_quarantine_{env} | Unity catalog name | 
| bronze_database_quarantine_{env} | Bronze database for quarantine data which fails expectations. |
| bronze_quarantine_table	| Bronze Table for quarantine data which fails expectations |
| bronze_quarantine_table_comment | Bronze quarantine table comment |
| bronze_quarantine_table_path_{env} | Bronze database for quarantine data which fails expectations. |
| bronze_quarantine_table_partitions | Bronze quarantine tables partition cols |
| bronze_quarantine_table_cluster_by | Bronze quarantine tables cluster cols |
| bronze_quarantine_table_properties | Lakeflow Spark Declarative Pipeline table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| bronze_append_flows | Bronze table append flows json. e.g.`"bronze_append_flows":[{"name":"customer_bronze_flow", "create_streaming_table": false,"source_format": "cloudFiles", "source_details": {"source_database": "APP","source_table":"CUSTOMERS", "source_path_dev": "tests/resources/data/customers", "source_schema_path": "tests/resources/schema/customer_schema.ddl"},"reader_options": {"cloudFiles.format": "json","cloudFiles.inferColumnTypes": "true","cloudFiles.rescuedDataColumn": "_rescued_data"},"once": true}]` |
| silver_catalog_{env} | Unit Catalog name. |
| silver_database_{env} | Silver database name. |
| silver_table | Silver table name |
| silver_table_comment | Silver table comments |
| silver_partition_columns | Silver table partition columns list |
| silver_cluster_by | Silver tables cluster by cols list |
| silver_cluster_by_auto | Enable automatic liquid clustering on the silver table. Boolean value (true/false). Can be combined with silver_cluster_by to define initial clustering keys. See [Automatic liquid clustering](https://docs.databricks.com/aws/en/delta/clustering#auto-liquid) |
| silver_cdc_apply_changes | Silver cdc apply changes Json |
| silver_table_path_{env} | Silver table storage path. |
| silver_table_properties | Lakeflow Spark Declarative Pipeline table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false"}` |
| silver_sink | Lakeflow Spark Declarative Pipeline Sink API properties: e.g Delta:`{"name": "silver_sink","format": "delta","options": {"tableName": "my_catalog.my_schema.my_table"}}`, Kafka:`{"name": "silver_sink","format": "kafka","options": { "kafka.bootstrap.servers": "host:port","subscribe": "my_topic"}}`|
| silver_transformation_json | Silver table sql transformation json path |
| silver_data_quality_expectations_json_{env} | Silver table data quality expectations json file path
| silver_append_flows | Silver table append flows json. e.g.`"silver_append_flows":[{"name":"customer_bronze_flow", 
| silver_apply_changes_from_snapshot | Silver apply changes from snapshot Json e.g. Mandatory fields: keys=["userId"], scd_type=`1` or `2` optional fields: track_history_column_list=`[col1]`, track_history_except_column_list=`[col2]`|



### Data Quality Rules File Structure([Examples](https://github.com/databrickslabs/sdp-meta/tree/main/examples/dqe))
| Field | Description |
| :-----------: | :----------- |
| expect | Specify multiple data quality sql for each field when records that fail validation should be included in the target dataset| 
| expect_or_fail  | Specify multiple data quality sql for each field when records that fail validation should halt pipeline execution |
| expect_or_drop  | Specify multiple data quality sql for each field when records that fail validation should be dropped from the target dataset |
| expect_or_quarantine  | Specify multiple data quality sql for each field when records that fails validation will be dropped from main table and inserted into quarantine table specified in dataflowspec (only applicable for Bronze layer) |


### Silver transformation file structure

Examples: JSON ([`examples/json/silver_transformations.json`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/silver_transformations.json)) or YAML (e.g. [`demo/conf/yml/silver_transformations.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml)).
| Field | Description |
| :-----------: | :----------- |
| target_table | Specify target table name : Type String | 
| target_partition_cols  | Specify partition columns : Type Array |
| select_exp | Specify SQL expressions : Type Array | 
| where_clause  | Specify filter conditions if you want to prevent certain records from main input : Type Array |
