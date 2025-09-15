---
title: "Execution"
date: 2021-08-04T14:26:55-04:00
weight: 61
draft: false
---

**Q. How do I get started ?**

Please refer to the [Getting Started]({{%relref "getting_started/_index.md" %}}) guide

**Q. How do I create metadata DLT-META ?**

DLT-META needs following metadata files:
- [Onboarding File](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.template) captures input/output metadata 
- [Data Quality Rules File](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe) captures data quality rules
- [Silver transformation File](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json) captures  processing logic as sql 

**Q. What is DataflowSpecs?**

DLT-META translates input metadata into Delta table as DataflowSpecs


**Q. How many Lakeflow Declarative Pipelines will be launched using DLT-META?**

DLT-META uses data_flow_group to launch Lakeflow Declarative Pipelines, so all the tables belongs to same group will be executed under single Lakeflow Declarative pipeline. 

**Q. Can we run onboarding for bronze layer only?**

Yes! Please follow below steps:
1. Bronze Metadata preparation ([example](https://github.com/databrickslabs/dlt-meta/blob/main/examples/bronze_onboarding.template))
2. Onboarding Job
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#onboardjob)
    Use below parameters
    ```
    {                   
            "onboard_layer": "bronze",
            "database": "dlt_demo",
            "onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
            "bronze_dataflowspec_table": "bronze_dataflowspec_table",
            "import_author": "Ravi",
            "version": "v1",
            "uc_enabled": "True",
            "overwrite": "True",
            "env": "dev"
    } 
    ```
    - option#3: [Databircks Notebook](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#option2-databricks-notebook)
```
        onboarding_params_map = {
                "database": "uc_name.dlt_demo",
                "onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
                "bronze_dataflowspec_table": "bronze_dataflowspec_table", 
                "overwrite": "True",
                "env": "dev",
                "version": "v1",
                "import_author": "Ravi"
                }

        from src.onboard_dataflowspec import OnboardDataflowspec
        OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_bronze_dataflow_spec()
```
**Q. Can we run onboarding for silver layer only?**
Yes! Please follow below steps:
1. Bronze Metadata preparation ([example](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding_silverfanout.template))
2. Onboarding Job
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#onboardjob)
    Use below parameters
    ```
    {                   
            "onboard_layer": "silver",
            "database": "dlt_demo",
            "onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
            "silver_dataflowspec_table": "silver_dataflowspec_table",
            "import_author": "Ravi",
            "version": "v1",
            "uc_enabled": "True",
            "overwrite": "True",
            "env": "dev"
    } 
    ```
    - option#3: [Databircks Notebook](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#option2-databricks-notebook)
```
        onboarding_params_map = {
                "database": "uc_name.dlt_demo",
                "onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
                "silver_dataflowspec_table": "silver_dataflowspec_table", 
                "overwrite": "True",
                "env": "dev",
                "version": "v1",
                "import_author": "Ravi"
                }

        from src.onboard_dataflowspec import OnboardDataflowspec
        OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_silver_dataflow_spec()
```

**Q. How to chain multiple silver tables after bronze table?**
- Example: After customers_cdc bronze table, can I have customers silver table reading from customers_cdc and another customers_clean silver table reading from customers_cdc? If so, how do I define these in onboarding.json?

- You can run onboarding for additional silver customer_clean table by having [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding_silverfanout.template) and [silver transformation](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations_fanout.template) with filter condition for fan out.

- Run onboarding for slilver layer in append mode("overwrite": "False") so it will append to existing silver tables.
When you launch Lakeflow Declarative Pipeline it will read silver onboarding and run Lakeflow Declarative Pipeline for bronze source and silver as target

**Q. How can I do type1 or type2 merge to target table?**

- Using Lakeflow Declarative Pipeline's [dlt.create_auto_cdc_flow](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-apply-changes) we can do type1 or type2 merge.
- DLT-META have tag in onboarding file as `bronze_cdc_apply_changes` or `silver_cdc_apply_changes` which maps to Lakeflow Declarative Pipeline's create_auto_cdc_flow API.
```
"silver_cdc_apply_changes": {
   "keys":[
      "customer_id"
   ],
   "sequence_by":"dmsTimestamp,enqueueTimestamp,sequenceId",
   "scd_type":"2",
   "apply_as_deletes":"Op = 'D'",
   "except_column_list":[
      "Op",
      "dmsTimestamp",
      "_rescued_data"
   ]
}
```

**Q. How can I write to same target table using different sources?**

- Using Lakeflow Declarative Pipeline's [dlt.append_flow API](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-append-flow) we can write to same target from different sources. 
- DLT-META have tag in onboarding file as [bronze_append_flows](https://github.com/databrickslabs/dlt-meta/blob/main/integration_tests/conf/cloudfiles-onboarding.template#L41) and [silver_append_flows](https://github.com/databrickslabs/dlt-meta/blob/main/integration_tests/conf/cloudfiles-onboarding.template#L67) 
dlt.append_flow API is mapped to 
```json 
[
   {
      "name":"customer_bronze_flow1",
      "create_streaming_table":false,
      "source_format":"cloudFiles",
      "source_details":{
         "source_path_dev":"tests/resources/data/customers",
         "source_schema_path":"tests/resources/schema/customer_schema.ddl"
      },
      "reader_options":{
         "cloudFiles.format":"json",
         "cloudFiles.inferColumnTypes":"true",
         "cloudFiles.rescuedDataColumn":"_rescued_data"
      },
      "once":true
   },
   {
      "name":"customer_bronze_flow2",
      "create_streaming_table":false,
      "source_format":"delta",
      "source_details":{
         "source_database":"{uc_catalog_name}.{bronze_schema}",
         "source_table":"customers_delta"
      },
      "reader_options":{
         
      },
      "once":false
   }
]
```

**Q. How to add autloaders file metadata to bronze table?**

DLT-META have tag [source_metadata](https://github.com/databrickslabs/dlt-meta/blob/ebd53114e5e8a79bf12f946e8dd425ac3f329289/integration_tests/conf/cloudfiles-onboarding.template#L11) in onboarding json under `source_details`
```
"source_metadata":{
   "include_autoloader_metadata_column":"True",
   "autoloader_metadata_col_name":"source_metadata",
   "select_metadata_cols":{
      "input_file_name":"_metadata.file_name",
      "input_file_path":"_metadata.file_path"
   }
}
```
- `include_autoloader_metadata_column` flag will add _metadata column to target bronze dataframe.
- `autoloader_metadata_col_name` if this provided then will be used to rename _metadata to this value otherwise default is `source_metadata`
- `select_metadata_cols:{key:value}` will be used to extract columns from _metadata. key is target dataframe column name and value is expression used to add column from _metadata column

**Q. After upgrading dlt-meta, why do Lakeflow Declarative Pipeline fail with the message “Materializing tables in custom schemas is not supported,” and how can this be fixed?**

This failure happens because the pipeline was created using Legacy Publishing mode, which does not support saving tables with catalog or schema qualifiers (such as catalog.schema.table). As a result, using qualified table names leads to an error:

``
com.databricks.pipelines.common.errors.DLTAnalysisException: Materializing tables in custom schemas is not supported. Please remove the database qualifier from table 'catalog_name.schema_name.table_name'
``

To resolve this, migrate the pipeline to the default (Databricks Publishing Mode) by following Databricks’ guide: [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode). 

