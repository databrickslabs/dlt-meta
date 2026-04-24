---
title: "Execution"
date: 2021-08-04T14:26:55-04:00
weight: 61
draft: false
---

**Q. How do I get started ?**

Please refer to the [Getting Started]({{%relref "getting_started/_index.md" %}}) guide

**Q. How do I create metadata SDP-META ?**

SDP-META needs following metadata files:
- Onboarding File captures input/output metadata — JSON ([`examples/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/onboarding.template)) or YAML ([`examples/yml/onboarding.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/onboarding.yml))
- Data Quality Rules File captures data quality rules — JSON ([`examples/json/dqe`](https://github.com/databrickslabs/sdp-meta/tree/main/examples/json/dqe)) or YAML ([`demo/conf/yml/dqe`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe))
- Silver transformation File captures processing logic as sql — JSON ([`examples/json/silver_transformations.json`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/silver_transformations.json)) or YAML ([`demo/conf/yml/silver_transformations.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml))

**Q. What is DataflowSpecs?**

SDP-META translates input metadata into Delta table as DataflowSpecs


**Q. How many Lakeflow Spark Declarative Pipelines will be launched using SDP-META?**

SDP-META uses data_flow_group to launch Lakeflow Spark Declarative Pipelines, so all the tables belongs to same group will be executed under single Lakeflow Declarative pipeline. 

**Q. Can we run onboarding for bronze layer only?**

Yes! Please follow below steps:
1. Bronze Metadata preparation — JSON ([`examples/json/bronze_onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/bronze_onboarding.template)) or YAML ([`examples/yml/bronze_onboarding.template.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/bronze_onboarding.template.yml))
2. Onboarding Job
    - Option#1: [SDP-META CLI](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#onboardjob)
    Use below parameters
    ```
    {                   
            "onboard_layer": "bronze",
            "database": "dlt_demo",
            "onboarding_file_path": "/Volumes/<catalog>/<schema>/<volume>/sdp-meta/conf/onboarding.json",
            "bronze_dataflowspec_table": "bronze_dataflowspec_table",
            "import_author": "Ravi",
            "version": "v1",
            "uc_enabled": "True",
            "overwrite": "True",
            "env": "dev"
    } 
    ```
    - option#3: [Databircks Notebook](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#option2-databricks-notebook)
```
        onboarding_params_map = {
                "database": "uc_name.dlt_demo",
                "onboarding_file_path": "/Volumes/<catalog>/<schema>/<volume>/sdp-meta/conf/onboarding.json",
                "bronze_dataflowspec_table": "bronze_dataflowspec_table", 
                "overwrite": "True",
                "env": "dev",
                "version": "v1",
                "import_author": "Ravi"
                }

        from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
        OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_bronze_dataflow_spec()
```
**Q. Can we run onboarding for silver layer only?**
Yes! Please follow below steps:
1. Silver Metadata preparation — JSON ([`examples/json/onboarding_silverfanout.template`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/onboarding_silverfanout.template)) or YAML ([`examples/yml/onboarding_silverfanout.template.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/onboarding_silverfanout.template.yml))
2. Onboarding Job
    - Option#1: [SDP-META CLI](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#onboardjob)
    Use below parameters
    ```
    {                   
            "onboard_layer": "silver",
            "database": "dlt_demo",
            "onboarding_file_path": "/Volumes/<catalog>/<schema>/<volume>/sdp-meta/conf/onboarding.json",
            "silver_dataflowspec_table": "silver_dataflowspec_table",
            "import_author": "Ravi",
            "version": "v1",
            "uc_enabled": "True",
            "overwrite": "True",
            "env": "dev"
    } 
    ```
    - option#3: [Databircks Notebook](https://databrickslabs.github.io/sdp-meta/getting_started/sdp_meta_manual/#option2-databricks-notebook)
```
        onboarding_params_map = {
                "database": "uc_name.dlt_demo",
                "onboarding_file_path": "/Volumes/<catalog>/<schema>/<volume>/sdp-meta/conf/onboarding.json",
                "silver_dataflowspec_table": "silver_dataflowspec_table", 
                "overwrite": "True",
                "env": "dev",
                "version": "v1",
                "import_author": "Ravi"
                }

        from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
        OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_silver_dataflow_spec()
```

**Q. How to chain multiple silver tables after bronze table?**
- Example: After customers_cdc bronze table, can I have customers silver table reading from customers_cdc and another customers_clean silver table reading from customers_cdc? If so, how do I define these in onboarding.json?

- You can run onboarding for additional silver customer_clean table by having an onboarding file ([JSON](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/onboarding_silverfanout.template) or [YAML](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/onboarding_silverfanout.template.yml)) and a silver transformation file ([JSON](https://github.com/databrickslabs/sdp-meta/blob/main/examples/json/silver_transformations_fanout.template) or [YAML](https://github.com/databrickslabs/sdp-meta/blob/main/examples/yml/silver_transformations_fanout.template.yml)) with a filter condition for fan out.

- Run onboarding for slilver layer in append mode("overwrite": "False") so it will append to existing silver tables.
When you launch Lakeflow Spark Declarative Pipeline it will read silver onboarding and run Lakeflow Spark Declarative Pipeline for bronze source and silver as target

**Q. How can I do type1 or type2 merge to target table?**

- Using Lakeflow Spark Declarative Pipeline's [dp.create_auto_cdc_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes) we can do type1 or type2 merge.
- SDP-META have tag in onboarding file as `bronze_cdc_apply_changes` or `silver_cdc_apply_changes` which maps to Lakeflow Spark Declarative Pipeline's create_auto_cdc_flow API.
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

- Using Lakeflow Spark Declarative Pipeline's [dp.append_flow API](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) we can write to same target from different sources. 
- SDP-META have tag in onboarding file as [bronze_append_flows](https://github.com/databrickslabs/sdp-meta/blob/main/integration_tests/conf/cloudfiles-onboarding.template#L41) and [silver_append_flows](https://github.com/databrickslabs/sdp-meta/blob/main/integration_tests/conf/cloudfiles-onboarding.template#L67) 
dp.append_flow API is mapped to 
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

SDP-META have tag [source_metadata](https://github.com/databrickslabs/sdp-meta/blob/ebd53114e5e8a79bf12f946e8dd425ac3f329289/integration_tests/conf/cloudfiles-onboarding.template#L11) in onboarding json under `source_details`
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

**Q. After upgrading sdp-meta, why do Lakeflow Spark Declarative Pipeline fail with the message “Materializing tables in custom schemas is not supported,” and how can this be fixed?**

This failure happens because the pipeline was created using Legacy Publishing mode, which does not support saving tables with catalog or schema qualifiers (such as catalog.schema.table). As a result, using qualified table names leads to an error:

``
com.databricks.pipelines.common.errors.DLTAnalysisException: Materializing tables in custom schemas is not supported. Please remove the database qualifier from table 'catalog_name.schema_name.table_name'
``

To resolve this, migrate the pipeline to the default (Databricks Publishing Mode) by following Databricks’ guide: [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/ldp/migrate-to-dpm#migrate-to-the-default-publishing-mode). 

