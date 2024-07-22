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
- [Onboarding File](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json) captures input/output metadata 
- [Data Quality Rules File](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe) captures data quality rules
- [Silver transformation File](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json) captures  processing logic as sql 

**Q. What is DataflowSpecs?**

DLT-META translates input metadata into Delta table as DataflowSpecs


**Q. How many DLT pipelines will be launched using DLT-META?**

DLT-META uses data_flow_group to launch DLT pipelines, so all the tables belongs to same group will be executed under single DLT pipeline. 

**Q. Can we run onboarding for bronze layer only?**

Yes! Please follow below steps:
1. Bronze Metadata preparation ([example](https://github.com/databrickslabs/dlt-meta/blob/main/integration_tests/conf/cloudfiles-onboarding_A2.template))
2. Onboarding Job
    - Option#1: [DLT-META CLI](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#onboardjob)
    - Option#2: [Manual Job](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_manual/#onboardjob)
    Use below parameters
    ```
    {                   
            "onboard_layer": "bronze",
            "database": "dlt_demo",
            "onboarding_file_path": "dbfs:/onboarding_files/users_onboarding.json",
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
                "onboarding_file_path": "dbfs:/onboarding_files/users_onboarding.json",
                "bronze_dataflowspec_table": "bronze_dataflowspec_table", 
                "overwrite": "True",
                "env": "dev",
                "version": "v1",
                "import_author": "Ravi"
                }

        from src.onboard_dataflowspec import OnboardDataflowspec
        OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_bronze_dataflow_spec()
```
**Q. How can I do type1 or type2 merge to target table?**

Using DLT's [dlt.apply_changes](https://docs.databricks.com/en/delta-live-tables/cdc.html) we can do type1 or type2 merge. DLT-META have tag in onboarding file as bronze_cdc_apply_changes or silver_apply_changes which maps to DLT's apply_changes API.
```
"silver_cdc_apply_changes": {
   "keys":[
      "customer_id"
   ],
   "sequence_by":"dmsTimestamp",
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

Using DLT's [dlt.append_flow API](https://docs.databricks.com/en/delta-live-tables/flows.html) we can write to same target from different sources. DLT-META have tag in onboarding file as [bronze_append_flows](https://github.com/databrickslabs/dlt-meta/blob/main/integration_tests/conf/cloudfiles-onboarding.template#L41) and [silver_append_flows](https://github.com/databrickslabs/dlt-meta/blob/main/integration_tests/conf/cloudfiles-onboarding.template#L67) 
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

**Q. How to add custom python transformation to bronze layer?**

You can add the [example dlt pipeline](https://github.com/databrickslabs/dlt-meta/blob/main/examples/dlt_meta_pipeline.ipynb) code or import iPython notebook as is and add customer transformation function as given in below code. Your function should take Dataframe as argument and return it as Dataframe
```
        %pip install dlt-meta
```
```
        from pyspark.sql import DataFrame
        from pyspark.sql.functions import lit
        def custom_transform_func_test(input_df) -> DataFrame:
        return input_df.withColumn('custom_col', lit('test'))
```
```
        layer = spark.conf.get("layer", None)
        from src.dataflow_pipeline import DataflowPipeline
        DataflowPipeline.invoke_dlt_pipeline(spark, layer, custom_transform_func_test=custom_transform_func_test)
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

