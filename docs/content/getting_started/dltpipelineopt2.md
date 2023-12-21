---
title: "Launch Generic DLT pipeline"
date: 2021-08-04T14:25:26-04:00
weight: 21
draft: false
---
### Option#2: Manual

#### 1. Create a Delta Live Tables launch notebook

1. Go to your Databricks landing page and select Create a notebook, or click New Icon New in the sidebar and select Notebook. The Create Notebook dialog appears.

2. In the Create Notebook dialogue, give your notebook a name e.g ```dlt_meta_pipeline``` and select Python from the Default Language dropdown menu. You can leave Cluster set to the default value. The Delta Live Tables runtime creates a cluster before it runs your pipeline.

3. Click Create.

4. You can add the [example dlt pipeline](https://github.com/databrickslabs/dlt-meta/blob/main/examples/dlt_meta_pipeline.ipynb) code or import iPython notebook as is.
    ```
        %pip install dlt-meta
    ```
    ```
        layer = spark.conf.get("layer", None)
        from src.dataflow_pipeline import DataflowPipeline
        DataflowPipeline.invoke_dlt_pipeline(spark, layer)
    ```
### 2. Create a DLT pipeline

1. Click Jobs Icon Workflows in the sidebar, click the Delta Live Tables tab, and click Create Pipeline.

2. Give the pipeline a name e.g. DLT_META_BRONZE and click File Picker Icon to select a notebook ```dlt_meta_pipeline``` created in step: ```Create a dlt launch notebook```.

3. Optionally enter a storage location for output data from the pipeline. The system uses a default location if you leave Storage location empty.

4. Select Triggered for Pipeline Mode.

5. Enter Configuration parameters e.g.
    ```
    "layer": "bronze",
    "bronze.dataflowspecTable": "dataflowspec table name",
    "bronze.group": "enter group name from metadata e.g. G1",
    ```

6. Enter target schema where you wants your bronze/silver tables to be created

7. Click Create.

8. Start pipeline: click the Start button on in top panel. The system returns a message confirming that your pipeline is starting 
