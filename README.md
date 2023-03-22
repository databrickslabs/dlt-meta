# DLT-META

<!-- Top bar will be removed from PyPi packaged versions -->
<!-- Dont remove: exclude package -->
[Documentation](https://databrickslabs.github.io/dlt-meta/) |
[Release Notes](CHANGELOG.md) |
[Examples](https://github.com/databrickslabs/dlt-meta/tree/main/examples) 
<!-- Dont remove: end exclude package -->

---
<p align="left">
    <a href="https://databrickslabs.github.io/dlt-meta/">
        <img src="https://img.shields.io/badge/DOCS-PASSING-green?style=for-the-badge" alt="Documentation Status"/>
    </a>
    <a href="https://pypi.org/project/dlt-meta/">
        <img src="https://img.shields.io/badge/PYPI-v%200.0.1-green?style=for-the-badge" alt="Latest Python Release"/>
    </a>
    <a href="https://github.com/databrickslabs/dlt-meta/actions/workflows/onpush.yml">
        <img src="https://img.shields.io/github/workflow/status/databrickslabs/dlt-meta/build/main?style=for-the-badge"
             alt="GitHub Workflow Status (branch)"/>
    </a>
    <a href="https://codecov.io/gh/databrickslabs/dlt-meta">
        <img src="https://img.shields.io/codecov/c/github/databrickslabs/dlt-meta?style=for-the-badge&amp;token=KI3HFZQWF0"
             alt="codecov"/>
    </a>
    <a href="https://lgtm.com/projects/g/databrickslabs/dlt-meta/alerts">
        <img src="https://img.shields.io/lgtm/alerts/github/databricks/dlt-meta?style=for-the-badge" alt="lgtm-alerts"/>
    </a>
    <a href="https://lgtm.com/projects/g/databrickslabs/dlt-meta/context:python">
        <img src="https://img.shields.io/lgtm/grade/python/github/databrickslabs/dbx?style=for-the-badge"
             alt="lgtm-code-quality"/>
    </a>
    <a href="https://pypistats.org/packages/dl-meta">
        <img src="https://img.shields.io/pypi/dm/dlt-meta?style=for-the-badge" alt="downloads"/>
    </a>
    <a href="https://github.com/PyCQA/flake8">
        <img src="https://img.shields.io/badge/FLAKE8-FLAKE8-lightgrey?style=for-the-badge"
             alt="We use flake8 for formatting"/>
    </a>
</p>

---

# Project Overview
```DLT-META``` is a metadata-driven framework based on Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) (aka DLT) which lets you automate your bronze and silver data pipelines.

With this framework you need to record the source and target metadata in an onboarding json file which acts as the data flow specification aka Dataflowspec. A single generic ```DLT``` pipeline takes the ```Dataflowspec``` and runs your workloads.

### Components:

#### Metadata Interface 
- Capture input/output metadata in [onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json)
- Capture [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe/customers/bronze_data_quality_expectations.json)
- Capture  processing logic as sql in [Silver transformation file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json)

#### Generic DLT pipeline
- Apply appropriate readers based on input metadata
- Apply data quality rules with DLT expectations 
- Apply CDC apply changes if specified in metadata
- Builds DLT graph based on input/output metadata
- Launch DLT pipeline

## High-Level Process Flow:
![DLT-META High-Level Process Flow](./docs/static/images/solutions_overview.png)

## More questions

Refer to the [FAQ](https://databrickslabs.github.io/dlt-meta/faq)
and DLT-META [documentation](https://databrickslabs.github.io/dlt-meta/)

## Steps
![DLT-META Stages](./docs/static/images/dlt-meta_stages.png)


## 1. Metadata preparation 
1. Create ```onboarding.json``` metadata file and save to s3/adls/dbfs e.g.[onboarding file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/onboarding.json)
2. Create ```silver_transformations.json``` and save to s3/adls/dbfs e.g [Silver transformation file](https://github.com/databrickslabs/dlt-meta/blob/main/examples/silver_transformations.json)
3. Create data quality rules json and store to s3/adls/dbfs e.g [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe/customers/bronze_data_quality_expectations.json)

## 2. Onboarding job

1. Go to your Databricks landing page and do one of the following:

2. In the sidebar, click Jobs Icon Workflows and click Create Job Button.

3. In the sidebar, click New Icon New and select Job from the menu.

4. In the task dialog box that appears on the Tasks tab, replace Add a name for your job… with your job name, for example, Python wheel example.

5. In Task name, enter a name for the task, for example, ```dlt_meta_onboarding_pythonwheel_task```.

6. In Type, select Python wheel.

5. In Package name, enter ```dlt_meta```.

6. In Entry point, enter ``run``. 

7. Click Add under Dependent Libraries. In the Add dependent library dialog, under Library Type, click PyPI. Enter Package: ```dlt-meta```
 

8. Click Add.

9. In Parameters, select keyword argument then select JSON. Past below json parameters with :
    ``` 
    {
                        "database": "dlt_demo",
                        "onboarding_file_path": "dbfs:/onboarding_files/users_onboarding.json",
                        "silver_dataflowspec_table": "silver_dataflowspec_table",
                        "silver_dataflowspec_path": "dbfs:/onboarding_tables_cdc/silver",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_table",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": "dbfs:/onboarding_tables_cdc/bronze",
                        "overwrite": "True",
                        "env": "dev"
    } 
    ```
    Alternatly you can enter keyword arguments, click + Add and enter a key and value. Click + Add again to enter more arguments. 

10. Click Save task.

11. Run now

12. Make sure job run successfully. Verify metadata in your dataflow spec tables entered in step: 9 e.g ```dlt_demo.bronze_dataflowspec_table``` , ```dlt_demo.silver_dataflowspec_table```

## 3. Launch Dataflow DLT Pipeline  
### Create a dlt launch notebook

1. Go to your Databricks landing page and select Create a notebook, or click New Icon New in the sidebar and select Notebook. The Create Notebook dialog appears.

2. In the Create Notebook dialogue, give your notebook a name e.g ```dlt_meta_pipeline``` and select Python from the Default Language dropdown menu. You can leave Cluster set to the default value. The Delta Live Tables runtime creates a cluster before it runs your pipeline.

3. Click Create.

4. You can add the [example dlt pipeline](https://github.com/databrickslabs/dlt-meta/blob/main/examples/dlt_meta_pipeline.ipynb) code or import iPython notebook as is.

### Create a DLT pipeline

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



# Additional
You can run integration tests from you local with dlt-meta.
## Run Integration Tests
1. Clone [DLT-META](https://github.com/databrickslabs/dlt-meta)

2. Open terminal and Goto root folder ```DLT-META```

3. Create environment variables.

```
export DATABRICKS_HOST=<DATABRICKS HOST>
export DATABRICKS_TOKEN=<DATABRICKS TOKEN> # Account needs permission to create clusters/dlt pipelines.
```

4. Run itegration tests for different supported input sources: cloudfiles, eventhub, kafka

    4a. Run the command for cloudfiles ```python integration-tests/run-integration-test.py  --cloud_provider_name=aws --dbr_version=11.3.x-scala2.12 --source=cloudfiles --dbfs_path=dbfs:/tmp/DLT-META/```

    4b. Run the command for eventhub ```python integration-tests/run-integration-test.py --cloud_provider_name=azure --dbr_version=11.3.x-scala2.12 --source=eventhub --dbfs_path=dbfs:/tmp/DLT-META/ --eventhub_name=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer ----eventhub_consumer_accesskey_name=consumer```

        For eventhub integration tests, the following are the prerequisites:
        1. Needs eventhub instance running
        2. Using Databricks CLI, Create databricks secrets scope for eventhub keys
        3. Using Databricks CLI, Create databricks secrets to store producer and consumer keys using the scope created in step 2 

        Following are the mandatory arguments for running EventHubs integration test
        1. Provide your eventhub topic name : ```--eventhub_name```
        2. Provide eventhub namespace using ```--eventhub_namespace```
        3. Provide eventhub port using ```--eventhub_port```
        4. Provide databricks secret scope name using ```----eventhub_secrets_scope_name```
        5. Provide eventhub producer access key name using ```--eventhub_producer_accesskey_name```
        6. Provide eventhub access key name using ```--eventhub_consumer_accesskey_name```


    4c. Run the command for kafka ```python3 integration-tests/run-integration-test.py --cloud_provider_name=aws --dbr_version=11.3.x-scala2.12 --source=kafka --dbfs_path=dbfs:/tmp/DLT-META/ --kafka_topic_name=dlt-meta-integration-test --kafka_broker=host:9092```

        For kafka integration tests, the following are the prerequisites:
        1. Needs kafka instance running

        Following are the mandatory arguments for running EventHubs integration test
        1. Provide your kafka topic name : ```--kafka_topic_name```
        2. Provide kafka_broker  ```--kafka_broker```



    Once finished integration output file will be copied locally to  ```integration-test-output_<run_id>.csv```

5. Output of a successful run should have the following in the file 

    ```
    ,0
    0,Completed Bronze DLT Pipeline.
    1,Completed Silver DLT Pipeline.
    2,Validating DLT Bronze and Silver Table Counts...
    3,Validating Counts for Table bronze_7b866603ab184c70a66805ac8043a03d.transactions_cdc.
    4,Expected: 10002 Actual: 10002. Passed!
    5,Validating Counts for Table bronze_7b866603ab184c70a66805ac8043a03d.transactions_cdc_quarantine.
    6,Expected: 9842 Actual: 9842. Passed!
    7,Validating Counts for Table bronze_7b866603ab184c70a66805ac8043a03d.customers_cdc.
    8,Expected: 98928 Actual: 98928. Passed!
    9,Validating Counts for Table silver_7b866603ab184c70a66805ac8043a03d.transactions.
    10,Expected: 8759 Actual: 8759. Passed!
    11,Validating Counts for Table silver_7b866603ab184c70a66805ac8043a03d.customers.
    12,Expected: 87256 Actual: 87256. Passed!
    ```

# Project Support
Please note that all projects released under [`Databricks Labs`](https://www.databricks.com/learn/labs)
 are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements 
(SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket 
relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as issues on the Github Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.