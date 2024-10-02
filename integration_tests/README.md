#### Run Integration Tests
1. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

2. ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
    ```

3. ```commandline
    cd dlt-meta
    ```

4. ```commandline
    python -m venv .venv
    ```

5. ```commandline
    source .venv/bin/activate
    ```

6. ```commandline
    pip install databricks-sdk
    ```

7. ```commandline
    dlt_meta_home=$(pwd)
    ```

8. ```commandline
    export PYTHONPATH=$dlt_meta_home
    ```

9. Run integration test against cloudfile or eventhub or kafka using below options. To use the Databricks profile configured using CLI then pass ```--profile <profile-name>``` to below command otherwise provide workspace url and token in command line. You will also need to provide a Unity Catalog catalog for which the schemas, tables, and files will be created in.

    - 9a. Run the command for cloudfiles
        ```commandline
        python integration_tests/run_integration_tests.py  --uc_catalog_name=<<uc catalog name>> --source=cloudfiles --cloud_provider_name=aws --profile=<<DEFAULT>>
        ```

    - 9b. Run the command for eventhub
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>> --source=eventhub  --cloud_provider_name=aws --eventhub_name=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer --eventhub_consumer_accesskey_name=consumer  --eventhub_name_append_flow=TODO? --eventhub_accesskey_secret_name=TODO?
        ```

    - - For eventhub integration tests, the following are the prerequisites:
        1. Needs eventhub instance running
        2. Use Databricks CLI, Create databricks secrets scope for eventhub keys (```databricks secrets create-scope eventhubs_creds```)
        3. Use Databricks CLI, Create databricks secrets to store producer and consumer keys using the scope created in step

    - - Following are the mandatory arguments for running EventHubs integration test
        1. Provide your eventhub topic : --eventhub_name
        2. Provide eventhub namespace : --eventhub_namespace
        3. Provide eventhub port : --eventhub_port
        4. Provide databricks secret scope name : --eventhub_secrets_scope_name
        5. Provide eventhub producer access key name : --eventhub_producer_accesskey_name
        6. Provide eventhub access key name : --eventhub_consumer_accesskey_name


    - 9c. Run the command for kafka
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>>  --source=kafka --kafka_topic_name=dlt-meta-integration-test --kafka_broker=host:9092 --cloud_provider_name=aws --profile=DEFAULT
        ```

    - - For kafka integration tests, the following are the prerequisites:
        1. Needs kafka instance running

    - - Following are the mandatory arguments for running EventHubs integration test
        1. Provide your kafka topic name : --kafka_topic_name
        2. Provide kafka_broker : --kafka_broker

10. Once finished integration output file will be copied locally to
```integration-test-output_<run_id>.txt```

11. Output of a successful run should have the following in the file
```
,0
0,Completed Bronze DLT Pipeline.
1,Completed Silver DLT Pipeline.
2,Validating DLT Bronze and Silver Table Counts...
3,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions.
4,Expected: 10002 Actual: 10002. Passed!
5,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions_quarantine.
6,Expected: 7 Actual: 7. Passed!
7,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.customers.
8,Expected: 98928 Actual: 98928. Passed!
9,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.customers_quarantine.
10,Expected: 1077 Actual: 1077. Passed!
11,Validating Counts for Table silver_7d1d3ccc9e144a85b07c23110ea50133.transactions.
12,Expected: 8759 Actual: 8759. Passed!
13,Validating Counts for Table silver_7d1d3ccc9e144a85b07c23110ea50133.customers.
14,Expected: 87256 Actual: 87256. Passed!
```