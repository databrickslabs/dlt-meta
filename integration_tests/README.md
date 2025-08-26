#### Run Integration Tests
1. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

2. Clone dlt-meta:
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
    ```

3. Navigate to project directory:
    ```commandline
    cd dlt-meta
    ```

4. Create Python virtual environment:
    ```commandline
    python -m venv .venv
    ```

5. Activate virtual environment:
    ```commandline
    source .venv/bin/activate
    ```

6. Install required packages:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk
    
    # Development requirements
    pip install delta-spark==3.0.0 pyspark==3.5.5 pytest>=7.0.0 coverage>=7.0.0
    
    # Integration test requirements
    pip install "typer[all]==0.6.1"
    ```

7. Set environment variables:
    ```commandline
    dlt_meta_home=$(pwd)
    export PYTHONPATH=$dlt_meta_home
    ```

9. Run integration test against cloudfile or eventhub or kafka using below options. To use the Databricks profile configured using CLI then pass ```--profile <profile-name>``` to below command otherwise provide workspace url and token in command line. You will also need to provide a Unity Catalog catalog for which the schemas, tables, and files will be created in.

    - 9a. Run the command for  **cloudfiles**
        ```commandline
        python integration_tests/run_integration_tests.py  --uc_catalog_name=<<uc catalog name>> --source=cloudfiles --profile=<<DEFAULT>>
        ```

    - 9b. Run the command for **eventhub**
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>> --source=eventhub --eventhub_name=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer --eventhub_consumer_accesskey_name=consumer  --eventhub_name_append_flow=test_append_flow --eventhub_accesskey_secret_name=test_secret_name --profile=<<DEFAULT>>
        ```
    Prerequisites for eventhub integration tests:
    1. Running eventhub instance
    2. Create databricks secrets scope for eventhub keys:
       ```commandline
       databricks secrets create-scope eventhubs_creds
       ```
    3. Create databricks secrets to store producer and consumer keys using the scope created in step 2

    Required arguments for EventHubs integration test:
    1. `--eventhub_name` : Your eventhub topic
    2. `--eventhub_namespace` : Eventhub namespace
    3. `--eventhub_port` : Eventhub port
    4. `--eventhub_secrets_scope_name` : Databricks secret scope name
    5. `--eventhub_producer_accesskey_name` : Eventhub producer access key name
    6. `--eventhub_consumer_accesskey_name` : Eventhub access key name


    - 9c. Run the command for **kafka**
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>>  --source=kafka --kafka_source_topic=dlt-meta-integration-test --kafka_sink_topic=dlt-meta_inttest_topic --kafka_source_broker=host:9092 --profile=<<DEFAULT>>
        ```
    Optional secret configuration:
    ```commandline
    --kafka_source_servers_secrets_scope_name=<<scope_name>> --kafka_source_servers_secrets_scope_key=<<scope_key>>
    --kafka_sink_servers_secret_scope_name=<<scope_name>> --kafka_sink_servers_secret_scope_key=<<scope_key>>
    ```

    Prerequisites for kafka integration tests:
    1. Running kafka instance

    Required arguments for kafka integration test:
    1. `--kafka_topic` : Your kafka topic name
    2. `--kafka_broker` : Kafka broker address
    
    - 9d. Run the command for **snapshot**
        ```commandline
        python integration_tests/run_integration_tests.py --source=snapshot --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```


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