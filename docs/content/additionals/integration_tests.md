---
title: "Integration Tests"
date: 2021-08-04T14:25:26-04:00
weight: 41
draft: false
---

#### Run Integration Tests
1. Initial steps
- **Prerequisite**: Datatbricks CLI installed as given [here](https://docs.databricks.com/en/dev-tools/cli/profiles.html)

```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git
```

```commandline
    cd dlt-meta
```

```commandline
    python -m venv .venv
```

```commandline
    source .venv/bin/activate
```

```commandline
    pip install databricks-sdk
```
```commandline
    dlt_meta_home=$(pwd)
```
 ```commandline
    export PYTHONPATH=$dlt_meta_home
```

2. Run integration test against cloudfile or eventhub or kafka using below options: If databricks profile configured using CLI then pass ```--profile <profile-name>``` to below command otherwise provide workspace url and token in command line
    - 2a. Run the command for cloudfiles ```python integration-tests/run_integration_tests.py  --cloud_provider_name=aws --dbr_version=15.3.x-scala2.12 --source=cloudfiles --dbfs_path=dbfs:/tmp/DLT-META/```

    - 2b. Run the command for eventhub ```python integration-tests/run_integration_tests.py --cloud_provider_name=azure --dbr_version=11.3.x-scala2.12 --source=eventhub --dbfs_path=dbfs:/tmp/DLT-META/ --eventhub_name=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer --eventhub_consumer_accesskey_name=consumer```

    - - For eventhub integration tests, the following are the prerequisites:
        1. Needs eventhub instance running
        2. Using Databricks CLI, Create databricks secrets scope for eventhub keys
        3. Using Databricks CLI, Create databricks secrets to store producer and consumer keys using the scope created in step 2 

    - - Following are the mandatory arguments for running EventHubs integration test
        1. Provide your eventhub topic : --eventhub_name
        2. Provide eventhub namespace : --eventhub_namespace
        3. Provide eventhub port : --eventhub_port
        4. Provide databricks secret scope name : --eventhub_secrets_scope_name
        5. Provide eventhub producer access key name : --eventhub_producer_accesskey_name
        6. Provide eventhub access key name : --eventhub_consumer_accesskey_name


    - 2c. Run the command for kafka ```python3 integration-tests/run_integration_tests.py --cloud_provider_name=aws --dbr_version=11.3.x-scala2.12 --source=kafka --dbfs_path=dbfs:/tmp/DLT-META/ --kafka_topic_name=dlt-meta-integration-test --kafka_broker=host:9092```

    - - For kafka integration tests, the following are the prerequisites:
        1. Needs kafka instance running

    - - Following are the mandatory arguments for running EventHubs integration test
        1. Provide your kafka topic name : --kafka_topic_name
        2. Provide kafka_broker : --kafka_broker

3. Once finished integration output file will be copied locally to 
```integration-test-output_<run_id>.txt```

4. Output of a successful run should have the following in the file 
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