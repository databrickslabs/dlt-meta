---
title: "Additionals"
date: 2021-08-04T14:25:26-04:00
weight: 19
draft: false
---
 This is easist way to launch dlt-meta to your databricks workspace with following steps.

## Run Integration Tests
1. Launch Terminal/Command promt

2. Goto to DLT-META directory

3. Create environment variables.

```
export DATABRICKS_HOST=<DATABRICKS HOST>
export DATABRICKS_TOKEN=<DATABRICKS TOKEN> # Account needs permission to create clusters/dlt pipelines.
```

4. Commit your local changes to your remote branch used above

5. Run integration test against cloudfile or eventhub or kafka using below options:
    5a. Run the command for cloudfiles ```python integration-tests/run-integration-test.py  --cloud_provider_name=aws --dbr_version=11.3.x-scala2.12 --source=cloudfiles --dbfs_path=dbfs:/tmp/DLT-META/```

    5b. Run the command for eventhub ```python integration-tests/run-integration-test.py --cloud_provider_name=azure --dbr_version=11.3.x-scala2.12 --source=eventhub --dbfs_path=dbfs:/tmp/DLT-META/ --eventhub_name=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer ----eventhub_consumer_accesskey_name=consumer```

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


    5c. Run the command for kafka ```python3 integration-tests/run-integration-test.py --cloud_provider_name=aws --dbr_version=11.3.x-scala2.12 --source=kafka --dbfs_path=dbfs:/tmp/DLT-META/ --kafka_topic_name=dlt-meta-integration-test --kafka_broker=host:9092```

        For kafka integration tests, the following are the prerequisites:
        1. Needs kafka instance running

        Following are the mandatory arguments for running EventHubs integration test
        1. Provide your kafka topic name : ```--kafka_topic_name```
        2. Provide kafka_broker  ```--kafka_broker```

6. Once finished integration output file will be copied locally to 
```integration-test-output_<run_id>.txt```

7. Output of a successful run should have the following in the file 
```
Generating Onboarding Json file for Integration Test.
Successfully Generated Onboarding Json file for Integration Test.
Setting up dlt-meta metadata tables.
Successfully setup dlt-meta metadata tables.
Completed Bronze DLT Pipeline.
Completed Silver DLT Pipeline.
Validating DLT Bronze and Silver Table Counts...
Validating Counts for Table bronze_f7d4934efe494de987f364e8d93acaba.transactions_cdc.
Expected: 10002 Actual: 10002. Passed!
Validating Counts for Table bronze_f7d4934efe494de987f364e8d93acaba.transactions_cdc_quarantine.
Expected: 9842 Actual: 9842. Passed!
Validating Counts for Table bronze_f7d4934efe494de987f364e8d93acaba.customers_cdc.
Expected: 98928 Actual: 98928. Passed!
Validating Counts for Table silver_f7d4934efe494de987f364e8d93acaba.transactions.
Expected: 8759 Actual: 8759. Passed!
Validating Counts for Table silver_f7d4934efe494de987f364e8d93acaba.customers.
Expected: 87256 Actual: 87256. Passed!
DROPPING DB bronze_f7d4934efe494de987f364e8d93acaba
DROPPING DB silver_f7d4934efe494de987f364e8d93acaba
DROPPING DB dlt_meta_framework_it_f7d4934efe494de987f364e8d93acaba_f7d4934efe494de987f364e8d93acaba
Removed Integration test databases
```