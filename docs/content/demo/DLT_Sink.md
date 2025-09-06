---
title: "Lakeflow Declarative Pipelines Sink Demo"
date: 2024-02-26T14:25:26-04:00
weight: 27
draft: false
---

### Lakeflow Declarative Pipelines Sink Demo
This demo will perform following steps:
- Showcase onboarding process for dlt writing to external sink pattern
- Run onboarding for the bronze iot events
- Publish test events to kafka topic
- Run Bronze Lakeflow Declarative Pipelines which will read from kafka source topic and write to:
  - Events delta table into UC
  - Create quarantine table as per data quality expectations
  - Writes to external kafka topics
  - Writes to external dbfs location as external delta sink

### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:
    
    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

3. Install Python package requirements:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk

    # Development requirements
    pip install flake8==6.0 delta-spark==3.0.0 pytest>=7.0.0 coverage>=7.0.0 pyspark==3.5.5
    ```

4. Clone dlt-meta:
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git 
    ```

5. Navigate to project directory:
    ```commandline
    cd dlt-meta
    ```

6. Set python environment variable into terminal:
    ```commandline
    dlt_meta_home=$(pwd)
    export PYTHONPATH=$dlt_meta_home
    ```

7. Configure Kafka (Optional):
    If you are using secrets for kafka, create databricks secrets scope for source and sink kafka:
    ```commandline 
    databricks secrets create-scope <<n>>
    ```
    ```commandline
    databricks secrets put-secret --json '{
        "scope": "<<n>>",
        "key": "<<keyname>>",
        "string_value": "<<value>>"
    }'
    ```

8. Run the command:
    ```commandline
    python demo/launch_dlt_sink_demo.py --uc_catalog_name=<<uc_catalog_name>> --source=kafka --kafka_source_topic=<<kafka source topic name>> --kafka_sink_topic=<<kafka sink topic name>> --kafka_source_servers_secrets_scope_name=<<kafka source servers secret name>> --kafka_source_servers_secrets_scope_key=<<kafka source server secret scope key name>> --kafka_sink_servers_secret_scope_name=<<kafka sink server secret scope key name>> --kafka_sink_servers_secret_scope_key=<<kafka sink servers secret scope key name>> --profile=<<DEFAULT>>
    ```

![dlt_demo_sink.png](/images/dlt_demo_sink.png)
![dlt_delta_sink.png](/images/dlt_delta_sink.png)
![dlt_kafka_sink.png](/images/dlt_kafka_sink.png)
