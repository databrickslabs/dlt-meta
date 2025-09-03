# Databricks notebook source
# DBTITLE 1,Install kafka python lib
# MAGIC %pip install kafka-python

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

# DBTITLE 1,Extract input from notebook params
dbutils.widgets.text("kafka_source_topic", "kafka_source_topic", "")
dbutils.widgets.text("kafka_source_servers_secrets_scope_name", "kafka_source_servers_secrets_scope_name", "")
dbutils.widgets.text("kafka_source_servers_secrets_scope_key", "kafka_source_servers_secrets_scope_key", "")
dbutils.widgets.text("kafka_input_data", "kafka_input_data", "")
kafka_source_topic = dbutils.widgets.get("kafka_source_topic")
kafka_source_servers_secrets_scope_name = dbutils.widgets.get("kafka_source_servers_secrets_scope_name")
kafka_source_servers_secrets_scope_key = dbutils.widgets.get("kafka_source_servers_secrets_scope_key")
kafka_input_data = dbutils.widgets.get("kafka_input_data")

print(f"kafka_source_topic: {kafka_source_topic}, kafka_source_servers_secrets_scope_name: {kafka_source_servers_secrets_scope_name}, kafka_source_servers_secrets_scope_key: {kafka_source_servers_secrets_scope_key}, kafka_input_data: {kafka_input_data}")

# COMMAND ----------

# DBTITLE 1,Initialize kafka producer
from kafka import KafkaProducer
import json
kafka_bootstrap_servers = dbutils.secrets.get(f"{kafka_source_servers_secrets_scope_name}", f"{kafka_source_servers_secrets_scope_key}")
for char in kafka_bootstrap_servers:
producer = KafkaProducer(
    bootstrap_servers=f"{kafka_bootstrap_servers}",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# COMMAND ----------

# DBTITLE 1,Send Messages
with open(f"{kafka_input_data}") as f:
    data = json.load(f)

for event in data:
    producer.send(kafka_source_topic, event)

producer.close()
