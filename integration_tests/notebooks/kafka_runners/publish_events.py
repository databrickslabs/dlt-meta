# Databricks notebook source
# DBTITLE 1,Install kafka python lib
# MAGIC %pip install kafka-python

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

# DBTITLE 1,Extract input from notebook params
dbutils.widgets.text("kafka_topic","kafka_topic","")
dbutils.widgets.text("kafka_broker","kafka_broker","")
dbutils.widgets.text("kafka_input_data","kafka_input_data","")
kafka_topic = dbutils.widgets.get("kafka_topic")
kafka_broker = dbutils.widgets.get("kafka_broker")
kafka_input_data = dbutils.widgets.get("kafka_input_data")
print(f"kafka_topic={kafka_topic}, kafka_broker={kafka_broker}, kafka_input_data={kafka_input_data}")

# COMMAND ----------

# DBTITLE 1,Initialize kafka producer
from kafka import KafkaProducer
import json
producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# COMMAND ----------

# DBTITLE 1,Send Messages
with open(f"{kafka_input_data}") as f:
    data = json.load(f)

for event in data:
  producer.send(kafka_topic,event)

producer.close()