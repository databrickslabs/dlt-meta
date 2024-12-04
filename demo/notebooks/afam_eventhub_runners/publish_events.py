# Databricks notebook source
# MAGIC %md
# MAGIC ## Install azure-eventhub

# COMMAND ----------

# MAGIC %sh pip install azure-eventhub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("eventhub_name","eventhub_name","")
dbutils.widgets.text("eventhub_name_append_flow","eventhub_name_append_flow","")
dbutils.widgets.text("eventhub_namespace","eventhub_namespace","")
dbutils.widgets.text("eventhub_secrets_scope_name","eventhub_secrets_scope_name","")
dbutils.widgets.text("eventhub_accesskey_name","eventhub_accesskey_name","")
dbutils.widgets.text("eventhub_input_data","eventhub_input_data","")
dbutils.widgets.text("eventhub_append_flow_input_data","eventhub_append_flow_input_data","")

# COMMAND ----------

eventhub_name = dbutils.widgets.get("eventhub_name")
eventhub_name_append_flow = dbutils.widgets.get("eventhub_name_append_flow")
eventhub_namespace = dbutils.widgets.get("eventhub_namespace")
eventhub_secrets_scope_name = dbutils.widgets.get("eventhub_secrets_scope_name")
eventhub_accesskey_name = dbutils.widgets.get("eventhub_accesskey_name")
eventhub_input_data = dbutils.widgets.get("eventhub_input_data")
eventhub_append_flow_input_data = dbutils.widgets.get("eventhub_append_flow_input_data")

# COMMAND ----------

print(f"eventhub_name={eventhub_name}, eventhub_name_append_flow={eventhub_name_append_flow}, eventhub_namespace={eventhub_namespace}, eventhub_secrets_scope_name={eventhub_secrets_scope_name}, eventhub_accesskey_name={eventhub_accesskey_name}, eventhub_input_data={eventhub_input_data}, eventhub_append_flow_input_data={eventhub_append_flow_input_data}")

# COMMAND ----------

import json
from azure.eventhub import EventHubProducerClient, EventData

eventhub_shared_access_value = dbutils.secrets.get(scope = eventhub_secrets_scope_name, key = eventhub_accesskey_name)
eventhub_conn = f"Endpoint=sb://{eventhub_namespace}.servicebus.windows.net/;SharedAccessKeyName={eventhub_accesskey_name};SharedAccessKey={eventhub_shared_access_value}"

client = EventHubProducerClient.from_connection_string(eventhub_conn, eventhub_name=eventhub_name)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish iot data to eventhub

# COMMAND ----------

with open(f"{eventhub_input_data}") as f:
    data = json.load(f)

for event in data:
  event_data_batch = client.create_batch()
  event_data_batch.add(EventData(json.dumps(event)))
  with client:
      client.send_batch(event_data_batch)

# COMMAND ----------

append_flow_client = EventHubProducerClient.from_connection_string(eventhub_conn, eventhub_name=eventhub_name_append_flow)

with open(f"{eventhub_append_flow_input_data}") as f:
    af_data = json.load(f)

for event in af_data:
  event_data_batch = client.create_batch()
  event_data_batch.add(EventData(json.dumps(event)))
  with client:
      append_flow_client.send_batch(event_data_batch)
