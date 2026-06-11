# SQL Server Connection Setup for DLT-META

This guide explains how to set up SQL Server connections for use with DLT-META's `sqlserver` source format.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Server database accessible from Databricks
- Appropriate permissions to create connections in Databricks

## Step 1: Create Databricks Connection

### Using Databricks UI:

1. Navigate to **Catalog** → **External Data** → **Connections**
2. Click **Create Connection**
3. Select **JDBC** as the connection type
4. Fill in the connection details:

```yaml
connections:
  my_sqlserver_connection:
    name: my_sqlserver_connection
    connection_type: JDBC
    options:
      url: "jdbc:sqlserver://<host>:<port>;databaseName=<database>"
      user: "{{secrets/my-secret-scope/db-username}}"
      password: "{{secrets/my-secret-scope/db-password}}"
```

### Using Databricks CLI:

```bash
databricks connections create \
  --name "my_sqlserver_connection" \
  --connection-type "JDBC" \
  --options '{"url": "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=MyDatabase", "user": "{{secrets/my-secret-scope/db-username}}", "password": "{{secrets/my-secret-scope/db-password}}"}'
```

### Using Terraform:

```hcl
resource "databricks_connection" "sqlserver" {
  name            = "my_sqlserver_connection"
  connection_type = "JDBC"
  options = {
    url      = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=MyDatabase"
    user     = "{{secrets/my-secret-scope/db-username}}"
    password = "{{secrets/my-secret-scope/db-password}}"
  }
}
```

## Step 2: Set Up Databricks Secrets

Create a secret scope and add your database credentials:

```bash
# Create secret scope
databricks secrets create-scope my-secret-scope

# Add username and password
databricks secrets put-secret my-secret-scope db-username
databricks secrets put-secret my-secret-scope db-password
```

## Step 3: Configure DLT-META Source Details

In your onboarding configuration file, use the connection-based approach:

```json
{
   "source_format": "sqlserver",
   "source_details": {
      "connection_name": "my_sqlserver_connection",
      "table": "dbo.Users"
   }
}
```

### For Custom Queries:

```json
{
   "source_format": "sqlserver", 
   "source_details": {
      "connection_name": "my_sqlserver_connection",
      "query": "SELECT * FROM dbo.Users WHERE created_date >= '2024-01-01'"
   }
}
```

## Step 4: Additional JDBC Options

You can specify additional JDBC options in `bronze_reader_options`:

```json
{
   "bronze_reader_options": {
      "fetchsize": "10000",
      "batchsize": "10000",
      "partitionColumn": "user_id",
      "lowerBound": "1",
      "upperBound": "1000000",
      "numPartitions": "10"
   }
}
```

## Connection URL Examples

### SQL Server on Azure:
```
jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=MyDB
```

### SQL Server with Windows Authentication:
```
jdbc:sqlserver://myserver:1433;databaseName=MyDB;integratedSecurity=true
```

### SQL Server with Additional Options:
```
jdbc:sqlserver://myserver:1433;databaseName=MyDB;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30
```

## Security Best Practices

1. **Always use secrets** for credentials - never hardcode passwords
2. **Use service accounts** with minimal required permissions
3. **Enable encryption** in your JDBC URL when possible
4. **Regularly rotate** database credentials
5. **Limit network access** to your SQL Server from Databricks

## Troubleshooting

### Connection Test:
```python
# Test the connection in a Databricks notebook
df = spark.read \
  .format("jdbc") \
  .option("connection", "my_sqlserver_connection") \
  .option("dbtable", "(SELECT 1 as test_col) as test_query") \
  .load()
  
df.show()
```

### Common Issues:

1. **Connection timeout**: Check network connectivity and firewall rules
2. **Authentication failed**: Verify credentials in secret scope
3. **Driver not found**: Ensure SQL Server JDBC driver is available
4. **SSL/TLS errors**: Configure encryption settings in JDBC URL

## Performance Optimization

For large tables, consider:

1. **Partitioning**: Use `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`
2. **Batch size**: Tune `fetchsize` and `batchsize` parameters
3. **Indexing**: Ensure proper indexes on partition columns
4. **Query optimization**: Use selective WHERE clauses in custom queries

## Example Complete Configuration

```json
{
   "data_flow_id": "300",
   "data_flow_group": "SQL1",
   "source_system": "SQL Server",
   "source_format": "sqlserver",
   "source_details": {
      "connection_name": "my_sqlserver_connection",
      "table": "dbo.Users"
   },
   "bronze_catalog_prod": "my_catalog",
   "bronze_database_prod": "bronze_db",
   "bronze_table": "users",
   "bronze_reader_options": {
      "fetchsize": "10000",
      "batchsize": "10000",
      "partitionColumn": "user_id",
      "lowerBound": "1",
      "upperBound": "1000000",
      "numPartitions": "4"
   }
}
```

