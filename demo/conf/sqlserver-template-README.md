# SQL Server Lakeflow Connect Onboarding Template

This template demonstrates how to configure DLT-META to ingest data from SQL Server using Databricks connections with Lakeflow support.

## Template Files

- `sqlserver-lakeflow-connect-onboarding.template` - Main onboarding configuration with embedded YAML connection definition
- `../resources/ddl/sqlserver_schema.ddl` - SQL Server table schema definition
- `dqe/sqlserver/bronze_data_quality_expectations.json` - Data quality rules for bronze layer
- `silver_transformations_sqlserver.json` - Silver layer transformations

## Template Variables

Replace these placeholders with actual values when using the template:

### Required Variables
| Variable | Description | Example |
|----------|-------------|---------|
| `{sqlserver_connection_name}` | Databricks connection name | `my_sqlserver_connection` |
| `{sqlserver_host}` | SQL Server hostname/IP | `myserver.database.windows.net` |
| `{sqlserver_port}` | SQL Server port | `1433` |
| `{sqlserver_database}` | SQL Server database name | `AdventureWorks` |
| `{sqlserver_secret_scope}` | Databricks secret scope name | `sqlserver-secrets` |
| `{sqlserver_table_name}` | SQL Server table name | `dbo.Orders` |
| `{uc_catalog_name}` | Unity Catalog name | `production` |
| `{bronze_schema}` | Bronze schema name | `bronze_db` |
| `{silver_schema}` | Silver schema name | `silver_db` |
| `{uc_volume_path}` | Unity Catalog volume path | `/Volumes/prod/default/dltmeta` |
| `{run_id}` | Unique run identifier | `20241201_001` |

### Optional Performance Variables
| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `{sqlserver_partition_column}` | Column for JDBC partitioning | - | `order_id` |
| `{sqlserver_partition_lower_bound}` | Partition lower bound | - | `1` |
| `{sqlserver_partition_upper_bound}` | Partition upper bound | - | `1000000` |
| `{sqlserver_num_partitions}` | Number of partitions | - | `10` |
| `{sqlserver_bronze_partition_columns}` | Bronze table partition columns | - | `order_date` |

### Sink Configuration Variables
| Variable | Description | Example |
|----------|-------------|---------|
| `{kafka_sink_servers_secret_scope_name}` | Kafka sink secret scope | `kafka_secrets` |
| `{kafka_sink_servers_secret_scope_key}` | Kafka sink secret key | `bootstrap_servers` |
| `{kafka_sink_topic}` | Kafka sink topic | `sqlserver_orders` |
| `{sqlserver_sink_where_clause}` | Filter clause for sinks | `total_amount > 100` |

## Prerequisites

1. **Secret Scope**: Create secrets for database credentials
   ```bash
   databricks secrets create-scope sqlserver-secrets
   databricks secrets put-secret sqlserver-secrets db-username
   databricks secrets put-secret sqlserver-secrets db-password
   ```

2. **Connection Configuration**: The template includes the connection definition in YAML format:
   ```yaml
   connections:
     my_sqlserver_connection:
       name: my_sqlserver_connection
       connection_type: JDBC
       options:
         url: "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=AdventureWorks"
         user: "{{secrets/sqlserver-secrets/db-username}}"
         password: "{{secrets/sqlserver-secrets/db-password}}"
   ```
   
   This connection definition is embedded as a YAML string in the template's `source_details.connections` field and will be processed by DLT-META.

3. **Unity Catalog**: Ensure Unity Catalog is enabled and configured

## Usage Example

1. **Replace template variables** in `sqlserver-lakeflow-connect-onboarding.template`:
   ```bash
   sed -i 's/{sqlserver_connection_name}/my_sqlserver_connection/g' sqlserver-lakeflow-connect-onboarding.template
   sed -i 's/{sqlserver_host}/myserver.database.windows.net/g' sqlserver-lakeflow-connect-onboarding.template
   sed -i 's/{sqlserver_port}/1433/g' sqlserver-lakeflow-connect-onboarding.template
   sed -i 's/{sqlserver_database}/AdventureWorks/g' sqlserver-lakeflow-connect-onboarding.template
   sed -i 's/{sqlserver_secret_scope}/sqlserver-secrets/g' sqlserver-lakeflow-connect-onboarding.template
   sed -i 's/{sqlserver_table_name}/dbo.Orders/g' sqlserver-lakeflow-connect-onboarding.template
   # ... replace other variables
   ```

2. **Run DLT-META onboarding**:
   ```bash
   python src/cli.py onboard \
     --onboarding_file_path demo/conf/sqlserver-lakeflow-connect-onboarding.json \
     --uc_catalog_name production \
     --bronze_schema bronze_db \
     --silver_schema silver_db
   ```

3. **Deploy DLT pipeline**:
   ```bash
   python src/cli.py deploy \
     --layer bronze_silver \
     --pipeline_name sqlserver_ingestion \
     --uc_catalog_name production
   ```

## Features Included

### Bronze Layer
- ✅ SQL Server data ingestion via Databricks connection
- ✅ Data quality expectations with quarantine handling
- ✅ Partitioning support for performance
- ✅ Dual sinks (Kafka + Delta) for real-time and batch processing

### Silver Layer  
- ✅ Data transformations (calculated columns, date parts, categorization)
- ✅ Business logic implementation
- ✅ Performance optimizations

### Data Quality
- ✅ Record validation (NOT NULL checks)
- ✅ Business rule validation (positive amounts, valid status values)
- ✅ Quarantine handling for invalid records

### Performance Optimizations
- ✅ JDBC partitioning for parallel reads
- ✅ Configurable fetch and batch sizes
- ✅ Table partitioning strategies

## Customization

### Adding Custom Transformations
Edit `silver_transformations_sqlserver.json` to add business-specific calculations:
```json
{
  "column_name": "profit_margin",
  "expression": "(total_amount - cost_amount) / total_amount * 100",
  "data_type": "decimal(5,2)"
}
```

### Modifying Data Quality Rules
Update `dqe/sqlserver/bronze_data_quality_expectations.json`:
```json
{
  "expect_or_drop": {
    "business_rule": "order_date >= '2024-01-01' AND region IS NOT NULL"
  }
}
```

### Custom SQL Queries
Instead of specifying a table, use a custom query in source_details:
```json
{
  "source_details": {
    "connection_name": "my_sqlserver_connection",
    "query": "SELECT * FROM dbo.Orders WHERE order_date >= DATEADD(day, -30, GETDATE())"
  }
}
```

## Monitoring and Troubleshooting

1. **Check connection**: Test your Databricks connection before running
2. **Monitor DLT pipeline**: Use Databricks DLT UI for pipeline monitoring  
3. **Review quarantine tables**: Check quarantine tables for data quality issues
4. **Performance tuning**: Adjust partition settings based on data volume

## Example Complete Configuration

After variable substitution, the `source_details` will contain:

```json
{
   "data_flow_id": "400",
   "data_flow_group": "SQL1",
   "source_system": "SQL Server",
   "source_format": "sqlserver",
   "source_details": {
      "connections": "connections:
  my_sqlserver_connection:
    name: my_sqlserver_connection
    connection_type: JDBC
    options:
      url: \"jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=AdventureWorks\"
      user: \"{{secrets/sqlserver-secrets/db-username}}\"
      password: \"{{secrets/sqlserver-secrets/db-password}}\"",
      "connection_name": "my_sqlserver_connection",
      "table": "dbo.Orders"
   },
   "bronze_catalog_demo": "production",
   "bronze_database_demo": "bronze_db",
   "bronze_table": "bronze_20241201_001_sqlserver_data"
}
```

The YAML connection definition will be parsed and processed by DLT-META to create the appropriate Databricks connection.

## Support

For issues or questions:
1. Check the main DLT-META documentation
2. Review Databricks connection setup guide
3. Examine DLT pipeline logs for specific errors
