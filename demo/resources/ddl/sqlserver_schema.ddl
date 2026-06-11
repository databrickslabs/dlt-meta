-- SQL Server table schema example
-- This DDL represents a typical SQL Server table structure that can be ingested via DLT-META

CREATE TABLE sqlserver_data (
    id BIGINT NOT NULL,
    customer_id STRING,
    product_id STRING,
    order_date TIMESTAMP,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    region STRING,
    sales_rep STRING
) USING DELTA
PARTITIONED BY (order_date);

