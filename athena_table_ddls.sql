-- Athena DDL for silver (Parquet)
CREATE EXTERNAL TABLE IF NOT EXISTS dev.orders_silver (
  order_id int,
  product string,
  quantity int,
  price double,
  total_price double
)
PARTITIONED BY (order_date string)
STORED AS PARQUET
LOCATION 's3://your-bucket/silver/';

-- Athena DDL for gold (aggregated)
CREATE EXTERNAL TABLE IF NOT EXISTS dev.orders_gold (
  product string,
  total_sales double
)
PARTITIONED BY (order_date string)
STORED AS PARQUET
LOCATION 's3://your-bucket/gold/';
