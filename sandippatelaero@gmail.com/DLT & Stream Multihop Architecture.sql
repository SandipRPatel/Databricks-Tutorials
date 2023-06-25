-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %run /Users/sandippatelaero@gmail.com/dataset_bookstore

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore'

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').option('cloudFiles.schemaLocation','dbfs:/mnt/demo/checkpoints/orders_raw').load('dbfs:/mnt/demo-datasets/bookstore/orders-raw').createOrReplaceTempView('orders_raw_temp_view_readstream')
-- MAGIC )

-- COMMAND ----------

create or replace temp view bronze_temp_view_orders_raw
as (select *,CURRENT_TIMESTAMP() as arrival_time,input_file_name() as source_file from orders_raw_temp_view_readstream);
select * from bronze_temp_view_orders_raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.table('bronze_temp_view_orders_raw').writeStream.format('delta').option('checkpointLocation','dbfs:/mnt/demo/checkpoints/orders_bronze').outputMode('append').table('bronze_delta_orders_raw')
-- MAGIC
-- MAGIC
-- MAGIC )

-- COMMAND ----------

select * from bronze_delta_orders_raw

-- COMMAND ----------

describe extended bronze_delta_orders_raw

-- COMMAND ----------

select count(*) as total_cnt from bronze_delta_orders_raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()
-- MAGIC # here we add one more file of 1000 records

-- COMMAND ----------

select count(*) as total_cnt from bronze_delta_orders_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.read.format('json').load('dbfs:/mnt/demo-datasets/bookstore/customers-json').createOrReplaceTempView("customers_json_temp_view")
-- MAGIC
-- MAGIC )

-- COMMAND ----------

select * from customers_json_temp_view;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.table('bronze_delta_orders_raw').createOrReplaceTempView('bronze_delta_orders_raw_temp_view')

-- COMMAND ----------

select count(*) as cnt from bronze_delta_orders_raw_temp_view;

-- COMMAND ----------

select * from bronze_delta_orders_raw_temp_view;

-- COMMAND ----------

create or replace temp view silver_order_enriched_temp_view as
(select order_id,quantity,o.customer_id,profile:first_name as f_name,profile:last_name as l_name,
cast(from_unixtime(order_timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp) as oredr_timestamp,books
from bronze_delta_orders_raw_temp_view as o inner join customers_json_temp_view as c
on o.customer_id = c.customer_id where quantity > 0 );


-- COMMAND ----------

select count(*) as cnt from silver_order_enriched_temp_view;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.table('silver_order_enriched_temp_view').writeStream.format('delta').option('checkpointLocation','dbfs:/mnt/demo/checkpoints/orders_silver').outputMode('append').table('silver_delta_orders_customer')
-- MAGIC
-- MAGIC )

-- COMMAND ----------

select * from silver_delta_orders_customer;

-- COMMAND ----------

select count(*) as silver_cnt from silver_delta_orders_customer;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # lets add new data file
-- MAGIC load_new_data()

-- COMMAND ----------

select count(*) as silver_cnt from silver_delta_orders_customer;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream.table('silver_delta_orders_customer').createOrReplaceTempView('gold_order_customer_temp_view')
-- MAGIC
-- MAGIC
-- MAGIC )

-- COMMAND ----------

create or replace temp view gold_order_customer_count_temp_view as
select customer_id,f_name,l_name,date_trunc("DD",oredr_timestamp) as order_date,sum(quantity) as books_count
from gold_order_customer_temp_view group by customer_id,f_name,l_name,date_trunc("DD",oredr_timestamp);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.table("gold_order_customer_count_temp_view").writeStream.format('delta').outputMode('complete').option('checkpointLocation','dbfs:/mnt/demo/checkpoints/daily_customer_books').table('gold_delta_order_customer')
-- MAGIC )
-- MAGIC # we can use .trigger(availableNow=True), if we want to use for batch and streaming data

-- COMMAND ----------

select count(*) as gold_cnt from gold_delta_order_customer;

-- COMMAND ----------

select * from gold_delta_order_customer order by books_count desc;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

select * from gold_delta_order_customer order by books_count desc;

-- COMMAND ----------

select count(*) as gold_cnt from gold_delta_order_customer;

-- COMMAND ----------

select * from gold_delta_order_customer;

-- COMMAND ----------

select count(*) as gold_cnt from silver_delta_orders_customer;

-- COMMAND ----------

describe history gold_delta_order_customer;

-- COMMAND ----------

select * from gold_delta_order_customer@v1

-- COMMAND ----------

select * from gold_delta_order_customer@v2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Note : after adding one more file in gold table, data update happened. total number of books count is increased. we can see in table history

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### stop all streaming

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in spark.streams.active:
-- MAGIC     print(i)
-- MAGIC     print("stopping stream: ",i.id)
-- MAGIC     i.stop()
-- MAGIC     i.awaitTermination()

-- COMMAND ----------


