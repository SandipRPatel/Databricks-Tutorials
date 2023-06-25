-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_raw

-- COMMAND ----------

--create database dlt_pipeline;
use dlt_pipeline;

-- COMMAND ----------

-- use auto loader using sql query and create table
create or refresh streaming live table order_raw_streaming_live_bronze
comment 'the raw books orders, ingested from orders-raw'
as select * from cloud_files("${datasets_path}/orders-raw","parquet",map("schema","order_id string,order_timestamp long,customer_id string,quantity long"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers

-- COMMAND ----------

create or refresh live table customer_live_bronze
comment 'the customers lookup table, ingested from customers-json'
-- as select * from cloud_files("${datasets_path}/orders-raw","parquet",map("schema","order_id string,order_timestamp long,customer_id string,quantity long"))
as select * from json.`${datasets_path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_cleaned

-- COMMAND ----------

create or refresh streaming live table orders_cleaned_streaming_live_silver (constraint valid_order_number expect (order_id is not null) on violation drop row)
comment 'the cleaned books orders with valid order_id'
as
select order_id,quantity,o.customer_id,c.profile:first_name as f_name,c.profile:last_name as l_name,
cast(from_unixtime(order_timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp) as order_timestamp,c.profile:address:country as country
from stream(live.order_raw_streaming_live_bronze) as o left join live.customer_live_bronze as c
on o.customer_id = c.customer_id



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### constraints:
-- MAGIC DROP ROW:: where we discard records that violate constraints &
-- MAGIC FAIL UPDATE:: where the pipeline fail when constraint is violated &
-- MAGIC OMITTED:: records violating constraint will be included, but violation will be reported in the metrics.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Tables

-- COMMAND ----------

create or refresh live table china_books_count_live_gold
comment 'daily numbers of books per customer in china'
as

select customer_id,f_name,l_name,date_trunc('DD',order_timestamp) as order_date,sum(quantity) as books_counts
from live.orders_cleaned_streaming_live_silver
where Country = 'China'
group by customer_id,f_name,l_name,date_trunc('DD',order_timestamp)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## QC
