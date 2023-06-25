-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/dlt/demo_bookstore_dlt'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/dlt/demo_bookstore_dlt/system'
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/dlt/demo_bookstore_dlt/system/events'
-- MAGIC

-- COMMAND ----------

select * from delta.`dbfs:/mnt/demo/dlt/demo_bookstore_dlt/system/events`

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/dlt/demo_bookstore_dlt/tables'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/dlt/demo_bookstore_dlt/tables/fr_daily_customer_books/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('delta').load('dbfs:/mnt/demo/dlt/demo_bookstore_dlt/tables/cn_daily_customer_books/')
-- MAGIC df.count()

-- COMMAND ----------

show tables in demo_bookstore_dlt_db

-- COMMAND ----------

describe extended demo_bookstore_dlt_db.orders_raw

-- COMMAND ----------

select * from demo_bookstore_dlt_db.fr_daily_customer_books

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/dlt/demo_bookstore_dlt/checkpoints/orders_cleaned/0/metadata/'

-- COMMAND ----------



-- COMMAND ----------


