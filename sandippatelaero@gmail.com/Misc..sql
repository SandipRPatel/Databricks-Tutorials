-- Databricks notebook source
-- MAGIC %py
-- MAGIC spark

-- COMMAND ----------

CREATE TABLE events( id LONG,
                     date STRING, 
                    location STRING,
                    description STRING 
                    ) USING DELTA;

-- COMMAND ----------

describe extended events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC aa = 'default'
-- MAGIC bb = 'events'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC spark.sql(f"select * from {aa}.{bb}")

-- COMMAND ----------

SELECT aggregate(array(1, 2, 3), 1, (acc, x) -> acc + x);

-- COMMAND ----------

SELECT aggregate(array(1, 2, 3), 1, (acc, x) -> acc + x, acc -> acc * 10);

-- COMMAND ----------


