-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from datetime import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table employees (id int,name string,department string,salary bigint);
-- MAGIC insert into employees (id,name,department,salary) values (1,'sandip','analytics',320),
-- MAGIC (2,'taksh','school',590),
-- MAGIC (3,'kiyan','sales',2100),
-- MAGIC (4,'ajit','software',450),
-- MAGIC (5,'henal','analytics',100),
-- MAGIC (6,'chintan','civil',3600),
-- MAGIC (7,'jugal','marketing',320),
-- MAGIC (8,'chatur','MBA',780),
-- MAGIC (9,'monika','CS',140),
-- MAGIC (10,'mahesh','medical',1700);
-- MAGIC
-- MAGIC select * from employees;

-- COMMAND ----------

describe extended employees;

-- COMMAND ----------

describe table employees;

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history default.employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/employees")
-- MAGIC df.dtypes

-- COMMAND ----------

select * from delta.`dbfs:/user/hive/warehouse/employees`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transformation : Update, Insert, Delete ...

-- COMMAND ----------

update employees set salary = 555 where name in ('sandip','taksh','kiyan');
describe history employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

delete from employees where name in ('mahesh', 'jugal');

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

-- delete some records which are not exists
delete from employees where name in ('prashant','kiran');
describe history employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ddf = spark.read.table("default.employees") # spark_catalog.default.employees or default.employees
-- MAGIC ddf.show()

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

-- COMMAND ----------

select * from json.`dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Advanced Delta Lake Feature

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Time Travel
-- MAGIC
-- MAGIC query older version of the data using:
-- MAGIC 1) Timestamp
-- MAGIC ::select * from table_name TIMESTAMP AS OF '2019-15-02'
-- MAGIC
-- MAGIC 2) version number
-- MAGIC :: select * from table_name VERSION AS OF 4   OR
-- MAGIC :: select * from table_name@v4
-- MAGIC

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

select * from employees version as of 2

-- COMMAND ----------

select * from employees@v2

-- COMMAND ----------

select * from employees TIMESTAMP AS OF '2023-06-17 13:16:55'

-- COMMAND ----------

select * from employees TIMESTAMP AS OF '2023-06-17T13:17:52.000+0000'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Rollback Versions
-- MAGIC 1) RESTORE TABLE table_name TO TIMESTAMP AS OF '2016-05-02'
-- MAGIC 1) RESTORE TABLE table_name TO VERSION AS OF 4

-- COMMAND ----------

delete from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

select * from employees version as of 2

-- COMMAND ----------

restore table employees version as of 4

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Compactions
-- MAGIC 1) OPTIMIZE table_name
-- MAGIC 2) Indexing : co-locate column information
-- MAGIC 3) OPTIMIZE table_name ZORDER BY column_name

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

optimize employees

-- COMMAND ----------

optimize employees zorder by id

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Truncate Table

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

truncate table employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees version as of 4;
select * from employees;

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Vacuum delta table
-- MAGIC 1) VACUUM table_name [retention_period]

-- COMMAND ----------

VACUUM employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum employees retain 0 hours;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- once we applied vacuum then after we cant use previous version of delta tables.
select * from employees version as of 8

-- COMMAND ----------

vacuum employees retain 0 hours dry run

-- COMMAND ----------

-- once we applied vacuum then after we cant use previous version of delta tables.
select * from employees version as of 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop Table

-- COMMAND ----------

drop table employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------


