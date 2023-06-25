-- Databricks notebook source
-- MAGIC %py
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Default Schema &Default storage

-- COMMAND ----------

create table managed_default
(width int,length int,height float);

insert into managed_default (width,length,height) values (2,3,34.21),(4,7,12.20);

-- COMMAND ----------

describe extended managed_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.read.format('delta').load("dbfs:/user/hive/warehouse/managed_default")
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df.write.mode('overwrite').saveAsTable("managed_default_2")

-- COMMAND ----------

describe extended managed_default_2

-- COMMAND ----------

use default;
create table external_default
(width int,length int,height int)
location 'dbfs:/spatel/managed_external/external_default';

insert into external_default (width,length,height) values (20,30,300.20),(60,80,403.09);
select * from external_default;

-- COMMAND ----------

describe extended external_default

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df2 = spark.read.format('delta').load("dbfs:/spatel/managed_external/external_default")
-- MAGIC df2.write.mode('overwrite').option('path','dbfs:/spatel/managed_external/external_default_2').saveAsTable("external_default_2")

-- COMMAND ----------

describe extended external_default_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Custom Schema &Default storage

-- COMMAND ----------

create schema custom_schema_default_storage;

-- COMMAND ----------

use custom_schema_default_storage;
create table managed_custom_schema
(width int,length int,height float);

insert into managed_custom_schema (width,length,height) values (2,3,34.21),(4,7,12.20);

-- COMMAND ----------

describe extended managed_custom_schema;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df3 = spark.read.format('delta').load('dbfs:/user/hive/warehouse/custom_schema_default_storage.db/managed_custom_schema')
-- MAGIC df3.write.mode('overwrite').saveAsTable('custom_schema_default_storage.managed_custom_schema_2')

-- COMMAND ----------

describe extended managed_custom_schema_2;

-- COMMAND ----------

create table external_custom_schema (width int,length int,height float,name string)
location 'dbfs:/spatel/managed_external/external_custom';
insert into external_custom_schema (width,length,height,name) values (2,3,34.21,'sandip'),(4,7,12.20,'komal');
describe extended external_custom_schema;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/spatel/managed_external/external_custom'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Custom Schema Storage

-- COMMAND ----------

create database custom_schema_storage
location 'dbfs:/spatel/managed_external/custom_schema_storage.db'

-- COMMAND ----------

use custom_schema_storage;
create table managed_custom_schema_storage
(width int,length int,height float,school string);
insert into managed_custom_schema_storage (width,length,height,school) values (2,3,34.21,'Adarsh'),(4,7,12.20,'ksv');
describe extended managed_custom_schema_storage;

-- COMMAND ----------

create table external_custom_schema_storage
(width int,length int,height float,school string)
location 'dbfs:/spatel/managed_external/external_custom_storage';
insert into external_custom_schema_storage (width,length,height,school) values (2,3,34.21,'Adarsh'),(4,7,12.20,'ksv');
describe extended external_custom_schema_storage;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df4 = spark.read.format('delta').load('dbfs:/spatel/managed_external/external_custom_storage')
-- MAGIC df4.write.mode('overwrite').option('path','dbfs:/spatel/managed_external/external_custom_storage_2').saveAsTable("external_custom_schema_storage_2")
-- MAGIC # here we havent mentioned schema name but stil its writing into custom_schema_storage, because its active schema

-- COMMAND ----------

describe extended external_custom_schema_storage_2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Drop Managed and External Tables

-- COMMAND ----------

select * from default.external_default

-- COMMAND ----------

select * from default.managed_default

-- COMMAND ----------

drop table default.external_default;
drop table default.managed_default;

-- COMMAND ----------

select * from default.external_default;

-- COMMAND ----------

select * from default.managed_default;

-- COMMAND ----------

describe extended default.external_default

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ext_file_read = spark.read.format('delta').load("dbfs:/spatel/managed_external/external_default")
-- MAGIC ext_file_read.show()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC managed_file_read = spark.read.format('delta').load("dbfs:/user/hive/warehouse/managed_default")
-- MAGIC managed_file_read.show()

-- COMMAND ----------


