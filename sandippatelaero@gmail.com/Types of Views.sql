-- Databricks notebook source
-- MAGIC %py
-- MAGIC spark

-- COMMAND ----------

create table if not exists smartphones
(id int,name string,brand string,price bigint);
insert into smartphones (id,name,brand,price) values (1,'iPhone13','apple',100),(2,'oppo1','oppo',987),(3,'vivo1','vivo',460),(4,'A50','Samsung',300),(5,'iPhone13','apple',470),(6,'iPhone13','apple',781),(7,'iPhone13','apple',100),(8,'realme','oppo',450),(9,'iPhone15','apple',120),(10,'iPhone14','apple',100);
select * from smartphones


-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Views

-- COMMAND ----------

create view smartphones_view
as select * from smartphones;

-- COMMAND ----------

select * from smartphones_view;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Temp View

-- COMMAND ----------

create temp view smartphones_temp_views
as select * from smartphones where id in (2,4,6,7);
select * from smartphones_temp_views;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Global Temp Views

-- COMMAND ----------

create global temporary view smartphones_global_view
as select * from smartphones where price >=500;
select * from global_temp.smartphones_global_view;

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

drop view if exists global_temp.smartphones_global_view

-- COMMAND ----------

drop table if exists smartphones_temp_views;

-- COMMAND ----------

drop view if exists smartphones_views;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print('Done!')

-- COMMAND ----------


