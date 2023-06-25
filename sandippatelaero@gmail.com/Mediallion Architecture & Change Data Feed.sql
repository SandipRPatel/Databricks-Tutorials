-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

create schema mediallion;
use mediallion

-- COMMAND ----------

drop table if exists bronze_eps01;
drop table if exists silver_eps01;
drop table if exists gold_consensus_eps01;

create table bronze_eps01
(date string,stock_symbol string, analyst int,estimated_eps double)
using delta
tblproperties (delta.enableChangeDataFeed = 'true');

create table silver_eps01
(date string,stock_symbol string, analyst int,estimated_eps double)
using delta
tblproperties (delta.enableChangeDataFeed = 'true');

create table gold_consensus_eps01
(date string,stock_symbol string,consensus_eps double)
using delta
tblproperties (delta.enableChangeDataFeed = 'true');


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.createDataFrame([
-- MAGIC     ('3/1/2021','a',1,2.2),
-- MAGIC     ('3/1/2021','a',2,2.0),
-- MAGIC     ('3/1/2021','b',1,1.3),
-- MAGIC     ('3/1/2021','b',2,1.2),
-- MAGIC     ('3/1/2021','c',1,3.5),
-- MAGIC     ('3/1/2021','c',2,2.6)
-- MAGIC ],('date','stock_symbol','analyst','estimated_eps'))
-- MAGIC df.createOrReplaceTempView('bronze_eps_march_dataset01')
-- MAGIC

-- COMMAND ----------

insert into bronze_eps01 table bronze_eps_march_dataset01

-- COMMAND ----------

describe extended bronze_eps01;

-- COMMAND ----------

describe history bronze_eps01;

-- COMMAND ----------

select * from table_changes('bronze_eps01',1)

-- COMMAND ----------

-- create silver table from bronze
insert into silver_eps01
select date,stock_symbol,analyst,estimated_eps
from table_changes('bronze_eps01',1);

-- COMMAND ----------

select * from table_changes('silver_eps01',1)

-- COMMAND ----------

-- create gold table from silver

insert into gold_consensus_eps01
select silver_eps01.date,silver_eps01.stock_symbol,avg(estimated_eps) as consensus_eps
from silver_eps01
inner join (select distinct date,stock_symbol from table_changes('silver_eps01',1)) as silver_cdf01
on silver_eps01.date = silver_cdf01.date and silver_eps01.stock_symbol = silver_cdf01.stock_symbol
group by silver_eps01.date,silver_eps01.stock_symbol;


-- COMMAND ----------

select * from gold_consensus_eps01

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Append Records in Bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.createDataFrame([
-- MAGIC     ('3/1/2021','a',2,2.4),
-- MAGIC     ('4/1/2021','a',1,2.3),
-- MAGIC     ('4/1/2021','a',2,2.1),
-- MAGIC     ('4/1/2021','b',1,1.3),
-- MAGIC     ('4/1/2021','b',2,1.2),
-- MAGIC     ('4/1/2021','c',1,3.5),
-- MAGIC     ('4/1/2021','c',2,2.6)
-- MAGIC ],('date','stock_symbol','analyst','estimated_eps'))
-- MAGIC df.createOrReplaceTempView('bronze_eps_april_dataset01')
-- MAGIC
-- MAGIC

-- COMMAND ----------

insert into bronze_eps01 table bronze_eps_april_dataset01;

-- COMMAND ----------

select * from table_changes('bronze_eps01',2)

-- COMMAND ----------

merge into silver_eps01
using (select * from table_changes('bronze_eps01',2)) as bronze_cdf01
on silver_eps01.date = bronze_cdf01.date and silver_eps01.stock_symbol = bronze_cdf01.stock_symbol and silver_eps01.analyst = bronze_cdf01.analyst
when matched then
update set silver_eps01.estimated_eps = bronze_cdf01.estimated_eps
when not matched then
insert (date,stock_symbol,analyst,estimated_eps) values (date,stock_symbol,analyst,estimated_eps)

-- COMMAND ----------

describe history silver_eps01 

-- COMMAND ----------

select * from table_changes('silver_eps01',2)

-- COMMAND ----------

select * from silver_eps01 order by date,stock_symbol;
-- when we query final silver table, it will show inserted and updaetd rows

-- COMMAND ----------

merge into gold_consensus_eps01
using
(select silver_eps01.date,silver_eps01.stock_symbol,avg(estimated_eps) as consensus_eps from silver_eps01
inner join (select distinct date,stock_symbol from table_changes('silver_eps01',2)) as silver_cdf01
on silver_eps01.date = silver_cdf01.date and silver_eps01.stock_symbol = silver_cdf01.stock_symbol
group by silver_eps01.date,silver_eps01.stock_symbol) as silver_cdf_agg01
on silver_cdf_agg01.date = gold_consensus_eps01.date and silver_cdf_agg01.stock_symbol = gold_consensus_eps01.stock_symbol
when matched then 
update set gold_consensus_eps01.consensus_eps = silver_cdf_agg01.consensus_eps
when not matched then
insert (date,stock_symbol,consensus_eps) values (date,stock_symbol,consensus_eps)


-- COMMAND ----------

select * from table_changes('gold_consensus_eps01',2);

-- COMMAND ----------

select * from gold_consensus_eps01

-- COMMAND ----------


