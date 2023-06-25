-- Databricks notebook source
-- MAGIC %py
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %run /Users/sandippatelaero@gmail.com/dataset_bookstore

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from csv.`dbfs:/mnt/demo-datasets/bookstore/books-streaming/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Read Streaming Tables

-- COMMAND ----------

drop table if exists book_csv_stream;
create table book_csv_stream
(book_id string,title string,author string,category string,price double)
using csv
options (header='true',delimiter=';')
location 'dbfs:/mnt/demo-datasets/bookstore/books-csv/';
select * from book_csv_stream;

-- COMMAND ----------

describe extended book_csv_stream;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.readStream.table('book_csv_stream').createOrReplaceTempView('book_csv_stream_temp_view')

-- COMMAND ----------

select * from book_csv_stream_temp_view;

-- COMMAND ----------

-- lets apply some aggregation on streaming temp view
select author,count(book_id) as cnt
from book_csv_stream_temp_view group by author order by author;

-- COMMAND ----------

-- sorting and deduplication is not working for streaming read:
select * from book_csv_stream_temp_view order by author;

-- COMMAND ----------

select distinct * from book_csv_stream_temp_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write Streaming Tables

-- COMMAND ----------

create or replace temp view book_author_count_temp_view
as (select author,count(book_id) as total_books
from book_csv_stream_temp_view group by author order by author);
select * from book_author_count_temp_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Trigger(processingTime='5 seconds')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.table('book_author_count_temp_view').writeStream.trigger(processingTime='5 seconds').outputMode('complete').option('checkpointLocation','dbfs:/mnt/demo/author_count_checkpoint').table('book_author_count_writestream')

-- COMMAND ----------

select * from book_author_count_writestream;

-- COMMAND ----------

-- lets add some new data into book streaming table
insert into book_csv_stream
values ('B19','A','AA','AAA',11),('B20','B','BB','BBB',22),('B21','C','CC','CCC',33),('B22','D','DD','DDD',44),('B23','E','EE','EEE',55);


-- COMMAND ----------

select * from book_author_count_writestream;


-- COMMAND ----------

insert into book_csv_stream
values ('B19','1A','1AA','1AAA',11),('B20','2B','2BB','2BBB',22),('B21','3C','3CC','3CCC',33),('B22','4D','4DD','4DDD',44),('B23','5E','5EE','5EEE',55);


-- COMMAND ----------


select * from book_author_count_writestream;


-- COMMAND ----------

describe extended book_author_count_writestream;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### trigger(availableNow=True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.table('book_author_count_temp_view').writeStream.trigger(availableNow=True).outputMode('complete').option("checkpointLocation",'dbfs:/mnt/demo/author_count_checkpoint_2').table('book_author_count_writestream_2').awaitTermination()
-- MAGIC
-- MAGIC # in availableNow=True, it will load data when new data arrived and it will run in batch mode.
-- MAGIC # if there is no new data added and you will run this, it will finished in sort periods.

-- COMMAND ----------

select * from book_author_count_writestream_2;

-- COMMAND ----------

insert into book_csv_stream
values ('B29','1A','sandip','1AAA',11);

-- COMMAND ----------

select * from book_author_count_writestream_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### trigger(once=True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.table('book_author_count_temp_view').writeStream.trigger(once=True).outputMode('complete').option("checkpointLocation",'dbfs:/mnt/demo/author_count_checkpoint_3').table('book_author_count_writestream_3').awaitTermination()

-- COMMAND ----------

select * from book_author_count_writestream_3;

-- COMMAND ----------

insert into book_csv_stream
values ('B30','111A','sandip','1111AAA',11111);

-- COMMAND ----------

select * from book_author_count_writestream_3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Auto Loader

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/orders-raw'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').option('cloudFiles.schemaLocation','dbfs:/mnt/demo/orders_checkpoint').load('dbfs:/mnt/demo-datasets/bookstore/orders-raw')
-- MAGIC .writeStream.option('checkpointLocation','dbfs:/mnt/demo/orders_checkpoint').table('order_updated_auto_loader')
-- MAGIC )

-- COMMAND ----------

describe extended order_updated_auto_loader;

-- COMMAND ----------

select * from order_updated_auto_loader;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/orders-raw'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # now lets add some more parquet file in this location 'dbfs:/mnt/demo-datasets/bookstore/orders-raw'
-- MAGIC # here we add two new files, each with 1000 records
-- MAGIC load_new_data()

-- COMMAND ----------

select * from order_updated_auto_loader;
-- here one file added

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/orders-raw'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/demo-datasets/bookstore/orders-raw/03.parquet',True)

-- COMMAND ----------

describe history order_updated_auto_loader;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def stop_all_streams():
-- MAGIC     for s in spark.stream.active:
-- MAGIC         try:
-- MAGIC             s.stop()
-- MAGIC             print(s)
-- MAGIC         except:
-- MAGIC             pass
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Convert to Delta Parquet

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sh mkdir -p /dbfs/tmp/delta_demo/loans_parquet/; wget -o /dbfs/tmp/delta_demo/loans_parquet/loans.parquet https://pages.databricks.com/rs/094/-YMS-629/images/SAISEU19-loan-risks.snappy.parquet

-- COMMAND ----------

-- MAGIC %fs ls '/dbfs/tmp/delta_demo/loans_parquet'

-- COMMAND ----------

convert to delta parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-raw/01.parquet`

-- COMMAND ----------


