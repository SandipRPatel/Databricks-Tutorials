-- Databricks notebook source
-- MAGIC %py
-- MAGIC spark

-- COMMAND ----------

--%fs ls 'dbfs:/mnt/demo-datasets/bookstore'

-- COMMAND ----------

--%fs ls 'dbfs:/mnt/demo-datasets/bookstore/books-cdc/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Databricks notebook source
-- MAGIC def path_exists(path):
-- MAGIC   try:
-- MAGIC     dbutils.fs.ls(path)
-- MAGIC     return True
-- MAGIC   except Exception as e:
-- MAGIC     if 'java.io.FileNotFoundException' in str(e):
-- MAGIC       return False
-- MAGIC     else:
-- MAGIC       raise
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC def download_dataset(source, target):
-- MAGIC     files = dbutils.fs.ls(source)
-- MAGIC
-- MAGIC     for f in files:
-- MAGIC         source_path = f"{source}/{f.name}"
-- MAGIC         target_path = f"{target}/{f.name}"
-- MAGIC         if not path_exists(target_path):
-- MAGIC             print(f"Copying {f.name} ...")
-- MAGIC             dbutils.fs.cp(source_path, target_path, True)
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
-- MAGIC dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC def get_index(dir):
-- MAGIC     files = dbutils.fs.ls(dir)
-- MAGIC     index = 0
-- MAGIC     if files:
-- MAGIC         file = max(files).name
-- MAGIC         index = int(file.rsplit('.', maxsplit=1)[0])
-- MAGIC     return index+1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC # Structured Streaming
-- MAGIC streaming_dir = f"{dataset_bookstore}/orders-streaming"
-- MAGIC raw_dir = f"{dataset_bookstore}/orders-raw"
-- MAGIC
-- MAGIC def load_file(current_index):
-- MAGIC     latest_file = f"{str(current_index).zfill(2)}.parquet"
-- MAGIC     print(f"Loading {latest_file} file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")
-- MAGIC
-- MAGIC     
-- MAGIC def load_new_data(all=False):
-- MAGIC     index = get_index(raw_dir)
-- MAGIC     if index >= 10:
-- MAGIC         print("No more data to load\n")
-- MAGIC
-- MAGIC     elif all == True:
-- MAGIC         while index <= 10:
-- MAGIC             load_file(index)
-- MAGIC             index += 1
-- MAGIC     else:
-- MAGIC         load_file(index)
-- MAGIC         index += 1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC # DLT
-- MAGIC streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
-- MAGIC streaming_books_dir = f"{dataset_bookstore}/books-streaming"
-- MAGIC
-- MAGIC raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
-- MAGIC raw_books_dir = f"{dataset_bookstore}/books-cdc"
-- MAGIC
-- MAGIC def load_json_file(current_index):
-- MAGIC     latest_file = f"{str(current_index).zfill(2)}.json"
-- MAGIC     print(f"Loading {latest_file} orders file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
-- MAGIC     print(f"Loading {latest_file} books file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")
-- MAGIC
-- MAGIC     
-- MAGIC def load_new_json_data(all=False):
-- MAGIC     index = get_index(raw_orders_dir)
-- MAGIC     if index >= 10:
-- MAGIC         print("No more data to load\n")
-- MAGIC
-- MAGIC     elif all == True:
-- MAGIC         while index <= 10:
-- MAGIC             load_json_file(index)
-- MAGIC             index += 1
-- MAGIC     else:
-- MAGIC         load_json_file(index)
-- MAGIC         index += 1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC download_dataset(data_source_uri, dataset_bookstore)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC load_new_json_data()

-- COMMAND ----------

--%fs ls 'dbfs:/mnt/demo-datasets/bookstore/books-cdc/'

-- COMMAND ----------

--select * from json.`dbfs:/mnt/demo-datasets/bookstore/books-cdc`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze Layer

-- COMMAND ----------

create or refresh streaming live table books_bronze
comment 'the raw books data, ingested from cdc feed'
as select * from cloud_files("${datasets_path}/books-cdc",'json')


-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver Layer

-- COMMAND ----------

create or refresh streaming live table books_silver;
apply changes into live.books_silver
from stream(live.books_bronze)
keys (book_id)
apply as delete when row_status = 'DELETE'
sequence by row_time
columns * except (row_status,row_time)


-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold Layer

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE author_counts_state
comment 'number of books per author'
as select author,count(*) as books_count,current_timestamp() as updated_time
from live.books_silver
group by author
-- here author_counts_state is not streaming table,because streaming table is only valid when data are only appended. but from books_silver table table can be appended, updated or deleted

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DLT Views

-- COMMAND ----------

create live table books_sales
as select b.title,o.quantity
from (select * from live.orders_cleaned) as o
inner join live.books_silver as b
on o.customer_id = b.book_id;

-- COMMAND ----------

-- DLT views are temporary views scoped to the DLT pipeline they are a part of, so they are not persisted to the metastore.

-- COMMAND ----------

 --select * from demo_bookstore_dlt_db.books_silver

-- COMMAND ----------


