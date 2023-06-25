-- Databricks notebook source
-- MAGIC %py
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %run /Users/sandippatelaero@gmail.com/dataset_bookstore

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### from_json, schema_of_json

-- COMMAND ----------

create table customer_json_extracted
as select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;
select * from customer_json_extracted;

-- COMMAND ----------

describe extended customer_json_extracted;

-- COMMAND ----------

describe customer_json_extracted

-- COMMAND ----------

select customer_id,email,updated,profile:first_name,profile:address:country from customer_json_extracted

-- COMMAND ----------

select customer_id,profile:first_name,profile:country,profile:address:city,profile:address from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json` order by customer_id asc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Convert string into struct type

-- COMMAND ----------

select from_json(profile) as profile_struct from customer_json_extracted;

-- COMMAND ----------

select profile from customer_json_extracted limit 1;

-- COMMAND ----------

create view customer_json_extract_view
as select *,from_json(profile,schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) as profile_struct from customer_json_extracted;

select * from customer_json_extract_view;

-- COMMAND ----------

describe customer_json_extract_view

-- COMMAND ----------

select *,profile_struct.first_name,profile_struct.gender,profile_struct.last_name,profile_struct.address.* from customer_json_extract_view

-- COMMAND ----------

describe extended customer_json_extract_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### explode, collect_set

-- COMMAND ----------

create table order_parquet_extracted
as select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders/`;

select * from order_parquet_extracted;

-- COMMAND ----------

describe order_parquet_extracted;

-- COMMAND ----------

describe extended order_parquet_extracted;

-- COMMAND ----------

select order_id,customer_id,explode(books) as exploded_books,books from order_parquet_extracted;

-- COMMAND ----------

select t1.*,t1.exploded_books.* from (select order_id,customer_id,explode(books) as exploded_books,books from order_parquet_extracted) as t1 order by 1 asc

-- COMMAND ----------

-- collect list based on customer_id

select customer_id,collect_list(order_id) as order_set,collect_set(books.book_id) as book_id_set from order_parquet_extracted group by customer_id order by 1 asc;

-- COMMAND ----------

select customer_id,collect_list(order_id) as order_set,collect_set(books.book_id) as book_id_set,flatten(collect_set(books.book_id)) as flat_book_id_set,
array_distinct(flatten(collect_set(books.book_id))) as dist_flat_book_id_set
from order_parquet_extracted group by customer_id order by 1 asc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Join Operation

-- COMMAND ----------

create table ext_non_delta_book_csv
(book_id string,title string,author string,category string,price double)
using CSV
options (path='dbfs:/mnt/demo-datasets/bookstore/books-csv',header='true',delimiter=';');
describe extended ext_non_delta_book_csv;

-- COMMAND ----------

create temp view book_csv_temp_view_non_delta
(book_id string,title string,author string,category string,price double)
using csv
options (path='dbfs:/mnt/demo-datasets/bookstore/books-csv',header='true',delimiter=';');

create table book_csv_temp_view_delta as
select * from book_csv_temp_view_non_delta;

select * from book_csv_temp_view_delta;

-- COMMAND ----------

describe extended book_csv_temp_view_delta;

-- COMMAND ----------

describe extended book_csv_temp_view_non_delta;

-- COMMAND ----------

select * from book_csv_temp_view_delta

-- COMMAND ----------

select *,explode(books) as book from order_parquet_extracted

-- COMMAND ----------

select * from customer_json_extracted;

-- COMMAND ----------

-- book_csv_temp_view_delta & customer_json_extracted
create or replace view order_enriched
as select * from (select *,explode(books) as book from order_parquet_extracted) as a inner join book_csv_temp_view_delta as b on a.book.book_id = b.book_id;
select * from order_enriched;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Union

-- COMMAND ----------

create or replace temp view order_new_parquet_temp_view
as select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-new`;
select count(*) as new_cnt from order_new_parquet_temp_view;

-- COMMAND ----------

select count(*) as total_cnt from order_parquet_extracted;

-- COMMAND ----------

create table order_union
as select * from order_parquet_extracted union select * from order_new_parquet_temp_view;

-- COMMAND ----------

select count(*) as cnt from order_union;

-- COMMAND ----------

desc extended order_union;

-- COMMAND ----------

select * from order_parquet_extracted;

-- COMMAND ----------

select * from order_new_parquet_temp_view

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Intersect

-- COMMAND ----------

select * from order_union intersect select * from order_new_parquet_temp_view;

-- COMMAND ----------

select * from order_union intersect select * from order_parquet_extracted;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Minus

-- COMMAND ----------

select * from order_union minus select * from order_new_parquet_temp_view;

-- COMMAND ----------

select * from order_union minus select * from order_parquet_extracted;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pivot

-- COMMAND ----------

create or replace table order_pivot as
select * from (select customer_id,book.book_id as book_id,book.quantity as quantity from order_enriched)
pivot (sum(quantity) for book_id in ('B01','B02','B03','B04','B05','B06','B07','B08','B09','B10','B11','B12'));
select * from order_pivot;

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe schema default;

-- COMMAND ----------

describe catalog spark_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Advanced Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter

-- COMMAND ----------

select * from order_parquet_extracted;

-- COMMAND ----------

select *,filter(books,i -> i.quantity >= 2 ) as more_than1_quantity from order_parquet_extracted;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Size

-- COMMAND ----------

-- in above filter we are getting empty list wherever condition is failed, so we can filter that kind of list using size
select t1.* from (select order_id,books,filter(books,i -> i.quantity>=2) as multiple_quantity from order_parquet_extracted) as t1 where size(t1.multiple_quantity) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transform

-- COMMAND ----------

select order_id,books,transform(books,i -> cast(i.subtotal*2.56 as float)) as discount_subtotal from order_parquet_extracted;

-- COMMAND ----------

select order_id,books,discount_subtotal,sum(new_col) as sum_discount from
(select m1.*,explode(m1.discount_subtotal) as new_col from (select order_id,books,transform(books,i -> cast(i.subtotal*2.56 as float)) as discount_subtotal from order_parquet_extracted) as m1) as m2
group by order_id,books,discount_subtotal

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UDF

-- COMMAND ----------

create or replace function get_url(email string)
returns string

return concat("https://www.",split(email,"@")[1])

-- COMMAND ----------

select email,get_url(email) as cleaned_email from customer_json_extracted

-- COMMAND ----------

describe function get_url


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Note that user defined functions are permanent objects that are persisted to the database, so you can use them between different Spark sessions and notebooks.

-- COMMAND ----------

describe function extended get_url;
-- type : SCALAR

-- COMMAND ----------

create or replace function site_type(email string)
returns string
return case 
when email like "%.com" then 'Commercial Business'
when email like "%.org" then 'Non-Profit Organization'
when email like "%.edu" then 'Educational Institute'
else concat("unknown extension for domain:",split(email,"@")[1]) end;

-- COMMAND ----------

select email,site_type(email) as domain_category from customer_json_extracted;

-- COMMAND ----------

create or replace function higher_quantity_orders (quantity_val bigint)
returns table(order_id STRING,customer_id string,quantity bigint,books ARRAY<STRUCT<book_id: STRING, quantity: INT, subtotal: BIGINT>>)
return select order_id,customer_id,quantity,books FROM order_parquet_extracted where quantity >= higher_quantity_orders.quantity_val;

-- COMMAND ----------

select * from higher_quantity_orders(4)

-- COMMAND ----------

describe function higher_quantity_orders;
-- type : TABLE

-- COMMAND ----------

describe function extended higher_quantity_orders;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.udf.register("get_higher_quantity_ordersurl",higher_quantity_orders)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC for i in spark.catalog.listFunctions():
-- MAGIC     if(i.name in ['get_url','higher_quantity_orders']):
-- MAGIC         print(i,"\n")

-- COMMAND ----------




