-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets/travel_recommendations_realtime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets/travel_recommendations_realtime/raw_travel_data")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jsonDF = spark.read.json("dbfs:/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json/*.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jsonDF.show()

-- COMMAND ----------

select *
from json.`dbfs:/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json/*.json`
limit 20

-- COMMAND ----------

use catalog weissler;

-- COMMAND ----------

create schema if not exists travel_bronze;

-- COMMAND ----------

-- drop table travel_bronze.availability_logs;

-- COMMAND ----------

create table if not exists travel_bronze.availability_logs using delta;
describe extended travel_bronze.availability_logs;

-- COMMAND ----------

copy into travel_bronze.availability_logs
from 'dbfs:/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json/*.json'
fileformat = JSON
format_options ('inferSchema' = 'true')
copy_options ('mergeSchema' = 'true');

-- COMMAND ----------

select *
from travel_bronze.availability_logs
limit 20;
