-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("databricks-datasets/songs")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/databricks-datasets/songs/data-001")
