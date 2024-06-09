# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS mycatalog
# MAGIC   MANAGED LOCATION 'abfss://container1@sa12299may.dfs.core.windows.net/depts/finance';
# MAGIC
# MAGIC GRANT SELECT ON mycatalog TO `finance-team`;

# COMMAND ----------


