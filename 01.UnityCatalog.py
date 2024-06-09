# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT CURRENT_METASTORE();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.department
# MAGIC (
# MAGIC    deptcode   INT,
# MAGIC    deptname  STRING,
# MAGIC    location  STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.department VALUES
# MAGIC    (10, 'FINANCE', 'EDINBURGH'),
# MAGIC    (20, 'SOFTWARE', 'PADDINGTON');

# COMMAND ----------


