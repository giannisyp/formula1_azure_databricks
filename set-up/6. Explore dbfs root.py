# Databricks notebook source
# MAGIC %md
# MAGIC # Explore DBFS root 
# MAGIC 1. List all the folders in dbfs root 
# MAGIC 2. Interact with dbfs file browswer
# MAGIC 3. Upload file to dbfs root 

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))

# COMMAND ----------

