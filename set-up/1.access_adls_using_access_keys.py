# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get("formula1-scope","formula1dl-account-key")

# COMMAND ----------



# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1giannisdl.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1giannisdl.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1giannisdl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1giannisdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

