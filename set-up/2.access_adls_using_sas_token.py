# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------



# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

sas_token = dbutils.secrets.get("formula1-scope" ,"formula1-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1giannisdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1giannisdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1giannisdl.dfs.core.windows.net", sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1giannisdl.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1giannisdl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1giannisdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

