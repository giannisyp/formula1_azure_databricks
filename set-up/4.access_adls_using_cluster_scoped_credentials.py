# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster 
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1giannisdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1giannisdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1giannisdl.dfs.core.windows.net", "sp=rl&st=2023-10-26T17:01:48Z&se=2023-10-27T01:01:48Z&spr=https&sv=2022-11-02&sr=c&sig=Umzo8PGfn7OR47Z%2B0yDtouPJME5gb%2FxnlHQ6FoIMwZk%3D")


# COMMAND ----------

client_id = "2fc9eb1f-79b6-420b-8ac3-21e98838d6cd"
tenant_id = "8d34f8a5-7afc-42bf-bdfd-d5b734a0aceb"
client_secret = "Krd8Q~E7cV.ZfC_LZcT65qp7Xu17p3zMmKgrAcR~"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1giannisdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1giannisdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1giannisdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1giannisdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1giannisdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1giannisdl.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1giannisdl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1giannisdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

