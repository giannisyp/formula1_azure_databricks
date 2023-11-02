# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS Token 
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set spark Config with app/ client id , directory/ tenand id & Secret
# MAGIC 4. Assign Role 'Storage Blob data contributor' to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get("formula1-scope", "formula1-app-client-id" )
tenant_id = dbutils.secrets.get("formula1-scope", "formula1-tenant-id" )
client_secret = dbutils.secrets.get("formula1-scope", "formula1-client-secret" )

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

