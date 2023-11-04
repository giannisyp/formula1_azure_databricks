-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
LIMIT 10;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
WHERE nationality = "British"
AND dob >=  "1990-01-01";

-- COMMAND ----------

SELECT name,dob FROM f1_processed.drivers
WHERE nationality = "British"
AND dob >=  "1990-01-01";

-- COMMAND ----------

SELECT name,dob FROM f1_processed.drivers
WHERE nationality = "British"
AND dob >=  "1990-01-01"
ORDER BY dob;

-- COMMAND ----------

