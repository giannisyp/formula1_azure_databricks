# Databricks notebook source
v_result = dbutils.notebook.run("1._ingestion_circuits_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2._ingest_races_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3._ingest_constructors_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4._ingest_drivers_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5._ingest_results_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6._ingest_pit_stops_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6._ingest_pit_stops_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7._ingest_lap_time_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8._ingest_qualifying_file",0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result