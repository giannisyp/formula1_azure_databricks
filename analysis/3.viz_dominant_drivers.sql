-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers 
AS
SELECT driver_name, count(1) AS total_races,
SUM(calculated_points) AS total_points,
avg(calculated_points) as avg_points,
RANK() OVER(ORDER BY avg(calculated_points) DESC) driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT race_year,
driver_name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
GROUP BY race_year, driver_name
ORDER BY race_year,avg_points DESC;

-- COMMAND ----------

SELECT race_year,
driver_name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
GROUP BY race_year, driver_name
ORDER BY race_year,avg_points DESC;

-- COMMAND ----------

SELECT race_year,
driver_name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
GROUP BY race_year, driver_name
ORDER BY race_year,avg_points DESC;

-- COMMAND ----------

