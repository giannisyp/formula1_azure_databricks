-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT * FROM drivers;

-- COMMAND ----------

SELECT *, concat(driver_ref," ", name ) AS new_driver_ref FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] AS forname ,SPLIT(name, ' ')[1] as surname FROM  drivers;

-- COMMAND ----------

SELECT *, date_format(dob, "yyyy") FROM drivers;

-- COMMAND ----------

select count(*) from drivers;

-- COMMAND ----------

SELECT MAX(dob) from drivers;

-- COMMAND ----------

select * from drivers
where dob = "2000-05-11";

-- COMMAND ----------

select count(*) from drivers
where nationality = "Greek";

-- COMMAND ----------

select nationality,count(*) from drivers
group by nationality
order by 2 desc;

-- COMMAND ----------

select nationality,count(*) from drivers
group by nationality
having count(*) > 100
order by 2 desc;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

USE f1_presentation;

-- COMMAND ----------

show tables;

-- COMMAND ----------

DESCRIBE driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS 
SELECT race_year, driver_name, team, total_points,wins , rank
  FROM driver_standings
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS 
SELECT race_year, driver_name, team, total_points,wins , rank
  FROM driver_standings
  WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018
INNER JOIN v_driver_standings_2020 ON v_driver_standings_2018.driver_name = v_driver_standings_2020.driver_name;


-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018
LEFT JOIN v_driver_standings_2020 
ON v_driver_standings_2018.driver_name = v_driver_standings_2020.driver_name;

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018
RIGHT JOIN v_driver_standings_2020 
ON v_driver_standings_2018.driver_name = v_driver_standings_2020.driver_name;

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018
FULL OUTER JOIN v_driver_standings_2020 
ON v_driver_standings_2018.driver_name = v_driver_standings_2020.driver_name;

-- COMMAND ----------

