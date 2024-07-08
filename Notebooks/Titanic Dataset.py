# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment: 
# MAGIC
# MAGIC Objective: Use Spark SQL to analyze the Titanic dataset. Perform data exploration, aggregation, and derive insights from the dataset.
# MAGIC
# MAGIC Tasks:
# MAGIC
# MAGIC Load the Titanic dataset into Spark SQL.
# MAGIC
# MAGIC Write SQL queries to:
# MAGIC
# MAGIC Calculate the average age of passengers.
# MAGIC
# MAGIC Count the number of survivors and non-survivors.
# MAGIC
# MAGIC Find the percentage of passengers who survived.
# MAGIC
# MAGIC Identify the passenger class with the highest survival rate.
# MAGIC
# MAGIC Explore relationships between variables (e.g., survival rate by gender or age group).
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TitanicDataSetAnalysis").getOrCreate()

file_path = "/Workspace/first-folder/Titanic-Dataset.csv"

df = pd.read_csv(file_path)

spark_df = spark.createDataFrame(df)

spark_df.createOrReplaceTempView("titanic_table")

spark_df.describe().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(Age) as Avd_Age FROM titanic_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (COUNT(CASE WHEN Survived = 1 THEN 1 END) / COUNT(*)) * 100 AS survival_percentage
# MAGIC FROM titanic_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select ((select Count(Survived) FROM titanic_table Where Survived = 1) / (select Count(*) FROM titanic_table))* 100 as Survival_Rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Pclass, AVG(Survived) AS survival_rate
# MAGIC FROM titanic_table
# MAGIC GROUP BY Pclass
# MAGIC ORDER BY survival_rate DESC
# MAGIC LIMIT 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Sex, AVG(Survived) AS survival_rate
# MAGIC FROM titanic_table
# MAGIC GROUP BY Sex;
# MAGIC