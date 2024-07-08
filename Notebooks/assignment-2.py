# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment: 
# MAGIC
# MAGIC Create another DataFrame witrh 3 columns "species": ["Iris-setosa", "Iris-versicolor", "Iris-virginica"], "scientific_name": ["Setosa", "Versicolor", "Virginica"], "common_name": ["Setosa Iris", "Versicolor Iris", "Virginica Iris"]
# MAGIC
# MAGIC Table name: species_details_table
# MAGIC
# MAGIC Group by species and calculate average sepal_length
# MAGIC
# MAGIC Sort data by sepal_length in descending order
# MAGIC
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IrisDataAnalysis").getOrCreate()

file_path = "/Workspace/first-folder/iris.data"
columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]

df = pd.read_csv(file_path, names=columns)

map = {"species": ["Iris-setosa", "Iris-versicolor", "Iris-virginica"], "scientific_name": ["Setosa", "Versicolor", "Virginica"], "common_name": ["Setosa Iris", "Versicolor Iris", "Virginica Iris"]}

new_df = pd.DataFrame(map)
newSpark_df = spark.createDataFrame(new_df)

spark_df = spark.createDataFrame(df)

spark_df.show()

spark_df.printSchema()

spark_df.createOrReplaceTempView("iris_table")

newSpark_df.createOrReplaceTempView("species_details_table")

spark_df.describe().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iris_table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iris_table where sepal_length > 5.0
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select species, avg(sepal_length) from iris_table group by species 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * FROM species_details_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iris_table i inner join species_details_table sd on i.species = sd.species LIMIT 10

# COMMAND ----------

spark_df.join(newSpark_df, "species").limit(10).show()