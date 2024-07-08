# Databricks notebook source
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

row_count = spark_df.count()
print(f"Row count: {row_count}")

spark_df.select("species").distinct().show()


# COMMAND ----------

filtered_df = spark_df.filter(spark_df.sepal_length > 5.0)
filtered_df.show()

row_count_fil = filtered_df.count()
print(f"{row_count_fil}")

groupby_species = spark_df.groupBy(spark_df.species)
groupby_species.avg("sepal_length").show()
groupby_species.count().show()

# COMMAND ----------

df_with_ratio = spark_df.withColumn("sepal_ratio", spark_df.sepal_length / spark_df.sepal_width)
df_with_ratio.show()

# COMMAND ----------

renamed_df = spark_df.withColumnRenamed("species", "flower_species") 
renamed_df.show()

df_dropped = spark_df.drop("petal_width") 
df_dropped.show()

sorted_df = spark_df.orderBy(spark_df.sepal_length.desc())
sorted_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iris_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iris_table where sepal_length > 5.0

# COMMAND ----------

# MAGIC %sql
# MAGIC select species, avg(sepal_length) from iris_table group by species 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iris_table order by sepal_length desc