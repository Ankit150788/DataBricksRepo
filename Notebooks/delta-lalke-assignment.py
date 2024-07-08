# Databricks notebook source
# MAGIC %md
# MAGIC Assignment: Using the Titanic dataset from Kaggle:
# MAGIC 	
# MAGIC   1.	Create a Delta table in Databricks from the CSV file.
# MAGIC 	
# MAGIC   2.	Perform an upsert (merge) operation to update existing passenger records and insert new records based on a new dataset of passengers.
# MAGIC 	
# MAGIC   3.	Query the Delta table to display the updated passenger data after the merge operation.
# MAGIC 	
# MAGIC   4.	Explain how Delta Lake handles schema evolution and provide an example scenario using the Titanic dataset.
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark =  SparkSession.builder.appName("titanicData").getOrCreate()

filepath = "/Workspace/first-folder/Titanic-Dataset.csv"

df = pd.read_csv(filepath)

spark_df = spark.createDataFrame(df)

spark_df.show()

spark_df.createOrReplaceTempView("titanic_data")

spark_df.write.format("delta").mode("overwrite").save("/tmp/delta/titanic")

delta_df = spark.read.format("delta").load("/tmp/delta/titanic")

delta_df.show()
print(delta_df.count())

new_data = [
(1, 0, 3,"Braund, Mr. Owen Harris","male",25.0,1,0,"A/5 21171",7.25,0,"S"),
(893, 0, 3,"Alice","female",30.0,1,0,"A/5 21171",7.25,0,"S"),
(894, 0, 3,"Bob","male",35.0,1,0,"A/5 21171",7.25,0,"S"),
]

columns = ["PassengerId","Survived","Pclass","Name","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked"]
new_df = spark.createDataFrame(new_data,columns)
new_df.show()

delta_table = DeltaTable.forPath(spark, "/tmp/delta/titanic")

delta_table.alias("old_data").merge(
new_df.alias("new_data"),
"old_data.PassengerId = new_data.PassengerId"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

updated_df = spark.read.format("delta").load("/tmp/delta/titanic")
print(updated_df.count())
updated_df.show()
updated_df.createOrReplaceTempView("new_titanic_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from new_titanic_data WHERE PassengerId = 1