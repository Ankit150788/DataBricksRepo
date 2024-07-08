# Databricks notebook source
# MAGIC %md
# MAGIC # DELTA LAKE INFORMATIOM
# MAGIC
# MAGIC Storing Data in delta lake
# MAGIC
# MAGIC Fetching the data from the deltag lake
# MAGIC
# MAGIC Update the data in a delta lake
# MAGIC
# MAGIC Getting the old version data in delta lake

# COMMAND ----------

from delta.tables import DeltaTable

data = [
    (1, "Alice", 28),
    (2, "Bob", 32),
    (3, "Catherine", 25)
]


columns = ["id", "name", "age"]


df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").save("/tmp/delta/people")

delta_df = spark.read.format("delta").load("/tmp/delta/people")
delta_df.show()

new_data = [
    (1, "Alice", 29),  # Update Alice's age
    (4, "David", 35)   # Insert new record for David
]


new_df = spark.createDataFrame(new_data, columns)


delta_table = DeltaTable.forPath(spark, "/tmp/delta/people")


delta_table.alias("old_data").merge(
    new_df.alias("new_data"),
    "old_data.id = new_data.id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()


updated_df = spark.read.format("delta").load("/tmp/delta/people")
updated_df.show()

previous_version_df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/people")


previous_version_df.show()

# Creating new dataset

new_data_with_new_column = [
    (1, "Alice", 30, "F"), 
    (4, "David", 36, "M")
]

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

columns_with_new_column = ["id", "name", "age", "gender"]
new_df_with_new_column = spark.createDataFrame(new_data_with_new_column, columns_with_new_column)


delta_table.alias("old_data").merge(
    new_df_with_new_column.alias("new_data"),
    "old_data.id = new_data.id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()


# Read data from the Delta table after schema evolution
updated_df_with_new_column = spark.read.format("delta").load("/tmp/delta/people")


# Show the updated data
updated_df_with_new_column.show()



