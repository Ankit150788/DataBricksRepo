# Databricks notebook source
# MAGIC %md
# MAGIC # Question
# MAGIC
# MAGIC Use PySpark to analyze the Superstore Sales dataset. Perform basic data manipulation operations to gain insights into sales trends and customer behavior.
# MAGIC
# MAGIC Tasks:
# MAGIC
# MAGIC Load the Superstore Sales dataset into a PySpark DataFrame.
# MAGIC
# MAGIC Calculate total sales per category.
# MAGIC
# MAGIC Identify the top-selling products.
# MAGIC
# MAGIC Analyze sales trends over time.
# MAGIC
# MAGIC Segment customers based on purchasing behavior (optional).
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()
file_path = "/Workspace/first-folder/train.csv"

df = pd.read_csv(file_path)

spark_df = spark.createDataFrame(df)

spark_df.show()


group_sales_by_category = spark_df.groupBy("Category").sum("Sales")
group_sales_by_category.show()



# COMMAND ----------

top_selling_products = spark_df.groupBy("Product ID").sum("Sales").alias("TotalSales")
top_selling_products.show()
top_selling_products.orderBy(top_selling_products.columns[1], ascending=False).show()


# COMMAND ----------

from pyspark.sql.functions import month, year

sales_df = spark_df.withColumn("OrderMonth", month("Order Date")).withColumn("OrderYear", year("Order Date"))
sales_df.show()
monthly_sales = sales_df.groupBy("OrderYear", "OrderMonth").agg(sum("Sales").alias("MonthlySales")).orderBy("OrderYear", "OrderMonth")

monthly_sales.show()