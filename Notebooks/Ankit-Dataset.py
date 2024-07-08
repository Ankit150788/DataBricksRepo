# Databricks notebook source
# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Initialize SparkSession
spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()


# Sample data
data = [("John", 25), ("Alice", 30), ("Bob", 35)]


# Define schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])


# Create DataFrame
df = spark.createDataFrame(data, schema)


# Show the content of the DataFrame
df.show()


# Perform operations on DataFrame
# Example: Filter data where age is greater than 30
filtered_df = df.filter(df["age"] > 30)
filtered_df.show()
