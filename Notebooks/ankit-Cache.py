# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder.appName("IrisDataAnalysis").getOrCreate()


# File path for the Iris dataset
file_path = "/Workspace/first-folder/iris.data"


# Define columns for the DataFrame
columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]


# Read the CSV into a Pandas DataFrame
df = pd.read_csv(file_path, names=columns)


# Create a Spark DataFrame from the Pandas DataFrame
spark_df = spark.createDataFrame(df)


spark_df.cache()


# Print the schema of the Spark DataFrame
spark_df.printSchema()


# Create a temporary view for SQL queries
spark_df.createOrReplaceTempView("iris_table")


spark_df.unpersist()


spark.stop()

