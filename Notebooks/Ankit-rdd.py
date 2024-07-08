# Databricks notebook source
from pyspark import SparkConf, SparkContext


data = [1, 2, 3, 4, 5]


rdd = sc.parallelize(data)


sum_of_squares = rdd.map(lambda x: x**2).reduce(lambda x, y: x + y)
print("Sum of squares:", sum_of_squares)


filtered_rdd = rdd.filter(lambda x: x > 3)
filtered_elements = filtered_rdd.collect()
print("Filtered elements greater than 3:", filtered_elements)


# Question: Calculate average

avg = rdd.reduce(lambda x, y: x + y) / rdd.count()
print("Average:", avg)

# Question: write a code to demonstrate dataset

