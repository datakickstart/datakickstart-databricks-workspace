# Databricks notebook source
df = spark.read.format("delta").load("dbfs:/mnt/datalake/raw/stack_overflow_streaming/posts_so_v1")
display(df)

# COMMAND ----------


