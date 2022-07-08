// Databricks notebook source
// import com.databricks.spark.

val df = spark.read.format("xml").option("samplingRatio", 0.01).option("rootTag", "users").option("rowTag", "row").option("inferSchema", "true").load("dbfs:/tmp/Users.xml")
df.cache()
df.count()

// COMMAND ----------

df.show()

// COMMAND ----------

val df_text = spark.read.text("dbfs:/tmp/Users.xml").limit(1)
df_text.show(truncate=false)
