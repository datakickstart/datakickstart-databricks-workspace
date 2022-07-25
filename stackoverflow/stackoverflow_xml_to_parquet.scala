// Databricks notebook source
// import com.databricks.spark.
val file = "" // "Tags", "Posts", "Users", "Votes", "Comments", "Badges", "PostLinks"
val table = file.toLowerCase()
val df = spark.read.format("xml").option("samplingRatio", 0.001).option("rootTag", table).option("rowTag", "row").option("inferSchema", "true").load(s"dbfs:/tmp/${file}.xml")
df.cache()
df.count()

// COMMAND ----------

df.createOrReplaceTempView(s"stackoverflow_${table}")
df.show()

// COMMAND ----------

// df.write.format("parquet").mode("overwrite").save(s"dbfs:/tmp/stackoverflow/${table}")

// COMMAND ----------

if (table == "posts") {
  val df_enriched = spark.sql(
  """Select *, regexp_extract_all(_Tags, '(<[^<>]*>)',0) as TagsArray, date_format(_CreationDate, "yyyy-MM-01") as CreationMonth, date_format(_LastEditDate, "yyyy-MM-01") as LastEditMonth, date_format(_LastActivityDate, "yyyy-MM-01") as LastActivityMonth
  from stackoverflow_posts""")

  df_enriched.write.partitionBy("CreationMonth").format("parquet").mode("overwrite").save(s"dbfs:/tmp/stackoverflow/${table}")
} else {
  df.write.format("parquet").mode("overwrite").save(s"dbfs:/tmp/stackoverflow/${table}")
}

// COMMAND ----------

// MAGIC %fs ls dbfs:/tmp/stackoverflow/

// COMMAND ----------

// val df_text = spark.read.text("dbfs:/tmp/Users.xml").limit(1)
// df_text.show(truncate=false)

// COMMAND ----------

// MAGIC %fs ls 

// COMMAND ----------

// MAGIC %fs cp -r dbfs:/tmp/stackoverflow/votes dbfs:/mnt/datalake/raw/stackoverflow/votes

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/datalake/raw/stackoverflow/

// COMMAND ----------


