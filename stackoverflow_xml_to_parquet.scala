// Databricks notebook source
// import com.databricks.spark.
val file = "Posts" // "users"
val table = file.toLowerCase()
val df = spark.read.format("xml").option("samplingRatio", 0.01).option("rootTag", table).option("rowTag", "row").option("inferSchema", "true").load(s"dbfs:/tmp/${file}.xml")
df.cache()
df.count()

// COMMAND ----------

df.createOrReplaceTempView(s"stackoverflow_${table}")
df.show()

// COMMAND ----------

df.write.format("parquet").mode("overwrite").save(s"dbfs:/tmp/stackoverflow/${table}")

// COMMAND ----------

// import org.apache.spark.sql.functions._
// val df2 = df.withColumn("tags_split", regexp_extract_all("_DisplayName", "<[A-za-z]*>"))

// COMMAND ----------

// MAGIC %sql
// MAGIC Select _DisplayName, regexp_extract_all(_DisplayName, '(.i)',1)
// MAGIC from stackoverflow_users
// MAGIC where _DisplayName = 'Twilio'

// COMMAND ----------

// MAGIC %sql
// MAGIC Select tags, regexp_extract_all(tags, '(<[A-za-z]>)',0)
// MAGIC from stackoverflow_posts
// MAGIC limit 100

// COMMAND ----------

// val df_text = spark.read.text("dbfs:/tmp/Users.xml").limit(1)
// df_text.show(truncate=false)
