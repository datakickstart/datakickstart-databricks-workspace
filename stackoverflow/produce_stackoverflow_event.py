# Databricks notebook source
from pyspark.sql.functions import col

so_posts = spark.read.parquet("dbfs:/mnt/datalake/raw/stackoverflow/posts/CreationMonth=2022-06-01")

# COMMAND ----------

so_posts = so_posts.sort(col("_CreationDate").desc())
display(so_posts)


# COMMAND ----------

bootstrapServers = dbutils.secrets.get("demo", "confluent-cloud-brokers")
confluentApiKey =  dbutils.secrets.get("demo", "confluent-cloud-user")
confluentSecret =  dbutils.secrets.get("demo", "confluent-cloud-password")
confluentTopicName = "stackoverflow_post"

options = {
    "kafka.bootstrap.servers": bootstrapServers,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret),
    "kafka.sasl.mechanism": "PLAIN",
    "topic": confluentTopicName
}

# COMMAND ----------

min_dt = None

for i in range(20):
    if min_dt:
        df = so_posts.where(col("_CreationDate")< min_dt).limit(10)
    else:
        df = so_posts.limit(10)
    min_dt = df.selectExpr("min(_CreationDate) as minCreationDate").first().minCreationDate
    df.selectExpr("cast(_Id as String) as key", "to_json(struct(*)) as value").write.format("kafka").options(**options).save()


# COMMAND ----------


