# Databricks notebook source
##Define Variables

confluentBootstrapServers = "pkc-41973.westus2.azure.confluent.cloud:9092"

confluentApiKey =  mssparkutils.credentials.getSecretWithLS('demokv', 'confluent-cloud-user')

confluentSecret = mssparkutils.credentials.getSecretWithLS('demokv', 'confluent-cloud-password')

confluentTopicName = "stackoverflow_post"


##Import Library

import pyspark.sql.functions as fn

from pyspark.sql.types import StringType
 

##Create Spark Readstream

post_df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .load()
  # .select(fn.col("value").cast(StringType()).alias("value"))
)

post_transformed_df = (post_df
.withColumn('key', fn.col("key").cast(StringType()))
  .withColumn('fixedValue', fn.expr("substring(value, 6, length(value)-5)"))
  .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
  .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'valueSchemaId','fixedValue')
)

query = (post_transformed_df.writeStream
  .option("checkpointLocation", f"abfss://raw@datakickstartadls.dfs.core.windows.net/checkpoints/so_checkpoint_{version}")
  .format("delta")
  .outputMode("append")
  .trigger(processingTime="5 seconds")
  .start("abfss://raw@datakickstartadls.dfs.core.windows.net/stackoverflow_streaming")
)

# query.processAllAvailable()
# query.stop()

