# Databricks notebook source
##Define Variables

confluentBootstrapServers = "pkc-41973.westus2.azure.confluent.cloud:9092"

confluentApiKey = dbutils.secrets.get('demo', 'confluent-cloud-user')

confluentSecret = dbutils.secrets.get('demo', 'confluent-cloud-password')

confluentTopicName = "stackoverflow_post"


##Import Library

import pyspark.sql.functions as fn
from pyspark.sql.functions import col

from pyspark.sql.types import StructType, StringType
 

##Create Spark Readstream

post_df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
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
  .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'value', 'fixedValue')
)

# query = (post_transformed_df.writeStream
#   .option("checkpointLocation", f"abfss://raw@datakickstartadls.dfs.core.windows.net/checkpoints/so_checkpoint_{version}")
#   .format("delta")
#   .outputMode("append")
#   .trigger(processingTime="5 seconds")
#   .start("abfss://raw@datakickstartadls.dfs.core.windows.net/stackoverflow_streaming")
# )

# query.processAllAvailable()
# query.stop()


# COMMAND ----------

post_schema = (
  StructType()
    .add('_Id','long')
    .add('_ParentId','long')
    .add('_PostTypeId','long')
    .add('_Score','long')
    .add('_Tags','string')
    .add('_Title','string')
    .add('_ViewCount','long')  
    .add('_LastActivityDate','timestamp')
    .add('_LastEditDate','timestamp')
    .add('_LastEditorDisplayName','string')
    .add('_LastEditorUserId','long')
    .add('_OwnerDisplayName','string')
    .add('_OwnerUserId','long')
    .add('_ParentId','long')
    .add('_PostTypeId','long')
    .add('_AcceptedAnswerId','long')
    .add('_AnswerCount','long')
    .add('_Body','string')
    .add('_ClosedDate','timestamp')
    .add('_CommentCount','long')
)

# _CommunityOwnedDate:timestamp
# _ContentLicense:string
# _CreationDate:timestamp
# _FavoriteCount:long 


df_parsed = post_transformed_df.select(
    col('value').cast('string').alias("value_str"),
    fn.from_json(col('value').cast('string'), post_schema).alias("json"))
df = df_parsed.selectExpr("json.*")
display(df)

# COMMAND ----------


