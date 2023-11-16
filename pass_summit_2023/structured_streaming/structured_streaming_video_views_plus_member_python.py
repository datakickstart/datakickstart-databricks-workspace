# Databricks notebook source
# MAGIC %md
# MAGIC ## Stream Processing To Delta Lake
# MAGIC Must have data streaming to the topic usage".
# MAGIC
# MAGIC *Note: If not working, try changing the GROUP_ID and Consumer Group values to reset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Load of incoming data - Video Views
# MAGIC Read streaming data from Confluent Cloud or Event Hubs (using Apache Kafka API) and save in the same delta location within Azure Data Lake Storage (ADLS).

# COMMAND ----------

GROUP_ID = "video_plus_member_python_v1"

def get_confluent_config(topic):
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
        'kafka.group.id': GROUP_ID,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "subscribe": confluentTopicName
    }
    return options

def get_event_hub_config(topic):
    # Password is really a Event Hub connection string, for example ->
    # Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=ReadWriteTmp;SharedAccessKey=vhNXxXXXXXxxxXXXXXXXxx=;EntityPath=demo-message-1
    password = dbutils.secrets.get(scope = "demo", key = "eh-sasl-{0}".format(topic))

    EH_SASL = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{0}";'.format(password)

    config = {
      'kafka.bootstrap.servers': 'dustin-demo-eh.servicebus.windows.net:9093',
      'kafka.security.protocol': 'SASL_SSL',
      'kafka.sasl.mechanism': 'PLAIN',
      'kafka.group.id': GROUP_ID,
      'kafka.request.timeout.ms': "60000",
      'kafka.session.timeout.ms': "20000",
      'kafka.heartbeat.interval.ms': "10000",
      'kafka.sasl.jaas.config': EH_SASL,
      'subscribe': topic
    }
    return config

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType

mode = "confluent"
write_mode = "append"
checkpoint_path1 = "dbfs:/tmp/checkpoints/ss_python_member_v1"
checkpoint_path2 = "dbfs:/tmp/checkpoints/ss_python_video_view_v1"
checkpoint_path3 = "dbfs:/tmp/checkpoints/ss_python_video_view_full_v1"
refresh_all = False

if refresh_all:
    dbutils.fs.rm(checkpoint_path1, recurse=True)
    dbutils.fs.rm(checkpoint_path2, recurse=True)
    dbutils.fs.rm(checkpoint_path3, recurse=True)
    spark.sql(f"DROP TABLE IF EXISTS main.default.ss_member_bronze;")
    spark.sql(f"DROP TABLE IF EXISTS main.default.ss_video_view_bronze;")
    spark.sql(f"DROP TABLE IF EXISTS main.default.ss_member_video_views_bronze;")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
from pyspark.sql.functions import col, from_json, expr

usage_topic = "usage"
membership_topic = "membership"

if mode == "confluent":
    usage_kafka_options = get_confluent_config(usage_topic)
    member_kafka_options = get_confluent_config(membership_topic)
else:
    usage_kafka_options = get_event_hub_config(usage_topic)
    member_kafka_options = get_event_hub_config(membership_topic)

usage_schema = StructType([
    StructField("usageId", IntegerType()),
    StructField("user", StringType()),
    StructField("completed", BooleanType()),
    StructField("durationSeconds", IntegerType()),
    StructField("eventTimestamp", TimestampType())
])

membership_schema = StructType([
    StructField("membershipId", StringType()),
    StructField("user", StringType()),
    StructField("planId", StringType()),
    StructField("startDate", StringType()),
    StructField("endDate", StringType()),
    StructField("updatedAt", StringType()),
    StructField("eventTimestamp", StringType())
])

usage_stream_raw = spark.readStream \
    .format("kafka") \
    .options(**usage_kafka_options) \
    .option("subscribe", usage_topic) \
    .option("startingOffsets", "earliest") \
    .load()

member_stream_raw = spark.readStream \
    .format("kafka") \
    .options(**member_kafka_options) \
    .option("subscribe", membership_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# COMMAND ----------

# DBTITLE 1,Start member_stream
member_stream = member_stream_raw \
    .select(from_json(col("value").cast(StringType()), membership_schema).alias("value_json")) \
    .selectExpr("value_json.*") \
    .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").alias("eventTimestamp")) \
    .withWatermark("eventTimestamp", "3 minutes")

q1 = member_stream.writeStream \
    .outputMode(write_mode) \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path1) \
    .trigger(processingTime='1 minute') \
    .toTable("main.default.ss_member_bronze")

q1.processAllAvailable()

# COMMAND ----------

# DBTITLE 1,Start usage_stream
usage_stream = usage_stream_raw.select(from_json(col("value").cast(StringType()), usage_schema).alias("value_json")) \
    .selectExpr("value_json.*") \
    .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").alias("eventTimestamp")) \
    .withWatermark("eventTimestamp", "3 minutes")
    
q2 = usage_stream.writeStream \
    .outputMode(write_mode) \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path2) \
    .trigger(processingTime='1 minute') \
    .toTable("main.default.ss_video_view_bronze")

q2.processAllAvailable()

# COMMAND ----------

# DBTITLE 1,Example of join
# # TODO: Get only latest member entry before join or modify data generator to avoid cartesian product.
# result_df = (usage_stream.alias("u")
#     .join(member_stream.alias("m"),
#           expr("m.user ==u.user and m.eventTimestamp between u.eventTimestamp and u.eventTimestamp + interval 5 minutes"),
#           how="left")
#     .select("u.*", "m.membershipId", "m.startDate", "m.endDate")
# )

# write_mode = "append"

# q3 = result_df \
#     .writeStream \
#     .outputMode(write_mode) \
#     .format("delta") \
#     .option("checkpointLocation", checkpoint_path3) \
#     .trigger(processingTime='1 minute') \
#     .toTable("main.default.ss_member_video_views_bronze")

# q3.processAllAvailable()

# COMMAND ----------

# # Read data out of delta table
# delta_stream_df = spark.readStream.table("main.default.ss_member_video_views_bronze")
# display(delta_stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Storage as a destination
# MAGIC * One option for streaming output is to write directly to you data lake storage (Azure Data Lake Storage Gen 2 or standard Azure Blob Storage).
# MAGIC * Databricks Delta / Delta Lake file format makes this more efficient, but could do with Parquet, Avro or other formats.

# COMMAND ----------

# video_views_delta_path_2 = video_views_delta_path + "_" + run_version

# q2 = (
# transformed_df.writeStream
#   .queryName("StreamingVideoViewsDelta")
#   .format("delta")
#   .outputMode("append")
#   .trigger(processingTime="5 seconds")
#   .option("checkpointLocation", f"/delta/events/_checkpoints/streaming_video_views_{run_version}")
#   .start(video_views_delta_path_2)
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternatively: Send transformed data to Event Hubs for next steps in pipeline

# COMMAND ----------

# topic2 = "member_video_views"
# producer_config = member_kafka_options
# producer_config.pop('subscribe')
# producer_config['topic'] = topic2

# q3 = result_df \
#     .selectExpr("concat(cast(usageId as string),'-',membershipId) as key",
#                 "to_json(struct(*)) as value") \
#     .writeStream \
#     .outputMode(write_mode) \
#     .format("kafka") \
#     .options(**producer_config) \
#     .option("topic", topic2) \
#     .option("checkpointLocation", checkpoint_path) \
#     .trigger(processingTime='20 seconds') \
#     .start()
