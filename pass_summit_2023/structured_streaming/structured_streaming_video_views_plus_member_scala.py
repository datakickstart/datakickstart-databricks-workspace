# Databricks notebook source
# MAGIC %md
# MAGIC ## Stream Processing To Delta Lake
# MAGIC Must have data streaming to the topic "video_usage".
# MAGIC
# MAGIC *Note: If not working, try changing the GROUP_ID and Consumer Group values to reset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared imports and variables
# MAGIC Run this first since most cells below need at least one of these imports or variables

# COMMAND ----------

# from pyspark.sql.functions import col, desc, regexp_replace, substring, to_date, from_json, explode, expr
# from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType, BooleanType, TimestampType

# date_format = "yyyy-MM-dd HH:mm:ss"

# video_views_delta_path = "dbfs:/usage/video"

# # Define a schema that Spark understands. This is one of several ways to do it.
# usage_schema = StructType([
#     StructField("usageId", IntegerType()),
#     StructField("user", StringType()),
#     StructField("completed", BooleanType()),
#     StructField("durationSeconds", IntegerType()),
#     StructField("eventTimestamp", TimestampType()),
#     StructField("t", StringType(), False)
# ])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Load of incoming data - Video Views
# MAGIC Read streaming data from Confluent Cloud or Event Hubs (using Apache Kafka API) and save in the same delta location within Azure Data Lake Storage (ADLS).

# COMMAND ----------

# MAGIC %scala
# MAGIC // def get_confluent_cloud_config():
# MAGIC //   val bootstrap_servers = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-brokers")
# MAGIC //   val username = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-user")
# MAGIC //   val password = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-password")
# MAGIC //   val SASL = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{0}" password="{1}";'.format(username, password)
# MAGIC
# MAGIC //   val config = {
# MAGIC //       'kafka.bootstrap.servers': bootstrap_servers,
# MAGIC //       'kafka.security.protocol': 'SASL_SSL',
# MAGIC //       'kafka.sasl.mechanism': 'PLAIN',
# MAGIC //       'kafka.group.id': GROUP_ID,
# MAGIC //       'kafka.request.timeout.ms': "60000",
# MAGIC //       'kafka.session.timeout.ms': "20000",
# MAGIC //       'kafka.heartbeat.interval.ms': "10000",
# MAGIC //       'kafka.sasl.jaas.config': SASL,
# MAGIC //       'subscribe': topic
# MAGIC //   }
# MAGIC //   return config
# MAGIC
# MAGIC
# MAGIC // consumer_config = get_confluent_cloud_config()

# COMMAND ----------

# MAGIC %python
# MAGIC logger = spark._jvm.org.apache.log4j
# MAGIC log = logger.LogManager.getLogger("myLogger")
# MAGIC
# MAGIC log.info("Starting streaming job")

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC import org.apache.spark.SparkConf
# MAGIC import org.apache.spark.sql.functions.{col, expr, from_json}
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.{DataFrame, SparkSession}
# MAGIC
# MAGIC   val mode = "confluent" //sys.env("KAFKA_PRODUCER_MODE")
# MAGIC   val checkpointPath = "dbfs:/tmp/checkpoints/ss_video_view_v1"
# MAGIC   val refreshAll = false
# MAGIC
# MAGIC   if (refreshAll) {dbutils.fs.rm(checkpointPath, recurse=true)}
# MAGIC
# MAGIC
# MAGIC   val bootstrapServers = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-brokers") // sys.env("KAFKA_BROKERS")
# MAGIC   val kafkaAPIKey = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-user") //sys.env("KAFKA_API_KEY")
# MAGIC   val kafkaAPISecret = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-password") //sys.env("KAFKA_API_SECRET")
# MAGIC
# MAGIC   val kafkaOptions = Map(
# MAGIC     "kafka.bootstrap.servers"-> bootstrapServers,
# MAGIC     "kafka.security.protocol"-> "SASL_SSL",
# MAGIC     "kafka.sasl.jaas.config"-> s"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule  required username='$kafkaAPIKey'   password='$kafkaAPISecret';",
# MAGIC     "kafka.sasl.mechanism"-> "PLAIN",
# MAGIC     "kafka.client.dns.lookup"-> "use_all_dns_ips",
# MAGIC     "kafka.acks"-> "all"
# MAGIC   )
# MAGIC
# MAGIC   val sparkConf = new SparkConf()
# MAGIC     .set("spark.sql.streaming.metricsEnabled", "true")
# MAGIC     .set("spark.sql.shuffle.partitions", "16")
# MAGIC     .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# MAGIC .set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
# MAGIC // .set("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true" )
# MAGIC // .set("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "2000")
# MAGIC // .set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "true" )
# MAGIC // //.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true" )
# MAGIC // // .set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
# MAGIC
# MAGIC   val sparkBase = SparkSession.builder
# MAGIC     .appName("Structured Streaming Video Views")
# MAGIC     .config(sparkConf)
# MAGIC
# MAGIC   val spark =  if (mode == "local") sparkBase.master("local").getOrCreate() else sparkBase.getOrCreate()
# MAGIC
# MAGIC   import spark.implicits._
# MAGIC
# MAGIC   val usageTopic = "usage"
# MAGIC   val membershipTopic = "membership"
# MAGIC
# MAGIC   val usageSchema = StructType(Seq(
# MAGIC     StructField("usageId", IntegerType),
# MAGIC     StructField("user", StringType),
# MAGIC     StructField("completed", BooleanType),
# MAGIC     StructField("durationSeconds", IntegerType),
# MAGIC     StructField("eventTimestamp", TimestampType)
# MAGIC   ))
# MAGIC
# MAGIC  val membershipSchema = StructType(Seq(
# MAGIC    StructField("membershipId", StringType),
# MAGIC    StructField("user", StringType),
# MAGIC    StructField("planId", StringType),
# MAGIC    StructField("startDate", StringType),
# MAGIC    StructField("endDate", StringType),
# MAGIC    StructField("updatedAt", StringType),
# MAGIC    StructField("eventTimestamp", StringType)
# MAGIC  ))
# MAGIC
# MAGIC   val usageStreamRaw = spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .options(kafkaOptions)
# MAGIC     .option("subscribe", usageTopic)
# MAGIC     .option("startingOffsets", "earliest")
# MAGIC     .load()
# MAGIC
# MAGIC   // val membershipLookup = spark.read
# MAGIC   //   .format("delta")
# MAGIC   //   .load(memberPath)
# MAGIC
# MAGIC   val memberStreamRaw = spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .options(kafkaOptions)
# MAGIC     .option("subscribe", membershipTopic)
# MAGIC     .option("startingOffsets", "earliest")
# MAGIC     .load()
# MAGIC
# MAGIC   val memberStream = memberStreamRaw.select(from_json(col("value").cast(StringType), membershipSchema).as("value_json"))
# MAGIC     .selectExpr("value_json.*")
# MAGIC     .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").as("eventTimestamp"))
# MAGIC     .withWatermark("eventTimestamp", "3 minutes")
# MAGIC     // .as[VideoUsageEvent]
# MAGIC
# MAGIC   val usageStream = usageStreamRaw.select(from_json(col("value").cast(StringType), usageSchema).as("value_json"))
# MAGIC     .selectExpr("value_json.*")
# MAGIC     .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").as("eventTimestamp"))
# MAGIC     .withWatermark("eventTimestamp", "3 minutes")
# MAGIC     // .as[VideoUsageEvent]
# MAGIC     
# MAGIC
# MAGIC   val resultDF: DataFrame = usageStream.as("u")
# MAGIC       .join(memberStream.as("m")
# MAGIC         , expr("u.user = m.user AND u.eventTimestamp >= m.startDate AND (m.endDate is null OR u.eventTimestamp < m.endDate) AND m.eventTimestamp between u.eventTimestamp - interval 1 minutes and u.eventTimestamp + interval 3 minutes")
# MAGIC       )
# MAGIC       .drop("u.user")
# MAGIC       // .select("u.*", "membershipId", "planId", "startDate", "endDate", "updatedAt")
# MAGIC
# MAGIC   val writeMode = "append"
# MAGIC
# MAGIC   val streamingQuery = resultDF
# MAGIC     .selectExpr("concat(cast(usageId as string),'-',membershipId) as key", "to_json(struct(*)) as value")
# MAGIC     .writeStream.outputMode(writeMode)
# MAGIC     .format("kafka")
# MAGIC     .options(kafkaOptions)
# MAGIC     .option("topic", "member_video_views")
# MAGIC     .option("checkpointLocation", checkpointPath)
# MAGIC     .option("numStateStoreInstances", 16)
# MAGIC     // .trigger(Trigger.ProcessingTime("20 seconds"))
# MAGIC     .trigger(Trigger.AvailableNow)
# MAGIC     .start
# MAGIC
# MAGIC   // streamingQuery.awaitTermination() // used if trigger is set to a time interval instead of AvailableNow
# MAGIC

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# spark.conf.set("spark.sql.shuffle.partitions", "16")

# spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
# spark.conf.set("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true" )
# spark.conf.set("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "2000")
# spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "true" )
# spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true" )
# # spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")


# run_version = "v0.1"

# topic = 'video_usage'
# GROUP_ID = f'tst-group-video-usage-{run_version}'

# # To setup Key Vault backed secret scope for this first time, replace items in url and follow instructions: 
# #   https://<databricks-instance>/#secrets/createScopeSetup

# def get_event_hub_config():
#     # Password is really a Event Hub connection string, for example ->
#     # Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=ReadWriteTmp;SharedAccessKey=vhNXxXXXXXxxxXXXXXXXxx=;EntityPath=demo-message-1
#   password = dbutils.secrets.get(scope = "demo", key = "eh-sasl-{0}".format(topic))

#   EH_SASL = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{0}";'.format(password)

#   config = {
#       'kafka.bootstrap.servers': 'dustin-demo-eh.servicebus.windows.net:9093',
#       'kafka.security.protocol': 'SASL_SSL',
#       'kafka.sasl.mechanism': 'PLAIN',
#       'kafka.group.id': GROUP_ID,
#       'kafka.request.timeout.ms': "60000",
#       'kafka.session.timeout.ms': "20000",
#       'kafka.heartbeat.interval.ms': "10000",
#       'kafka.sasl.jaas.config': EH_SASL,
#       'subscribe': topic
#   }
#   return config


# def get_confluent_cloud_config():
#   bootstrap_servers = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-brokers")
#   username = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-user")
#   password = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-password")
#   SASL = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{0}" password="{1}";'.format(username, password)

#   config = {
#       'kafka.bootstrap.servers': bootstrap_servers,
#       'kafka.security.protocol': 'SASL_SSL',
#       'kafka.sasl.mechanism': 'PLAIN',
#       'kafka.group.id': GROUP_ID,
#       'kafka.request.timeout.ms': "60000",
#       'kafka.session.timeout.ms': "20000",
#       'kafka.heartbeat.interval.ms': "10000",
#       'kafka.sasl.jaas.config': SASL,
#       'subscribe': topic
#   }
#   return config


# consumer_config = get_confluent_cloud_config()
                                 
# # Read from Kafka, format will be a kafka record
# input_df = spark.readStream.format("kafka").options(**consumer_config).load()

# # Cast just the value as a string (instead of bytes) then use from_json to convert to an object matching the schema
# json_df = (
#   input_df.select(
#     from_json(
#       col("value").cast("string"), usage_schema).alias("json"),
#     col("value").cast("string").alias("value_raw")
#   )
# )

# # Select all attribues from json as individual columns, cast trip_distance, add columns
# transformed_df = (
#     json_df
#       .select("json.*", "value_raw")
# )

# # display(transformed_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Storage as a destination
# MAGIC * One option for streaming output is to write directly to you data lake storage (Azure Data Lake Storage Gen 2 or standard Azure Blob Storage).
# MAGIC * Databricks Delta / Delta Lake file format makes this more efficient, but could do with Parquet, Avro or other formats.

# COMMAND ----------

# video_views_delta_path_2 = video_views_delta_path + "_" + run_version

# (
# transformed_df.writeStream
#   .queryName("StreamingVideoViewsDelta")
#   .format("delta")
#   .outputMode("append")
#   .trigger(processingTime="5 seconds")
#   .option("checkpointLocation", f"/delta/events/_checkpoints/streaming_video_views_{run_version}")
#   .start(video_views_delta_path_2)
# )

# COMMAND ----------

# # Read data out of delta table
# delta_stream_df = spark.readStream.format("delta").load(video_views_delta_path_2)
# display(delta_stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternatively: Send transformed data to Event Hubs for next steps in pipeline

# COMMAND ----------

# topic2 = 'demo-message-transformed'

# producer_config = consumer_config
# producer_config.pop('subscribe')
# producer_config['topic'] = topic2

# kafka_output_df = trip_df.selectExpr(
#     "CAST(VendorId as STRING) as key",
#     "to_json(struct(*)) as value")

# # display(kafka_output_df)
# kafka_output_df.writeStream \
#   .format("kafka") \
#   .options(**producer_config) \
#   .option("checkpointLocation", f"/delta/events/_checkpoints/cp_{run_version}") \
#   .start()
