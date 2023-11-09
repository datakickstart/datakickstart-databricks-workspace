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

# MAGIC %scala
# MAGIC import org.apache.spark.SparkConf
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC import org.apache.spark.sql.functions.{col, expr, from_json}
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.{DataFrame, SparkSession}
# MAGIC
# MAGIC   val mode = "confluent" //sys.env("KAFKA_PRODUCER_MODE")
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
# MAGIC     // .set("spark.sql.streaming.checkpointLocation", "checkpoints/v3")
# MAGIC     .set("spark.sql.shuffle.partitions", "16")
# MAGIC     .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# MAGIC // .set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
# MAGIC // .set("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true" )
# MAGIC // .set("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "2000")
# MAGIC // .set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "true" )
# MAGIC // //.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true" )
# MAGIC // // .set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
# MAGIC
# MAGIC   val sparkBase = SparkSession.builder
# MAGIC     .appName("Structured Streaming Video Views Multicast")
# MAGIC     .config(sparkConf)
# MAGIC
# MAGIC   val spark =  if (mode == "local") sparkBase.master("local").getOrCreate() else sparkBase.getOrCreate()
# MAGIC
# MAGIC   import spark.implicits._
# MAGIC
# MAGIC   val memberPath = "dbfs:/tmp/tables/member" //"/opt/data/tables/member"
# MAGIC   // val usageTopic = "video_usage"
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
# MAGIC    StructField("membershipId", IntegerType),
# MAGIC    StructField("user", StringType),
# MAGIC    StructField("planId", StringType),
# MAGIC    StructField("startDate", StringType),
# MAGIC    StructField("endDate", StringType),
# MAGIC    StructField("updatedAt", StringType),
# MAGIC    StructField("eventTimestamp", TimestampType)
# MAGIC  ))
# MAGIC
# MAGIC   val usageStreamRaw = spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .options(kafkaOptions)
# MAGIC     .option("subscribe", usageTopic)
# MAGIC     .option("startingOffsets", "latest")
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
# MAGIC     .option("startingOffsets", "latest")
# MAGIC     .load()
# MAGIC
# MAGIC   val memberStream = memberStreamRaw.select(col("key").cast(StringType).as("key"), col("offset"), col("timestamp"), from_json(col("value").cast(StringType), membershipSchema).as("value_json"))
# MAGIC     .selectExpr("key", "offset", "timestamp", "value_json.*")
# MAGIC     .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").as("eventTimestamp"))
# MAGIC     // .as[VideoUsageEvent]
# MAGIC
# MAGIC   val usageStream = usageStreamRaw.select(col("key").cast(StringType).as("key"), col("offset"), col("timestamp"), from_json(col("value").cast(StringType), usageSchema).as("value_json"))
# MAGIC     .selectExpr("key", "offset", "timestamp", "value_json.*")
# MAGIC     .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").as("eventTimestamp"))
# MAGIC     // .as[VideoUsageEvent]
# MAGIC     
# MAGIC
# MAGIC   val writeMode = "append"

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC   // val streamingQuery1 = usageStream
# MAGIC   //   .writeStream.outputMode(writeMode)
# MAGIC   //   .format("delta")
# MAGIC   //   // .options(kafkaOptions)
# MAGIC   //   .option("checkpointLocation", "dbfs:/tmp/checkpoints/video_view_delta_bronze_v1")
# MAGIC   //   .trigger(Trigger.ProcessingTime("1 minute"))
# MAGIC   //   .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_delta")
# MAGIC   //   .start
# MAGIC
# MAGIC     // val streamingQuery2 = usageStream
# MAGIC     // .writeStream.outputMode(writeMode)
# MAGIC     // .format("json")
# MAGIC     // .options(kafkaOptions)
# MAGIC     // .option("checkpointLocation", "dbfs:/tmp/checkpoints/video_view_multicast_v1")
# MAGIC     // .trigger(Trigger.ProcessingTime("1 minute"))
# MAGIC     // .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json")
# MAGIC     // .start
# MAGIC
# MAGIC   // streamingQuery.awaitTermination()
# MAGIC

# COMMAND ----------

# MAGIC   %scala
# MAGIC   // val streamingQuery3 = memberStream
# MAGIC   //   .writeStream.outputMode(writeMode)
# MAGIC   //   .format("delta")
# MAGIC   //   // .options(kafkaOptions)
# MAGIC   //   .option("checkpointLocation", "dbfs:/tmp/checkpoints/member_delta_bronze_v1")
# MAGIC   //   .trigger(Trigger.ProcessingTime("1 minute"))
# MAGIC   //   .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_delta")
# MAGIC   //   .start
# MAGIC
# MAGIC     
# MAGIC     // val streamingQuery4 = memberStream
# MAGIC     // .writeStream.outputMode(writeMode)
# MAGIC     // .format("json")
# MAGIC     // .options(kafkaOptions)
# MAGIC     // .option("checkpointLocation", "dbfs:/tmp/checkpoints/member_multicast_v1")
# MAGIC     // .trigger(Trigger.ProcessingTime("1 minute"))
# MAGIC     // .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json")
# MAGIC     // .start

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Azure SQL DB or SQL Server
# MAGIC This can be used to write to a variety of SQL Server compatible destinations.
# MAGIC
# MAGIC ### Pre-req
# MAGIC If using mode of overwrite you can let it create the table for you. For this example it is set to append so you should create the table first using the following scripts on your SQL environment.
# MAGIC ```
# MAGIC SET ANSI_NULLS ON
# MAGIC GO
# MAGIC SET QUOTED_IDENTIFIER ON
# MAGIC GO
# MAGIC
# MAGIC IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[video_view]') AND type in (N'U'))
# MAGIC DROP TABLE [dbo].[video_view]
# MAGIC GO
# MAGIC
# MAGIC CREATE TABLE [dbo].[video_view](
# MAGIC 	[key] [varbinary](max) NULL,
# MAGIC 	[offset] [bigint] NULL,
# MAGIC 	[timestamp] [datetime] NULL,
# MAGIC 	[usageId] [int] NULL,
# MAGIC 	[user] [nvarchar](max) NULL,
# MAGIC 	[completed] [bit] NULL,
# MAGIC 	[durationSeconds] [int] NULL,
# MAGIC 	[eventTimestamp] [datetime] NULL
# MAGIC ) ON [PRIMARY]
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC SET ANSI_NULLS ON
# MAGIC GO
# MAGIC SET QUOTED_IDENTIFIER ON
# MAGIC GO
# MAGIC
# MAGIC IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[member]') AND type in (N'U'))
# MAGIC DROP TABLE [dbo].[member]
# MAGIC GO
# MAGIC
# MAGIC CREATE TABLE [dbo].[member](
# MAGIC 	[membershipId] [nvarchar](max) NULL,
# MAGIC 	[user] [nvarchar](max) NULL,
# MAGIC 	[planId] [nvarchar](max) NULL,
# MAGIC 	[startDate] [nvarchar](max) NULL,
# MAGIC 	[endDate] [nvarchar](max) NULL,
# MAGIC 	[updatedAt] [nvarchar](max) NULL,
# MAGIC 	[eventTimestamp] [datetime] NULL
# MAGIC ) ON [PRIMARY]
# MAGIC GO
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC def sqlWrite(batchDF: DataFrame, batchId: Long, table: String, path: String): Unit = {
# MAGIC   val writeMode = "append"
# MAGIC   val database = "sandbox-2-sql"
# MAGIC   val db_host_name = "sandbox-2-sqlserver.database.windows.net"
# MAGIC   val url = s"jdbc:sqlserver://${db_host_name};databaseName=${database}"
# MAGIC   val user = dbutils.secrets.get("demo", "sql-user-stackoverflow") // databricks
# MAGIC   val password = dbutils.secrets.get("demo", "sql-pwd-stackoverflow") // databricks    
# MAGIC
# MAGIC   val sqlOptions = Map(
# MAGIC           "url" -> url,
# MAGIC           "dbtable" -> table,
# MAGIC           "user"-> user,
# MAGIC         "password" -> password,
# MAGIC         "reliabilityLevel" ->"BEST_EFFORT",
# MAGIC         "tableLock" ->"false",
# MAGIC         "batchsize" -> "100000"
# MAGIC         // ,
# MAGIC         // "schemaCheckEnabled" -> "false"
# MAGIC       )
# MAGIC   
# MAGIC   batchDF.write
# MAGIC       .format("com.microsoft.sqlserver.jdbc.spark")
# MAGIC       .options(sqlOptions)
# MAGIC       .mode(SaveMode.Append)
# MAGIC       .save()
# MAGIC
# MAGIC   // batchDF.write
# MAGIC   //     .format("jdbc")
# MAGIC   //     .option("url", url)
# MAGIC   //     .option("dbtable", table)
# MAGIC   //     .option("user", user)
# MAGIC   //     .option("password", password)
# MAGIC   //     .mode(SaveMode.Append)
# MAGIC   //     .save()
# MAGIC
# MAGIC   // batchDF.write
# MAGIC   //   .mode(writeMode)
# MAGIC   //   .format("json")
# MAGIC   //   .option("path", path)
# MAGIC   //   .save()
# MAGIC }
# MAGIC
# MAGIC val streamingQuery5 = memberStream
# MAGIC     .writeStream
# MAGIC     .queryName("member_foreachBatch")
# MAGIC     .foreachBatch((batchDF: DataFrame, batchId: Long) => sqlWrite(batchDF, batchId, "dbo.member",
# MAGIC               "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json"))
# MAGIC     .option("checkpointLocation", "dbfs:/tmp/checkpoints/member_multicast_v1")
# MAGIC     .trigger(Trigger.ProcessingTime("1 minute"))
# MAGIC     .start
# MAGIC
# MAGIC val streamingQuery6 = usageStream
# MAGIC     .writeStream
# MAGIC     .queryName("video_views_foreachBatch")
# MAGIC     .foreachBatch((batchDF: DataFrame, batchId: Long) => sqlWrite(batchDF, batchId, "dbo.video_view",
# MAGIC               "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json"))
# MAGIC     .option("checkpointLocation", "dbfs:/tmp/checkpoints/video_view_multicast_v1")
# MAGIC     .trigger(Trigger.ProcessingTime("1 minute"))
# MAGIC     .start
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# %fs rm -r dbfs:/tmp/checkpoints/member_multicast_v1

# COMMAND ----------

# %fs rm -r dbfs:/tmp/checkpoints/video_view_multicast_v1

# COMMAND ----------

# %fs rm -r abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json

# COMMAND ----------

# %fs rm -r abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json
