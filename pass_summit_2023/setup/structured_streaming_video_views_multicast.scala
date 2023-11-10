// Databricks notebook source
// MAGIC %md
// MAGIC ## Stream Processing To Multiple Destinations
// MAGIC Must have data streaming to the topic "video_usage".
// MAGIC
// MAGIC *Note: If not working, try changing the GROUP_ID and Consumer Group values to reset

// COMMAND ----------

// MAGIC %md
// MAGIC ### Stream Load of incoming data - Video Views
// MAGIC Read streaming data from Confluent Cloud or Event Hubs (using Apache Kafka API) and save in the same delta location within Azure Data Lake Storage (ADLS).

// COMMAND ----------

// MAGIC %python
// MAGIC logger = spark._jvm.org.apache.log4j
// MAGIC log = logger.LogManager.getLogger("myLogger")
// MAGIC
// MAGIC log.info("Starting streaming job")

// COMMAND ----------

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

  val mode = "confluent" //sys.env("KAFKA_PRODUCER_MODE")
  val refreshAll = true

  val bootstrapServers = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-brokers") // sys.env("KAFKA_BROKERS")
  val kafkaAPIKey = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-user") //sys.env("KAFKA_API_KEY")
  val kafkaAPISecret = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-password") //sys.env("KAFKA_API_SECRET")

  val kafkaOptions = Map(
    "kafka.bootstrap.servers"-> bootstrapServers,
    "kafka.security.protocol"-> "SASL_SSL",
    "kafka.sasl.jaas.config"-> s"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule  required username='$kafkaAPIKey'   password='$kafkaAPISecret';",
    "kafka.sasl.mechanism"-> "PLAIN",
    "kafka.client.dns.lookup"-> "use_all_dns_ips",
    "kafka.acks"-> "all"
  )

  val sparkConf = new SparkConf()
    .set("spark.sql.streaming.metricsEnabled", "true")
    // .set("spark.sql.streaming.checkpointLocation", "checkpoints/v3")
    .set("spark.sql.shuffle.partitions", "16")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
// .set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
// .set("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true" )
// .set("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "2000")
// .set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "true" )
// //.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true" )
// // .set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")

  val sparkBase = SparkSession.builder
    .appName("Structured Streaming Video Views Multicast")
    .config(sparkConf)

  val spark =  if (mode == "local") sparkBase.master("local").getOrCreate() else sparkBase.getOrCreate()

  import spark.implicits._

  val memberPath = "dbfs:/tmp/tables/member" //"/opt/data/tables/member"
  // val usageTopic = "video_usage"
  val usageTopic = "usage"
  val membershipTopic = "membership"

  val usageSchema = StructType(Seq(
    StructField("usageId", IntegerType),
    StructField("user", StringType),
    StructField("completed", BooleanType),
    StructField("durationSeconds", IntegerType),
    StructField("eventTimestamp", TimestampType)
  ))

 val membershipSchema = StructType(Seq(
   StructField("membershipId", StringType),
   StructField("user", StringType),
   StructField("planId", StringType),
   StructField("startDate", StringType),
   StructField("endDate", StringType),
   StructField("updatedAt", StringType),
   StructField("eventTimestamp", TimestampType)
 ))

  val usageStreamRaw = spark.readStream
    .format("kafka")
    .options(kafkaOptions)
    .option("subscribe", usageTopic)
    .option("startingOffsets", "latest")
    .load()

  // val membershipLookup = spark.read
  //   .format("delta")
  //   .load(memberPath)

  val memberStreamRaw = spark.readStream
    .format("kafka")
    .options(kafkaOptions)
    .option("subscribe", membershipTopic)
    .option("startingOffsets", "latest")
    .load()

  val memberStream = memberStreamRaw.select(col("key").cast(StringType).as("key"), col("offset"), col("timestamp"), from_json(col("value").cast(StringType), membershipSchema).as("value_json"))
    .selectExpr("key", "offset", "timestamp", "value_json.*")
    .withColumn("membershipId", col("membershipId").cast("int").as("membershipId"))
    .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").as("eventTimestamp"))
    // .as[VideoUsageEvent]

  val usageStream = usageStreamRaw.select(col("key").cast(StringType).as("key"), col("offset"), col("timestamp"), from_json(col("value").cast(StringType), usageSchema).as("value_json"))
    .selectExpr("key", "offset", "timestamp", "value_json.*")
    .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp").as("eventTimestamp"))
    // .as[VideoUsageEvent]
    

  val writeMode = "append"

  if (refreshAll) {
    dbutils.fs.rm("dbfs:/tmp/checkpoints/member_multicast_v1", recurse=true)
    dbutils.fs.rm("dbfs:/tmp/checkpoints/video_view_multicast_v1", recurse=true)
  }

// COMMAND ----------


  // val streamingQuery1 = usageStream
  //   .writeStream.outputMode(writeMode)
  //   .format("delta")
  //   // .options(kafkaOptions)
  //   .option("checkpointLocation", "dbfs:/tmp/checkpoints/video_view_delta_bronze_v1")
  //   .trigger(Trigger.ProcessingTime("1 minute"))
  //   .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_delta")
  //   .start

    // val streamingQuery2 = usageStream
    // .writeStream.outputMode(writeMode)
    // .format("json")
    // .options(kafkaOptions)
    // .option("checkpointLocation", "dbfs:/tmp/checkpoints/video_view_multicast_v1")
    // .trigger(Trigger.ProcessingTime("1 minute"))
    // .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json")
    // .start

  // streamingQuery.awaitTermination()


// COMMAND ----------

  // val streamingQuery3 = memberStream
  //   .writeStream.outputMode(writeMode)
  //   .format("delta")
  //   // .options(kafkaOptions)
  //   .option("checkpointLocation", "dbfs:/tmp/checkpoints/member_delta_bronze_v1")
  //   .trigger(Trigger.ProcessingTime("1 minute"))
  //   .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_delta")
  //   .start

    
    // val streamingQuery4 = memberStream
    // .writeStream.outputMode(writeMode)
    // .format("json")
    // .options(kafkaOptions)
    // .option("checkpointLocation", "dbfs:/tmp/checkpoints/member_multicast_v1")
    // .trigger(Trigger.ProcessingTime("1 minute"))
    // .option("path", "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json")
    // .start

// COMMAND ----------

// MAGIC %md
// MAGIC ## Write to Azure SQL DB or SQL Server
// MAGIC This can be used to write to a variety of SQL Server compatible destinations.
// MAGIC
// MAGIC ### Pre-req
// MAGIC If using mode of overwrite you can let it create the table for you. For this example it is set to append so you should create the table first using the following scripts on your SQL environment.
// MAGIC ```
// MAGIC SET ANSI_NULLS ON
// MAGIC GO
// MAGIC SET QUOTED_IDENTIFIER ON
// MAGIC GO
// MAGIC
// MAGIC IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[video_view]') AND type in (N'U'))
// MAGIC DROP TABLE [dbo].[video_view]
// MAGIC GO
// MAGIC
// MAGIC CREATE TABLE [dbo].[video_view](
// MAGIC 	[key] [varbinary](max) NULL,
// MAGIC 	[offset] [bigint] NULL,
// MAGIC 	[timestamp] [datetime] NULL,
// MAGIC 	[usageId] [int] NULL,
// MAGIC 	[user] [nvarchar](max) NULL,
// MAGIC 	[completed] [bit] NULL,
// MAGIC 	[durationSeconds] [int] NULL,
// MAGIC 	[eventTimestamp] [datetime] NULL
// MAGIC ) ON [PRIMARY]
// MAGIC GO
// MAGIC ```
// MAGIC
// MAGIC ```
// MAGIC SET ANSI_NULLS ON
// MAGIC GO
// MAGIC SET QUOTED_IDENTIFIER ON
// MAGIC GO
// MAGIC
// MAGIC IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[member]') AND type in (N'U'))
// MAGIC DROP TABLE [dbo].[member]
// MAGIC GO
// MAGIC
// MAGIC CREATE TABLE [dbo].[member](
// MAGIC 	[membershipId] [nvarchar](max) NULL,
// MAGIC 	[user] [nvarchar](max) NULL,
// MAGIC 	[planId] [nvarchar](max) NULL,
// MAGIC 	[startDate] [nvarchar](max) NULL,
// MAGIC 	[endDate] [nvarchar](max) NULL,
// MAGIC 	[updatedAt] [nvarchar](max) NULL,
// MAGIC 	[eventTimestamp] [datetime] NULL
// MAGIC ) ON [PRIMARY]
// MAGIC GO
// MAGIC ```
// MAGIC

// COMMAND ----------

def sqlWrite(batchDF: DataFrame, batchId: Long, table: String, path: String): Unit = {
  val writeMode = "append"
  val database = "sandbox-2-sql"
  val db_host_name = "sandbox-2-sqlserver.database.windows.net"
  val url = s"jdbc:sqlserver://${db_host_name};databaseName=${database}"
  val user = dbutils.secrets.get("demo", "sql-user-stackoverflow") // databricks
  val password = dbutils.secrets.get("demo", "sql-pwd-stackoverflow") // databricks    

  val sqlOptions = Map(
          "url" -> url,
          "dbtable" -> table,
          "user"-> user,
        "password" -> password,
        "reliabilityLevel" ->"BEST_EFFORT",
        "tableLock" ->"false",
        "batchsize" -> "100000" ,
        "schemaCheckEnabled" -> "false"
      )
  
  batchDF.write
      .format("com.microsoft.sqlserver.jdbc.spark")
      .options(sqlOptions)
      .mode(SaveMode.Append)
      .save()

  // batchDF.write
  //     .format("jdbc")
  //     .option("url", url)
  //     .option("dbtable", table)
  //     .option("user", user)
  //     .option("password", password)
  //     .mode(SaveMode.Append)
  //     .save()

  batchDF.write
    .mode(writeMode)
    .format("json")
    .option("path", path)
    .save()
}

val streamingQuery5 = memberStream
    .writeStream
    .queryName("member_foreachBatch")
    .foreachBatch((batchDF: DataFrame, batchId: Long) => sqlWrite(batchDF, batchId, "dbo.member",
              "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json"))
    .option("checkpointLocation", "dbfs:/tmp/checkpoints/member_multicast_v1")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start


// COMMAND ----------

val streamingQuery6 = usageStream
    .writeStream
    .queryName("video_views_foreachBatch")
    .foreachBatch((batchDF: DataFrame, batchId: Long) => sqlWrite(batchDF, batchId, "dbo.video_view",
              "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json"))
    .option("checkpointLocation", "dbfs:/tmp/checkpoints/video_view_multicast_v1")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cleanup

// COMMAND ----------

// MAGIC %python
// MAGIC # %fs rm -r dbfs:/tmp/checkpoints/member_multicast_v1

// COMMAND ----------

// MAGIC %python
// MAGIC # %fs rm -r dbfs:/tmp/checkpoints/video_view_multicast_v1

// COMMAND ----------

// MAGIC %python
// MAGIC # %fs rm -r abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json

// COMMAND ----------

// MAGIC %python
// MAGIC # %fs rm -r abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json
