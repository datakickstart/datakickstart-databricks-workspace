# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup logic
# MAGIC Set many variables so its easy to change table names and checkpoint names later.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
from delta.tables import *

source_path = "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json"
source_path_member = "abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/member_json"
checkpoint_path1 = "dbfs:/tmp/checkpoints/video_view_json_autloader_v1"
checkpoint_path2 = "dbfs:/tmp/checkpoints/video_view_silver_autloader_v1"
checkpoint_path_member = "dbfs:/tmp/checkpoints/member_json_autloader_v1"
checkpoint_path_member_silver = "dbfs:/tmp/checkpoints/member_silver_autloader_v1"
checkpoint_path_member_gold = "dbfs:/tmp/checkpoints/member_gold_autloader_v1"

bronze_video_table = "main.default.video_view_bronze"
bronze_member_table = "main.default.member_bronze"
silver_video_table = "main.default.video_view_silver"
silver_member_table = "main.default.member_silver"
silver_video_full_table = "main.default.video_view_full_silver"
gold_member_agg = "main.default.member_totals"

refresh_all = False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear tables and restart
# MAGIC If `refresh_all = True` then clear everything and start over.
# MAGIC Note: If using Delta Live Tables, this is a simple call and doesn't require extra code like shown here. 

# COMMAND ----------

if refresh_all:
    dbutils.fs.rm(checkpoint_path1, recurse=True)
    dbutils.fs.rm(checkpoint_path2, recurse=True)
    dbutils.fs.rm(checkpoint_path_member, recurse=True)
    dbutils.fs.rm(checkpoint_path_member_silver, recurse=True)
    dbutils.fs.rm(checkpoint_path_member_gold, recurse=True)
    spark.sql(f"DROP TABLE {bronze_video_table};")
    spark.sql(f"DROP TABLE {bronze_member_table};")
    spark.sql(f"DROP TABLE {silver_video_table};")
    spark.sql(f"DROP TABLE {silver_member_table};")
    spark.sql(f"DROP TABLE {silver_video_full_table};")
    # spark.sql(f"DROP TABLE {gold_member_agg};")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read incremental using Autoloader
# MAGIC Trigger `availableNow=True` means it will get only new data from the stream. Autoloader takes care of tracking which files have not been processed yet. Several additional options are available to customize how autoloader works.

# COMMAND ----------

 q1 = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", checkpoint_path1)
  .load(source_path)
  .writeStream
  .queryName("bronze_video")
  .option("checkpointLocation", checkpoint_path1)
  .trigger(availableNow=True)
  .toTable(bronze_video_table)
)

q1.processAllAvailable()

# COMMAND ----------

q2 = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", checkpoint_path_member)
  .load(source_path_member)
  .writeStream
  .queryName("bronze_member")
  .option("checkpointLocation", checkpoint_path_member)
  .trigger(availableNow=True)
  .toTable(bronze_member_table)
)

q2.processAllAvailable()

# COMMAND ----------

q3 = (spark.readStream
  .table(bronze_video_table)
  .selectExpr(
    "cast(usageId as int) as usageId",
    "user",
    "cast(eventTimestamp as timestamp) eventTimestamp",
    "cast(durationSeconds as long) as durationSeconds",
    "cast(completed as boolean) as completed",
    "cast(offset as long) as offset",
    "cast(timestamp as timestamp) as receivedTimestamp"
  )
  .writeStream
  .queryName("silver_video")
  .option("checkpointLocation", checkpoint_path2)
  .trigger(availableNow=True)
  .toTable(silver_video_table)
)

q3.processAllAvailable()


# COMMAND ----------

def dedup(df):
    return (df
            .withColumn("row_num", 
                        row_number().over(
                            Window.partitionBy("membershipId").orderBy(desc("offset"))
                            )
                        )
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

def silver_merge(df, batchId):

    merge_sql = f"""
            MERGE INTO {silver_member_table} AS t
            USING s
            ON t.membershipId = s.membershipId
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """

    df = dedup(df)
    df.createOrReplaceTempView("s")
    session = df._jdf.sparkSession()

    try:
        # TODO: Add condition to Matched statement to check offset since order not guaranteed throughout all steps
        session.sql(merge_sql)
    except Exception as e:
        print("Table doesn't exist, creating table before merge.")
        schema = df.schema.fields
        schema_string = ', '.join([c.name + ' ' + c.dataType.typeName() for c in schema])

        session.sql(f"""
            CREATE TABLE {silver_member_table}
            ({schema_string})
            TBLPROPERTIES (
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5',
                'delta.enableChangeDataFeed' = 'true'
                -- ,'delta.columnMapping.mode' = 'name'
            )
         """)
        
        session.sql(merge_sql)
        

q4 = (spark.readStream
  .table(bronze_member_table)
  .writeStream
  .queryName("silver_member")
  .option("checkpointLocation", checkpoint_path_member_silver)
  .trigger(availableNow=True)
  .foreachBatch(silver_merge)
  .start()
)

q4.processAllAvailable()

# COMMAND ----------

# DBTITLE 1,Streaming join - Video view to latest member
# member_df = (spark.readStream.table(silver_member_table)
#             .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
#              .withWatermark("eventTimestamp", '3 minutes')
#             )

# q5 = (spark.readStream
#   .table(bronze_video_table)
#   .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
#   .withWatermark("eventTimestamp", "3 minutes").alias("v")
#   .join(member_df.alias("m"), expr("m.user ==v.user and m.eventTimestamp between v.eventTimestamp and v.eventTimestamp + interval 5 minutes"), how="left")
#   .selectExpr("v.*", "m.membershipId", "m.planId", "m.offset as membershipOffset")
#   .writeStream
#   .queryName("silver_video_full")
#   .option("checkpointLocation", checkpoint_path2)
#   .trigger(availableNow=True)
#   .toTable(silver_video_full_table)
# )

# q5.processAllAvailable()

# COMMAND ----------

# DBTITLE 1,Member views aggregation
spark.sql(f"""CREATE TABLE IF NOT EXISTS {gold_member_agg} 
          (membershipId int, view_date date, view_count int, total_duration long)""")

# member_df = (spark.readStream.table(silver_member_table)
#             .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
#              .withWatermark("eventTimestamp", '3 minutes')
#             )



def member_view_agg(df, batchId):
    df.createOrReplaceTempView("updated_members")
    # df.select("membership").distinct().createOrReplaceTempView("updated_members")

    session = df._jdf.sparkSession()
    session.sql(f"""
        MERGE INTO {gold_member_agg} AS t
        USING (
            SELECT membershipId, cast(sv.eventTimestamp as date) view_date, count(1) as view_count, sum(durationSeconds) as total_duration
            FROM {silver_member_table} sm
            JOIN {silver_video_table} sv
                ON sm.user = sv.user
                    and sv.eventTimestamp between sm.startDate and sm.endDate
            WHERE sm.membershipId in (SELECT distinct membershipId FROM updated_members)
            GROUP BY ALL
        ) AS s
        ON t.membershipId = s.membershipId
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *            
    """)

q6 = (spark.readStream
  .option("readChangeFeed", "true")
  .option("schemaTrackingLocation", checkpoint_path_member_gold)
  .table(silver_member_table)
  .writeStream
  .queryName("gold_member_agg")
  .option("checkpointLocation", checkpoint_path_member_gold)
  .trigger(availableNow=True)
  .foreachBatch(member_view_agg)
  .start()
)

q6.processAllAvailable()


# COMMAND ----------

# MAGIC %md
# MAGIC ## For those who don't like SQL...

# COMMAND ----------

from pyspark.errors import AnalysisException

def silver_merge_pyspark(df, batch_id):
    table = silver_member_table + "2"
    id = "membershipId"
    session =  df._jdf.sparkSession()
    try:
        delta_target = DeltaTable.forName(spark, table)
        df_target = delta_target.toDF()

        delta_target.alias('t') \
        .merge(df.alias('s'), f"t.{id} = s.{id}") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    except AnalysisException as e:
        if str(e).find(f"Path does not exist") > -1 or str(e).find(f"is not a Delta table") > -1:
            print(f"Delta table {table} doesn't exist yet, creating table")
            df.write.format("delta").saveAsTable(table)
        else:
            raise e
    

# df = spark.read.table(bronze_member_table).limit(1000)
# silver_merge_pyspark(df, 0)

(spark.readStream
  .table(bronze_member_table)
  .writeStream
  .option("checkpointLocation", checkpoint_path_member_silver+"2")
  .trigger(availableNow=True)
  .foreachBatch(silver_merge_pyspark)
)

# COMMAND ----------

## Extra conditions
# def silver_merge_pyspark(df, batch_id):
#     table = silver_member_table
#     try:
#         delta_target = DeltaTable.table(table)
#         df_target = delta_target.toDF()
#     except AnalysisException as e:
#         if str(e).find(f"Path does not exist") > -1 or str(e).find(f"is not a Delta table") > -1:
#             print(f"Delta table {table} doesn't exist yet, creating table")
#             df_source.write.format("delta").saveAsTable(table)
    
