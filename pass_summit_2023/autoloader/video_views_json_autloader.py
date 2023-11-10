# Databricks notebook source
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

refresh_all = True


# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE main.default.video_view_bronze

# COMMAND ----------

if refresh_all:
    dbutils.fs.rm(checkpoint_path1, recurse=True)
    dbutils.fs.rm(checkpoint_path2, recurse=True)
    dbutils.fs.rm(checkpoint_path_member, recurse=True)
    dbutils.fs.rm(checkpoint_path_member_silver, recurse=True)
    dbutils.fs.rm(checkpoint_path_member_gold, recurse=True)

# COMMAND ----------

(spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", checkpoint_path1)
  .load(source_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path1)
  .trigger(availableNow=True)
  .toTable(bronze_video_table)
)

# COMMAND ----------

(spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", checkpoint_path_member)
  .load(source_path_member)
  .writeStream
  .option("checkpointLocation", checkpoint_path_member)
  .trigger(availableNow=True)
  .toTable(bronze_member_table)
)

# COMMAND ----------

(spark.readStream
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
  .option("checkpointLocation", checkpoint_path2)
  .trigger(availableNow=True)
  .toTable(silver_video_table)
)


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
        print(e)
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
        

(spark.readStream
  .table(bronze_member_table)
  .writeStream
  .option("checkpointLocation", checkpoint_path_member_silver)
  .trigger(availableNow=True)
  .foreachBatch(silver_merge)
  .start()
)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Streaming join - Video view to latest member
member_df = (spark.readStream.table(silver_member_table)
            .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
             .withWatermark("eventTimestamp", '3 minutes')
            )

silver_df = (spark.readStream
  .table(bronze_video_table)
  .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
  .withWatermark("eventTimestamp", "3 minutes").alias("v")
  .join(member_df.alias("m"), expr("m.user ==v.user and m.eventTimestamp between v.eventTimestamp and v.eventTimestamp + interval 5 minutes"), how="left")
  .selectExpr("v.*", "m.membershipId", "m.planId", "m.offset as membershipOffset")
  .writeStream
  .option("checkpointLocation", checkpoint_path2)
  .trigger(availableNow=True)
  .toTable(silver_video_full_table)
)


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

(spark.readStream
  .option("readChangeFeed", "true")
  .option("schemaTrackingLocation", checkpoint_path_member_gold)
  .table(silver_member_table)
  .writeStream
  .option("checkpointLocation", checkpoint_path_member_gold)
  .trigger(availableNow=True)
  .foreachBatch(member_view_agg)
  .start()
)


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
    

df = spark.read.table(bronze_member_table).limit(1000)

silver_merge_pyspark(df, 0)

# (spark.readStream
#   .table(bronze_member_table)
#   .writeStream
#   .option("checkpointLocation", checkpoint_path_member_silver)
#   .trigger(availableNow=True)
#   .foreachBatch(silver_merge)
# )

# COMMAND ----------



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
    
