# Databricks notebook source
from pyspark.sql.functions import *

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
gold_member_agg = "main.default.member_totals"

refresh_all = False


# COMMAND ----------

if refresh_all:
    dbutils.fs.rm(checkpoint_path1, recurse=True)
    dbutils.fs.rm(checkpoint_path2, recurse=True)

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
  .writeStream
  .option("checkpointLocation", checkpoint_path2)
  .trigger(availableNow=True)
  .toTable(silver_video_table)
)


# COMMAND ----------

from delta.tables import *

def silver_merge(df, batch_id):
    silver_video_table
    try:
        delta_target = DeltaTable.table(silver_video_table)
        df_target = delta_target.toDF()
    except AnalysisException as e:
        if str(e).find(f"Path does not exist") > -1 or str(e).find(f"is not a Delta table") > -1:
            print(f"Delta table {table} doesn't exist yet, creating table and exiting notebook early.")
            df_source.write.format("delta").save(f"{refined_base_path}/{table}")
            dbutils.notebook.exit("success")
    
    delta_target.alias('t') \
        .merge(df_source.alias('s'), f"t.{id} = s.{id}") \
        .whenMatchedUpdate(condition=f"t.{id} = s.{id} and ({condition_str})", set=update_dct) \
        .whenNotMatchedInsertAll() \
        .execute()

(spark.readStream
  .table(bronze_member_table)
  .writeStream
  .option("checkpointLocation", checkpoint_path_member_silver)
  .trigger(availableNow=True)
  .foreachBatch(silver_merge)
)

# COMMAND ----------

member_df = (spark.readStream.table(silver_member_table)
            .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
             .withWatermark("eventTimestamp", '3 minutes')
            )

silver_df = (spark.readStream
  .table(bronze_video_table)
  .withColumn("eventTimestamp", col("eventTimestamp").cast("timestamp"))
  .withWatermark("eventTimestamp", "3 minutes").alias("v")
  .join(member_df.alias("m"), expr("m.user ==v.user and m.eventTimestamp between v.eventTimestamp and v.eventTimestamp + interval 5 minutes"), how="left")
  .selectExpr("v.*", )
#   .writeStream
#   .option("checkpointLocation", checkpoint_path2)
#   .trigger(availableNow=True)
#   .toTable(silver_video_table)
)
display(silver_df)
