# Databricks notebook source
dbutils.widgets.text("table", "area")
dbutils.widgets.text("id_column", "area_code")
dbutils.widgets.text("destination_table","dim_area")

# COMMAND ----------

# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
refined_base_path = "/mnt/dlpssa/refined" #dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
curated_base_path = "/mnt/dlpssa/curated" #dbutils.secrets.get("demo", "refined-datalake-path") + "cu_curated"
refined_format = "delta"
curated_format = "delta"
refined_db = 'refined'
curated_db = 'curated'
 
# adls_authenticate()

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prep source and target tables

# COMMAND ----------

from delta.tables import *
from datetime import datetime, timedelta
from pyspark.sql.functions import col, min, max

table = dbutils.widgets.get("table")
id = dbutils.widgets.get("id_column")
destination_table = dbutils.widgets.get("destination_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert if any column changed

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from delta.tables import *

try:
    delta_target = DeltaTable.forName(spark, f"{curated_db}.{destination_table}")

    min_date = datetime(2022,1,1)
    last_updated = spark.sql(f"select max(last_modified) last_modified from {curated_db}.{destination_table}").first().last_modified
    last_updated = (last_updated or min_date) + timedelta(hours=0, minutes=0, seconds=1)

    refined_df =spark.read.format("delta").option("readChangeFeed", "true") \
            .option("startingTimestamp", last_updated) \
            .load(f"{refined_base_path}/{table}") \
            .withColumnRenamed("_commit_timestamp", "last_modified") \
            .drop("_commit_version") \
            .drop("_change_type")

    # surrogate_key = destination_table + '_id'
    update_dct = {f"{c}": f"s.{c}" for c in refined_df.columns} #if c not in [id]

    delta_target.alias('t') \
    .merge(refined_df.alias('s'), f"t.{id} = s.{id}") \
    .whenMatchedUpdate(set=update_dct) \
    .whenNotMatchedInsert(values=update_dct) \
    .execute()
except AnalysisException as e:
    if str(e).find('not a Delta table') > 0 or str(e).find('no viable alternative at input') > 0:
        print("Table does not exist, need to create table first.")
        
        refined_df =spark.read.format("delta").option("readChangeFeed", "true") \
            .option("startingVersion", 0) \
            .load(f"{refined_base_path}/{table}") \
            .withColumnRenamed("_commit_timestamp", "last_modified") \
            .drop("_commit_version") \
            .drop("_change_type")
        refined_df.write.format("delta").saveAsTable(f"{curated_db}.{destination_table}")
    elif str(e).find('after the latest version available') > 0:
        print(f"No new updates found for table {destination_table}")


# COMMAND ----------

dbutils.notebook.exit("success")
