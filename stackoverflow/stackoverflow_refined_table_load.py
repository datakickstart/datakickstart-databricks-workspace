# Databricks notebook source
dbutils.widgets.text("table", "area")
dbutils.widgets.text("id_column", "area_code")
dbutils.widgets.text("columns","area_code,area_name,display_level")

# COMMAND ----------

# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from pyspark.sql.utils import AnalysisException
from datetime import datetime

load_time = datetime.now()
raw_base_path = 'dbfs:/mnt/datalake/raw/stackoverflow'
refined_base_path = 'dbfs:/mnt/datalake/refined/stackoverflow'
raw_format = "delta"
refined_format = "delta"

 
adls_authenticate()

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------


# #Area table - SETUP
# area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area_original")
# area_refined = area_raw_df.select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))
# area_refined.write.format(refined_format).mode("overwrite").option("enableChangeDataFeed","true").save(refined_base_path + "/area")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prep source and target tables

# COMMAND ----------

from delta.tables import *

table = dbutils.widgets.get("table")
id = dbutils.widgets.get("id_column")
columns = dbutils.widgets.get("columns").split(',')

print(columns)

raw_df = spark.read.format(raw_format).load(f"{raw_base_path}/{table}")
df_source = raw_df.selectExpr(*columns).withColumn("is_deleted", lit(False))

try:
    delta_target = DeltaTable.forPath(spark, f"{refined_base_path}/{table}")
    df_target = delta_target.toDF()
except AnalysisException as e:
    if str(e).find(f"Path does not exist") > -1 or str(e).find(f"is not a Delta table") > -1:
        print(f"Delta table {table} doesn't exist yet, creating table and exiting notebook early.")
        df_source.write.format("delta").save(f"{refined_base_path}/{table}")
        dbutils.notebook.exit("success")

# display(df_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete if not in source

# COMMAND ----------

t = df_target.filter("is_deleted == False")
df_source_ids = df_source.select(id)
df_deleted = t.join(df_source_ids, t[id] == df_source_ids[id], "left_anti").withColumn("is_deleted", lit(True))
display(df_deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert if any column changed

# COMMAND ----------

# Upsert to delta target table
update_dct = {f"{c}": f"s.{c}" for c in df_target.columns if c != id}
condition_str = ' or '.join(f"t.{k} != {v}" for k,v in update_dct.items())

df_source = df_source.union(df_deleted)

print(condition_str)

delta_target.alias('t') \
.merge(df_source.alias('s'), f"t.{id} = s.{id}") \
.whenMatchedUpdate(condition=f"t.{id} = s.{id} and ({condition_str})", set=update_dct) \
.whenNotMatchedInsertAll() \
.execute()

# COMMAND ----------

dbutils.notebook.exit("success")
