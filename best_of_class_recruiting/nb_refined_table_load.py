# Databricks notebook source
dbutils.widgets.text("table", "area")
dbutils.widgets.text("id_column", "area_code")
dbutils.widgets.text("columns","area_code,area_name,display_level")

# COMMAND ----------

# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col, expr
from datetime import datetime

# load_time = datetime.now()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "cu"
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
raw_format = "parquet"
refined_format = "delta"
refined_db = "refined"
 
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

from pyspark.sql.utils import AnalysisException
from delta.tables import *

table = dbutils.widgets.get("table")
id = dbutils.widgets.get("id_column")
columns = dbutils.widgets.get("columns").split(',')

print(columns)

raw_df = spark.read.format(raw_format).load(f"{raw_base_path}/{table}")
df_source = raw_df.select(*columns).withColumn("is_deleted", lit(False))


try:
    delta_target = DeltaTable.forName(spark, f"{refined_db}.{table}")
    df_target = delta_target.toDF().filter("is_deleted == False")
except AnalysisException as e:
    if str(e).find('not a Delta table'):
        print("Table does not exist, need to create table first.")
        df_source.write.format("delta").saveAsTable(f"{refined_db}.{table}")
        dbutils.notebook.exit("success")

# COMMAND ----------

# Build id string
id_list = id.split(',')
if ',' in id:
    id_str = ' and '.join(f"t.{i} = s.{i}" for i in id_list)
else:
    id_str = id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete if not in source

# COMMAND ----------

df_source_ids = df_source.select(*id_list)
df_deleted = df_target.alias('t').join(df_source_ids.alias('s'), expr(id_str), "left_anti").withColumn("is_deleted", lit(True))
display(df_deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert if any column changed

# COMMAND ----------

# Upsert to delta target table
update_dct = {f"{c}": f"s.{c}" for c in df_target.columns if c not in id}
condition_str = ' or '.join(f"t.{k} != {v}" for k,v in update_dct.items())

df_source = df_source.union(df_deleted)

print(id_str)
print(condition_str)

delta_target.alias('t') \
.merge(df_source.alias('s'), id_str) \
.whenMatchedUpdate(condition=f"{id_str} and ({condition_str})", set=update_dct) \
.whenNotMatchedInsertAll() \
.execute()

# COMMAND ----------

dbutils.notebook.exit("success")
