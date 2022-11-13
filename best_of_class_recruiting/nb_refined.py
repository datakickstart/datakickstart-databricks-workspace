# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "cu"
refined_base_path = "/mnt/dlpssa/refined" #dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
raw_format = "parquet"
refined_format = "delta"

adls_authenticate()

# COMMAND ----------

# def create_database(db_name, path, drop=False):
#     if drop:
#         spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")    
#     spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{path}'")

# create_database("refined", refined_base_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Delete records (for demo purposes)

# COMMAND ----------

# # DELETE records for testing and demo purposes
# raw_df2 = df_source.filter((col('area_code') != 'S35A') & (col('area_code') != 'S300'))
# raw_df2.write.format(raw_format).mode("overwrite").save(raw_base_path + "/area")

# spark.sql(f'REFRESH TABLE "{raw_base_path}/area"')
# df_source = spark.read.format(raw_format).load(raw_base_path + "/area").select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load refined tables concurrently

# COMMAND ----------


def load_table(args):
    status = dbutils.notebook.run("nb_refined_table_load", 60, arguments=args)
    print(status)
    if status != 'success':
        raise Exception(f"Failed to load refined database. Status: {str(status)}")

# status = dbutils.notebook.run("nb_refined_table_load", 60, arguments={"table": "area", "id_column": "area_code", "columns": "area_code,area_name,display_level" })

table_list = [
    {"table": "area", "id_column": "area_code", "columns": "area_code,area_name,display_level"},
    {"table": "series", "id_column": "series_id", 
     "columns": "series_id,area_code,item_code,seasonal,periodicity_code,base_code,base_period,series_title,begin_year,begin_period,end_year,end_period"},
    {"table": "current", "id_column": "series_id,year,period", "columns": "series_id,year,period,value"},
    {"table": "item", "id_column": "item_code", "columns": "item_code,item_name,display_level,sort_sequence"}
]

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()

worker_count = 2

def run_tasks(function, q):
    while not q.empty():
        try:
            value = q.get()
            function(value)
        except Exception as e:
            table = value.get("table", "UNKNOWN TABLE")
            print(f"Error processing table {table}")
            raise e
        finally:
            q.task_done()


print(table_list)

for table_args in table_list:
    q.put(table_args)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()
