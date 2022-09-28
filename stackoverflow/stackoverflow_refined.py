# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "stackoverflow"
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "stackoverflow"
raw_format = "delta"
refined_format = "delta"

adls_authenticate()

# COMMAND ----------

def create_database(db_name, path, drop=False):
    if drop:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{path}'")

# create_database("refined", refined_base_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load refined tables concurrently

# COMMAND ----------

def load_table(args):
    status = dbutils.notebook.run("stackoverflow_refined_table_load", 60, arguments=args)
    print(status)
    if status != 'success':
        raise Exception(f"Failed to load refined database. Status: {str(status)}")

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
            print(e)
        finally:
            q.task_done()


table_list = [
    {"table": "badges_delta", "id_column": "id", 
     "columns": "_Class as class, _Date as event_date, _Id as id, _Name as name, _UserId as userid"
    }]

print(table_list)

for table_args in table_list:
    q.put(table_args)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example to read only changes for next steps

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generic example from Databricks to merge the changes to gold
# MAGIC -- MERGE INTO goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC --         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC --         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------

# Example to read changes from refined before processing and loading to curated/certified zone\
# Need to find the right version, to do this manually run:
# display(delta_target.history())

table = "badges_delta"
refined_change_feed = spark.read.format("delta").option("readChangeData", True).option("startingVersion",0).load(f"{refined_base_path}/{table}")
display(refined_change_feed)

