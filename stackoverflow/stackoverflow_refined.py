# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime
from utils.logging_utils import *

load_time = datetime.now()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "stackoverflow"
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "stackoverflow"
raw_format = "delta"
refined_format = "delta"
job_name = "stackoverflow_refined"

logger = start_logging(spark, job_name)

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
    status = dbutils.notebook.run("stackoverflow_refined_table_load", 600, arguments=args)
    print(status)
    if status != 'success':
        raise Exception(f"Failed to load refined database. Status: {str(status)}")

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()
worker_count = 3
errors = {}

def run_tasks(function, q):
    while not q.empty():
        try:
            value = q.get()
            function(value)
        except Exception as e:
            table = value.get("table", "UNKNOWN TABLE")
            msg = f"Error processing table {table}: {str(e)}"
            errors[value] = e
            log_error_message(msg)
        finally:
            q.task_done()


table_list = [
    {"table": "badges_delta", "id_column": "id", 
     "columns": "_Class as class,_Date as event_date,_Id as id,_Name as name,_UserId as userid"
    },
    {"table": "comments_delta", "id_column": "id", 
     "columns": "_ContentLicense as content_license,_CreationDate as creation_date,_Id as id,_PostId as post_id,_Score as score,_Text as text,_UserDisplayName as user_display_name,_UserId as user_id"
    }
]

print(table_list)

for table_args in table_list:
    q.put(table_args)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()

if len(errors) == 0:
    log_informational_message("All tasks completed successfully.")
elif len(errors) > 0:
    msg = f"Errors during tasks {list(errors.keys())} -> \n {str(errors)}"
    raise Exception(msg)

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


# COMMAND ----------

stop_logging(job_name)
