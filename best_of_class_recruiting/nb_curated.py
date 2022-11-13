# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

# load_time = datetime.now()
refined_base_path = "/mnt/dlpssa/refined" #dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
curated_base_path = "/mnt/dlpssa/curated" #dbutils.secrets.get("demo", "refined-datalake-path") + "cu_curated"
refined_format = "delta"
curated_format = "delta"
curated_db = "curated"

# adls_mount(account_name="dlpssa4dev04", container="raw", mnt_pnt="/mnt/dlpssa/raw")
try:
    adls_mount(account_name="dlpssa4dev04", container="refined", mnt_pnt="/mnt/dlpssa/refined")
except Exception as e:
    print("Refined container already mounted")
try:
    adls_mount(account_name="dlpssa4dev04", container="curated", mnt_pnt="/mnt/dlpssa/curated")
except Exception as e:
    print("Curated container already mounted")

# COMMAND ----------

def load_table(args):
    status = dbutils.notebook.run("nb_curated_table_load", 60, arguments=args)
    print(status)
    if status != 'success':
        raise Exception(f"Failed to load curated database. Status: {str(status)}")

table_list = [
    {"table": "area", "destination_table": "dim_area", "id_column": "area_code" },
    {"table": "series", "destination_table": "dim_series", "id_column": "series_id" },
    {"table": "item", "destination_table": "dim_item", "id_column": "item_code"}
#     {"table": "current", "destination_table": "fact_current", "id_column": "series_id,year,period" },

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
            print(e)
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact table
# MAGIC After all dimensions loaded, populate the fact table.

# COMMAND ----------

from delta.tables import *
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException

table = "current"
destination_table = "fact_current"

try:
    delta_target = DeltaTable.forName(spark, f"{curated_db}.{destination_table}")

    min_date = datetime(2022,1,1)
    last_updated = spark.sql(f"select max(last_modified) last_modified from {curated_db}.{destination_table}").first().last_modified
    last_updated = (last_updated or min_date) + timedelta(hours=0, minutes=0, seconds=1)

    fact_changes_df =spark.read.format("delta").option("readChangeFeed", "true") \
            .option("startingTimestamp", last_updated) \
            .load(f"{refined_base_path}/{table}") \
            .withColumnRenamed("_commit_timestamp", "last_modified") \
            .drop("_commit_version") \
            .drop("_change_type")

    fact_changes_df.createOrReplaceTempView('fact_current_changes')

    refined_df = spark.sql(f"""
        select
            a.dim_area_id,
            i.dim_item_id,
            s.dim_series_id,
            f.year,
            f.period,
            f.value,
            f.is_deleted,
            f.last_modified
        from fact_current_changes f
          left join {curated_db}.dim_series s
            on f.series_id = s.series_id
          left join {curated_db}.dim_area a
            on s.area_code = a.area_code
          left join {curated_db}.dim_item i
            on s.item_code = i.item_code
    """)

    fact_columns = ['dim_area_id', 'dim_item_id', 'dim_series_id', 'year', 'period', 'value', 'is_deleted', 'last_modified']

    update_dct = {f"{c}": f"s.{c}" for c in fact_columns } 
    id_str = "t.dim_series_id = s.dim_series_id and t.year = s.year and t.period = s.period"

    delta_target.alias('t').merge(refined_df.alias('s'), id_str) \
    .whenMatchedUpdate(set=update_dct) \
    .whenNotMatchedInsert(values=update_dct) \
    .execute()

except AnalysisException as e:
    if str(e).find('not a Delta table') > 0 or str(e).find('no viable alternative at input') > 0:
        print("Table does not exist, need to create table first.")
    elif str(e).find('after the latest version available') > 0:
        print(f"No new updates found for table {destination_table}")
