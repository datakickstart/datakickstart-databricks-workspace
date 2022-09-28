# Databricks notebook source
from utils.logging_utils import *

job_name = 'stackoverflow_db_ingest'
logger = start_logging(spark, job_name)

# COMMAND ----------

# On Databricks, need to add library for com.microsoft.sqlserver.jdbc.spark and set secrets
database = "StackOverflow2010"
db_host_name = "sandbox-2-sqlserver.database.windows.net"
db_url = f"jdbc:sqlserver://{db_host_name};databaseName={database}"
db_user = dbutils.secrets.get("demo", "sql-user-stackoverflow") # databricks
db_password = dbutils.secrets.get("demo", "sql-pwd-stackoverflow") #databricks

raw_path = 'dbfs:/mnt/datalake/raw/stackoverflow_sql/'
raw_db = 'raw_stackoverflow_sql'

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {raw_db} LOCATION '{raw_path}'")

def load_table(table):
    print(table)
    destination_table = f"{raw_db}.{table}"

    df = (
        spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", db_url)
        .option("dbtable", table)
        .option("user", db_user)
        .option("password", db_password)
        .load()
    )

    df.write.format("parquet").mode("overwrite").saveAsTable(destination_table)
    log_informational_message(f"Saved raw table {destination_table}")

# COMMAND ----------

# Reload each StackOverflow table sequentially (one after another)

## Uncomment to run
# for table in table_list:
#   load_table(table)

# COMMAND ----------

def get_table_list(get_tables_sql):
    tables_df = (
        spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", db_url)
        .option("query", get_tables_sql)
        .option("user", db_user)
        .option("password", db_password)
        .load()
    )
    return [row.table_name for row in tables_df.collect()]


get_tables_sql = "Select table_name from INFORMATION_SCHEMA.TABLES where table_schema='dbo' and table_type='BASE TABLE'"
try:
    table_list = get_table_list(get_tables_sql)
except:
    time.sleep(10)
    table_list = get_table_list(get_tables_sql)

## Uncommment to override table list
#table_list = ["Badges", "Comments", "LinkTypes", "PostLinks", "Posts", "PostTypes", "Users", "Votes", "VoteTypes"]
    
print(table_list)

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()

worker_count = 3

def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        function(value)
        q.task_done()

for table in table_list:
    q.put(table)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()


# COMMAND ----------

stop_logging(job_name)
