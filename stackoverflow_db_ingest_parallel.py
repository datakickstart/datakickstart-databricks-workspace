# Databricks notebook source
# On Databricks, need to add library for com.microsoft.sqlserver.jdbc.spark and set secrets
database = "StackOverflow2010"
db_host_name = "sandbox-2-sqlserver.database.windows.net"
db_url = f"jdbc:sqlserver://{db_host_name};databaseName={database}"
db_user = dbutils.secrets.get("demo", "sql-user-stackoverflow") # databricks
db_password = dbutils.secrets.get("demo", "sql-pwd-stackoverflow") #databricks

# COMMAND ----------

table_list = ["Badges", "Comments", "LinkTypes", "PostLinks", "Posts", "PostTypes", "Users", "Votes", "VoteTypes"]
spark.sql(f"CREATE DATABASE IF NOT EXISTS raw_stackoverflow LOCATION '/demo/raw_stackoverflow'")

def load_table(table):
    print(table)
    destination_table = "raw_stackoverflow." + table

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

# COMMAND ----------

# Reload each StackOverflow table sequentially (one after another)

## Uncomment to run
# for table in table_list:
#   load_table(table)

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()

worker_count = 2

def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        function(value)
        q.task_done()


print(table_list)

for table in table_list:
    q.put(table)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()

