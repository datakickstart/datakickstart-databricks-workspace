# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/datalake/raw/stackoverflow/badges/

# COMMAND ----------

partitioned_tables = {"posts": "CreationMonth"}

# COMMAND ----------

def get_tables(path):
    table_folders = dbutils.fs.ls(path)
    return [(folder.path, folder.name[:-1]) for folder in table_folders if folder.name[-1] == '/']

def save_delta_table(df, table, partition_str=None):
    options = {"delta.enableChangeDataFeed": "true"}
    if partition_str:
        df.write.mode("overwrite").options(**options).format("delta").saveAsTable(table)
    else:
        df.write.mode("overwrite").options(**options).format("delta").saveAsTable(table)


# COMMAND ----------

source_path = 'dbfs:/mnt/datalake/raw/stackoverflow/'
source_db = 'raw_stackoverflow'
table_list = get_tables(source_path)

for (path, t) in table_list:
  try:
    df = spark.read.parquet(path)
    if t in partitioned_tables:
        save_delta_table(df, "raw_stackoverflow." + t + "_delta", partitioned_tables[t])
        print(t) #temporary
    else:
        save_delta_table(df, "raw_stackoverflow." + t + "_delta")
  except Exception as e:
    print(f"Error for table {t}: {str(e)}")

# COMMAND ----------


