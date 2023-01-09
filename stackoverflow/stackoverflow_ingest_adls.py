# Databricks notebook source
from utils.logging_utils import *

job_name = 'stackoverflow_ingest_adls'
logger = start_logging(spark, job_name)

# COMMAND ----------

partitioned_tables = {"posts": "CreationMonth"}

# COMMAND ----------

def get_tables(path):
    """Return list of items for file path and folder/table.

    Args:
        base_path (str): Azure path to get data directories (must be authenticated), 
            formatted abfss://<container>@<storage_account>.dfs.core.windows.net/[root_directory]

    Returns:
        list[(path, table_name)]: List of tuples, each item has full path in ADLS and the folder name
                                   (usually used as table name)
    """
    table_folders = dbutils.fs.ls(path)
    return [(folder.path, folder.name[:-1]) for folder in table_folders if folder.name[-1] == '/']

def save_delta_table(df, table, partition_str=None):
    """Save Spark table using format delta.
    
    Args:
        df (DataFrame): Data to save as Spark table.
        table (str): Name of destination table
        partition_str (str, optional): Column names used to partition folders as comma delimited string, 
                                        for example 'colA,colB'. No partitions created if not provided.
        format (str, optional): Defaults to 'delta'

    Returns:
        None
    """
    options = {"delta.enableChangeDataFeed": "true"}
    if partition_str:
        df.write.mode("overwrite").options(**options).format("delta").saveAsTable(table)
    else:
        df.write.mode("overwrite").options(**options).format("delta").saveAsTable(table)


# COMMAND ----------

source_path = 'dbfs:/mnt/dvtraining/demo/stackoverflow/'
raw_path = 'dbfs:/mnt/datalake/raw/stackoverflow/'
raw_db = 'raw_stackoverflow'

spark.sql(f"CREATE DATABASE IF NOT EXISTS {raw_db} LOCATION '{raw_path}'")

table_list = get_tables(source_path)
print(table_list)

for (path, t) in table_list:
    try:
        df = spark.read.format("parquet").load(path)
        if t in partitioned_tables:
            save_delta_table(df, f"{raw_db}.{t}_delta", partitioned_tables[t])
            log_informational_message(f"Saved partitioned raw table {t}")
        else:
            save_delta_table(df, f"{raw_db}.{t}_delta")
            log_informational_message(f"Saved raw table {t}")
    except Exception as e:
        log_error_message(f"Error for table {t}: {str(e)}")
        raise e

# COMMAND ----------

stop_logging(job_name)
