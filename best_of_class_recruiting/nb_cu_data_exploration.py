# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

adls_authenticate()
raw_storage_base_path = dbutils.secrets.get("demo", "raw-datalake-path")
raw_format = "delta"


# COMMAND ----------

# Mount directory to use file system commands easily
try:
    adls_mount()
except Exception as e:
    if 'Directory already mounted' in str(e):
        print("Already mounted")
    else:
        raise e

# COMMAND ----------

# MAGIC %sh du -h -s /dbfs/mnt/datalake/raw/cu/*

# COMMAND ----------

# MAGIC %fs ls /mnt/datalake/raw/cu/area

# COMMAND ----------

# MAGIC %md
# MAGIC ## Area
# MAGIC 1. Business key = area_code
# MAGIC 2. Data looks clean - not nulls
# MAGIC 3. Bigints: display_level (0 or 1), sort_sequenct (1-58)

# COMMAND ----------

# Evaluate and explore area
area_df = spark.read.format(raw_format).load(raw_storage_base_path + "cu/area")
display(area_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Series
# MAGIC 1. Business key = series_id
# MAGIC 2. Relationship to area, item, base, periodicity?
# MAGIC 3. base_period text not consistent
# MAGIC 4. Columns in series_title text
# MAGIC 5. footnote_codes (double): all NaN
# MAGIC 6. begin_year (bigint): min = 1913, max = 2020
# MAGIC 7. end_year (bigint): min = 1986, max = 2022

# COMMAND ----------

# Evaluate and explore area
series_df = spark.read.format(raw_format).load(raw_storage_base_path + "cu/series")
display(series_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Item
# MAGIC 1. Business key = item_code
# MAGIC 2. Bigints = display_level, sort_sequence
# MAGIC 3. Selectable all equals T

# COMMAND ----------

# Evaluate and explore area
item_df = spark.read.format(raw_format).load(raw_storage_base_path + "cu/item")
display(item_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Base
# MAGIC 1. Business key = base_code
# MAGIC 2. 2 entries: Alternate and Current

# COMMAND ----------

# Evaluate and explore area
base_df = spark.read.format(raw_format).load(raw_storage_base_path + "cu/base")
display(base_df)

# COMMAND ----------

# Evaluate and explore period
period_df = spark.read.format("parquet").load(raw_storage_base_path + "cu/period")
display(period_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current
# MAGIC 1. Business key = series_id, year, period
# MAGIC 2. footnote_codes all NaN
# MAGIC 3. year (bigint): min 1997, max 2022
# MAGIC 4. value is double, no nulls, min 1.27, max 3196.23

# COMMAND ----------

# Evaluate and explore area
current_df = spark.read.format(raw_format).load(raw_storage_base_path + "cu/current")
display(current_df)
