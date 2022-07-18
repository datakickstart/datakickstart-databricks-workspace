# Databricks notebook source
# MAGIC %sh du -h -s /dbfs/mnt/datalake/raw/cu/*

# COMMAND ----------

# MAGIC %run ../utils/mount_storage

# COMMAND ----------

adls_authenticate()
raw_storage_base_path = dbutils.secrets.get("demo", "raw-datalake-path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Area
# MAGIC 1. Business key = area_code
# MAGIC 2. Data looks clean - not nulls
# MAGIC 3. Bigints: display_level (0 or 1), sort_sequenct (1-58)

# COMMAND ----------

# Evaluate and explore area
area_df = spark.read.parquet(raw_storage_base_path + "cu/area")
display(area_df)


# COMMAND ----------

# Evaluate and explore area
series_df = spark.read.parquet(raw_storage_base_path + "cu/series")
display(series_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Series
# MAGIC 1. Business key = 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW raw_cu.vw_current_full
# MAGIC AS
# MAGIC SELECT
# MAGIC     c.year,
# MAGIC     c.period,
# MAGIC     s.*, 
# MAGIC     item.item_name,
# MAGIC     area.area_name,
# MAGIC     c.value
# MAGIC FROM `current` c
# MAGIC   JOIN series s
# MAGIC     ON c.series_id = s.series_id
# MAGIC   JOIN item
# MAGIC     ON s.item_code = item.item_code
# MAGIC   JOIN area
# MAGIC     ON s.area_code = area.area_code
