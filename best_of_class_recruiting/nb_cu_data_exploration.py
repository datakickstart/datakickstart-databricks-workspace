# Databricks notebook source
# MAGIC %sh du -h -s /dbfs/mnt/datalake/raw/cu/*

# COMMAND ----------

from utils import mount_storage
mount_storage.adls_authenticate()



# COMMAND ----------

raw_storage_base_path = dbutils.secrets.get("demo", "raw-datalake-path")

# Evaluate and explore area
area_df = spark.read.parquet(raw_storage_base_path + "cu/area")
display(area)

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
