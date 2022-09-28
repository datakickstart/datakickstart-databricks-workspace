# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

adls_authenticate()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "cu"
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path")

# COMMAND ----------


#Area table
area_raw_df = spark.read.parquet(raw_base_path + "/area")
area_refined = area_raw_df.select("area_code", "area_name", "display_level")
# TODO: Add surrogate key
area_refined.write.format("delta").save(refined_base_path + "dim_area")

# COMMAND ----------

# Series table
area_raw_df = spark.read.parquet(raw_base_path + "/area")
area_refined = area_raw_df.select("area_code", "area_name", "display_level")
# TODO: Add surrogate key
area_refined.write.format("delta").save(refined_base_path + "dim_area")

# COMMAND ----------

# Fact table
