-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE video_view_json_bronze 
PARTITIONED BY (event_date)
TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user")
AS
SELECT *, cast(eventTimestamp as date) event_date
FROM cloud_files("${my_etl.source_base_path}/video_usage/video_view_json_cdc", "json", map("cloudFiles.useNotifications", "false"))

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE video_view_csv_bronze 
-- PARTITIONED BY (event_date)
-- TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
--   delta.enableChangeDataFeed=true)
-- AS
-- SELECT * FROM cloud_files("abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_view_cdc_csv", "csv", map("header", "true", "cloudFiles.maxFilesPerTrigger", "2", "cloudFiles.useNotifications", "false"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE video_view_silver
PARTITIONED BY (event_date)
TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
  delta.enableChangeDataFeed=true)
AS
SELECT *
FROM STREAM(LIVE.video_view_json_bronze)

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE video_view_scd1_silver
-- PARTITIONED BY (event_date)
-- TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
--   delta.enableChangeDataFeed=true);

-- APPLY CHANGES INTO live.video_view_scd1_silver
-- FROM STREAM(LIVE.video_view_silver)
-- KEYS(usageId)
-- SEQUENCE BY offset
-- COLUMNS * EXCEPT
--   (key)
-- STORED AS
--   SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE member_json_bronze 
TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user")
AS
SELECT *, cast(eventTimestamp as date) event_date
FROM cloud_files("${my_etl.source_base_path}/video_usage/member_json_cdc", "json", map("cloudFiles.useNotifications", "false"))

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE member_silver
-- TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
--   delta.enableChangeDataFeed=true)
-- AS
-- SELECT *
-- FROM STREAM(LIVE.member_json_bronze)

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW member_silver AS
SELECT *
FROM  STREAM(LIVE.member_json_bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE member_scd1_silver
TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
  delta.enableChangeDataFeed=true);

APPLY CHANGES INTO live.member_scd1_silver
FROM STREAM(LIVE.member_silver)
KEYS(membershipId)
SEQUENCE BY timestamp
COLUMNS * EXCEPT
  (key)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE video_agg_gold
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "event_date")
AS
SELECT
  event_date,
  user,
  count(1) as RecordCount,
  sum(durationSeconds) as total_duration
FROM LIVE.video_view_silver
GROUP BY event_date, user

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE member_totals_gold
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "event_date")
AS
SELECT membershipId, cast(sv.eventTimestamp as date) view_date, count(1) as view_count, sum(durationSeconds) as total_duration
FROM LIVE.member_scd1_silver sm
JOIN LIVE.video_view_silver sv
    ON sm.user = sv.user
        and sv.eventTimestamp between sm.startDate and sm.endDate
GROUP BY membershipId, cast(sv.eventTimestamp as date)      

-- COMMAND ----------

-- Delta
CREATE OR REFRESH STREAMING TABLE deltasource_video_view_silver
PARTITIONED BY (event_date)
TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
  delta.enableChangeDataFeed=true)
AS
SELECT *, cast(eventTimestamp as date) event_date
FROM STREAM(delta.`abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_view_cdc`)

-- COMMAND ----------

-- Delta
CREATE OR REFRESH STREAMING TABLE deltasource_video_completed_silver
PARTITIONED BY (event_date)
TBLPROPERTIES(pipelines.autoOptimize.zOrderCols = "user",
  delta.enableChangeDataFeed=true)
AS
SELECT *
FROM STREAM(LIVE.deltasource_video_view_silver)
WHERE completed = True
