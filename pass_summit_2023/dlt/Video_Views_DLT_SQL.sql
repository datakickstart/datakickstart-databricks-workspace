-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE video_views AS
SELECT * FROM read_files("abfss://sources@datakickstartadls2.dfs.core.windows.net/video_usage/video_views_json");
