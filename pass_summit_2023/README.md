# Incremental Data Loading with Azure Databricks

## Setup and Producer notebooks
These notebooks are meant to setup the demo and are not meant to demonstrate the best way to read and write data between the services.

1. Run `video_plus_member_producer` to generate records to Kafka or Event Hubs for two topics - usage and membership.
1. Run `structured_streaming_video_views_multicast` to write data to Azure SQL and JSON so it can be used by the examples.
1. Run ADF data flow to do CDC from Azure SQL table to Parquet files that autoloader can ingest.

## Structured Streaming
Use notebook 

## Autoloader

## Delta Live Tables