# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC val writeMode = "append"
# MAGIC val database = "sandbox-2-sql"
# MAGIC val db_host_name = "sandbox-2-sqlserver.database.windows.net"
# MAGIC val url = s"jdbc:sqlserver://${db_host_name};databaseName=${database}"
# MAGIC val user = dbutils.secrets.get("demo", "sql-user-stackoverflow") // databricks
# MAGIC val password = dbutils.secrets.get("demo", "sql-pwd-stackoverflow") // databricks    
# MAGIC
# MAGIC val table = "dbo.video_view"
# MAGIC
# MAGIC val sqlOptions = Map(
# MAGIC       "url" -> url,
# MAGIC       "dbtable" -> table,
# MAGIC       "user"-> user,
# MAGIC       "password" -> password,
# MAGIC       // "reliabilityLevel" ->"BEST_EFFORT",
# MAGIC       // "tableLock" ->"false",
# MAGIC       // "batchsize" -> "100000"
# MAGIC     )
# MAGIC
# MAGIC val df = spark.read
# MAGIC     .format("com.microsoft.sqlserver.jdbc.spark")
# MAGIC     .options(sqlOptions)
# MAGIC     .load()
# MAGIC
# MAGIC display(df)
