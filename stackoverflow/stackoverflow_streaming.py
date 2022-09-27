# Databricks notebook source
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType

from utils.logging_utils import *

job_name = 'stackoverflow_streaming'
logger = start_logging(spark, job_name)

topic = "stackoverflow-post"
GROUP_ID = "so_v1"

def get_event_hub_config(topic):
    # Password is really a Event Hub connection string, for example ->
    # Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=ReadWriteTmp;SharedAccessKey=vhNXxXXXXXxxxXXXXXXXxx=;EntityPath=demo-message-1
    password = dbutils.secrets.get(scope = "demo", key = "eh-sasl-{0}".format(topic))

    EH_SASL = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{0}";'.format(password)

    config = {
      'kafka.bootstrap.servers': 'dustin-demo-eh.servicebus.windows.net:9093',
      'kafka.security.protocol': 'SASL_SSL',
      'kafka.sasl.mechanism': 'PLAIN',
      'kafka.group.id': GROUP_ID,
      'kafka.request.timeout.ms': "60000",
      'kafka.session.timeout.ms': "20000",
      'kafka.heartbeat.interval.ms': "10000",
      'kafka.sasl.jaas.config': EH_SASL,
      'subscribe': topic
    }
    return config



# COMMAND ----------

post_schema = (
  StructType()
    .add('_Id','long')
    .add('_ParentId','long')
    .add('_PostTypeId','long')
    .add('_Score','long')
    .add('_Tags','string')
    .add('_Title','string')
    .add('_ViewCount','long')  
    .add('_LastActivityDate','timestamp')
    .add('_LastEditDate','timestamp')
    .add('_LastEditorDisplayName','string')
    .add('_LastEditorUserId','long')
    .add('_OwnerDisplayName','string')
    .add('_OwnerUserId','long')
    .add('_AcceptedAnswerId','long')
    .add('_AnswerCount','long')
    .add('_Body','string')
    .add('_ClosedDate','timestamp')
    .add('_CommentCount','long')
    .add('_CommunityOwnedDate','timestamp')
    .add('_ContentLicense','string')
    .add('_CreationDate','timestamp')
    .add('_FavoriteCount','long')
)

# COMMAND ----------

##Create Spark Readstream
config = get_event_hub_config(topic)

post_df = (
  spark
  .readStream
  .format("kafka")
  .options(**config)
  .load()
)

post_transformed_df = (post_df
.withColumn('key', fn.col("key").cast(StringType()))
  .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'value')
)


# COMMAND ----------

df_parsed = post_transformed_df.select(
    col('value').cast('string').alias("value_str"),
    fn.from_json(col('value').cast('string'), post_schema).alias("json"))

df_post = df_parsed.selectExpr("json.*")


# COMMAND ----------

new_users = spark.read.format("delta").load("dbfs:/mnt/datakickstart/refined/stackoverflow_new_users").select(
    "user_id",
    "account_id",
    "display_name",
    "website_url"
)

old_users = spark.read.parquet("dbfs:/mnt/datalake/raw/stackoverflow/users").selectExpr(
    "_ID as user_id",
    "_AccountId as account_id",
    "_DisplayName as display_name",
    "_WebsiteUrl as website_url"
    )

all_users = old_users.union(new_users)

# COMMAND ----------

df_combined = df_post.join(all_users, df_post["_OwnerUserId"] == all_users["user_id"], how="left")

ckpt_path = f"dbfs:/mnt/datalake/raw/checkpoints/stackoverflow_streaming_{GROUP_ID}"
dest_path = f"dbfs:/mnt/datalake/raw/stack_overflow_streaming/posts_{GROUP_ID}"

log_informational_message("Starting stream for combined so posts to delta file.")

df_combined.writeStream.format("delta").option("checkpointLocation",ckpt_path) \
    .trigger(processingTime='30 seconds').outputMode("append") \
    .start(dest_path)

# COMMAND ----------

# q.processAllAvailable()
# q.stop()
stop_logging(job_name)
