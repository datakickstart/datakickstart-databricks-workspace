# Databricks notebook source
from pyspark.sql.functions import col, lit

so_posts = spark.read.parquet("dbfs:/mnt/dvtraining/demo/stackoverflow/posts/CreationMonth=2022-06-01")

# COMMAND ----------

so_posts = so_posts.sort(col("_Id").desc())
display(so_posts)


# COMMAND ----------

topic = "stackoverflow_post"
GROUP_ID = "so_v6"

def get_confluent_config(topic):
    bootstrapServers = dbutils.secrets.get("demo", "confluent-cloud-brokers")
    confluentApiKey =  dbutils.secrets.get("demo", "confluent-cloud-user")
    confluentSecret =  dbutils.secrets.get("demo", "confluent-cloud-password")
    confluentTopicName = "stackoverflow_post"

    options = {
        "kafka.bootstrap.servers": bootstrapServers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.ssl.endpoint.identification.algorithm": "https",
        "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret),
        "kafka.sasl.mechanism": "PLAIN",
        "topic": confluentTopicName
    }
    return options

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
      'topic': topic
    }
    return config

# COMMAND ----------

topic = 'stackoverflow-post'
config = get_confluent_config(topic)

min_id = None

for i in range(100):
    if min_id:
        print(min_id)
        df = so_posts.where((col("_Id")< lit(min_id))).limit(10)
        df.show()
    else:
        df = so_posts.limit(10)
    min_id = df.selectExpr("min(_Id) as minId").first().minId
    df.selectExpr("cast(_Id as String) as key", "to_json(struct(*)) as value").write.format("kafka").options(**config).save()


# COMMAND ----------


