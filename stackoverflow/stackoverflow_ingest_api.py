# Databricks notebook source
import json
import sys
import time
import requests

from utils.logging_utils import *

job_name = 'stackoverflow_ingest_api'
logger = start_logging(spark, job_name)

base_url = "https://api.stackexchange.com/2.2"
question_endpoint = base_url + "/questions"
users_endpoint = base_url + "/users"

# COMMAND ----------

def get_questions(endpoint, fromdate, todate):
    tags = "pyspark" # post must match all tags, consider separate calls for each of these: pyspark; apache-spark-sql; apache-spark
    query_string = f"?fromdate={str(fromdate)}&todate={str(todate)}&order=desc&min=1&sort=votes&tagged={tags}&filter=default&site=stackoverflow&run=true" #tagged=python&site=stackoverflow"
    r = requests.get(f"{endpoint}{query_string}")
    if r.status_code == 200:
        return r.text
    else:
        return r
    
def get_users(endpoint, id_list):
    ids_string = ';'.join(list(id_list))
    logger.debug("User ids:" + ids_string)
    ids = "/" + ids_string
    query_string = "?site=stackoverflow"
    r = requests.get(f"{endpoint}{ids}{query_string}")
    if r.status_code == 200:
        return r.text
    else:
        return r

# COMMAND ----------

# Get Questions
todate = int(time.time()) # - 18000  # test value: "1619178204"
fromdate = todate - 300000 #100000  # test value: "1617108204"

response = get_questions(question_endpoint, fromdate, todate)
response_json = json.loads(response)
question_list = response_json["items"]

logger.debug(f"fromdate={fromdate}, todate={todate}")
logger.debug(question_list)

# Remove items and print response metadata
del response_json["items"]
metadata = response_json
logger.info(f"API metadata: {metadata}")

# COMMAND ----------

print("Question count:", len(question_list))
print(question_list[0])

# COMMAND ----------

question_df = spark.createDataFrame(question_list)
display(question_df)

# COMMAND ----------

# Build unique set of users
question_rows = question_df.collect()
user_ids = set()
for row in question_rows:
    user_ids.add(str(row['owner']['user_id']))

print(user_ids)

# COMMAND ----------

# Get Users
users_response = get_users(users_endpoint, user_ids)
users_json = json.loads(users_response)
users_list = users_json["items"]

print(users_list[0])

# COMMAND ----------

# Create Users DataFrame
user_df = spark.createDataFrame(users_list).withColumnRenamed("creation_date", "user_creation_date").withColumnRenamed("link", "user_link")
user_df.write.mode("append").format("delta").option("mergeSchema", "true").save("dbfs:/mnt/datakickstart/refined/stackoverflow_new_users")
display(user_df)

# COMMAND ----------

#Join
combined_df = question_df.join(user_df, question_df["owner.user_id"] == user_df["user_id"], how="left")
combined_df.write.mode("append").format("delta").option("mergeSchema", "true").save("dbfs:/mnt/datakickstart/refined/questions_with_user")
display(combined_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/datakickstart/refined/questions_with_user` LIMIT 30
