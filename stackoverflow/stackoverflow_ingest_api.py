# Databricks notebook source
import json
import sys
import time
import requests

endpoint = "https://api.stackexchange.com/2.2/questions"

def get_questions(endpoint, fromdate, todate):
    tags = "pyspark" # post must match all tags - "pyspark;apache-spark-sql
    query_string = f"?fromdate={str(fromdate)}&todate={str(todate)}&order=desc&min=1&sort=votes&tagged={tags}&filter=default&site=stackoverflow&run=true" #tagged=python&site=stackoverflow"
    r = requests.get(f"{endpoint}{query_string}")
    if r.status_code == 200:
        return r.text
    else:
        return r
    

if __name__ == "__main__":
    todate = int(time.time()) # - 18000  # test value: "1619178204"
    fromdate = todate - 300000 #100000  # test value: "1617108204"

    response = get_questions(endpoint, fromdate, todate)
    response_json = json.loads(response)
    question_list = response_json["items"]
    print(fromdate, todate)
#     log.debug(f"fromdate={fromdate}, todate={todate}")
#     log.debug(question_list)

    # Remove items and print response metadata
#     del response_json["items"]
#     metadata = response_json
#     log.info(f"API metadata: {metadata}")
    print(response_json)


# COMMAND ----------

print(type(response_json["items"]))

# COMMAND ----------

df = spark.createDataFrame(response_json["items"])
display(df)

# COMMAND ----------

#users_df = spark.read.table("raw_stackoverflow.users")
#combined_df = df.join(users_df, df.owner.user_id == users_df.Id, how="left")
#display(combined_df)
def get_user(row):
#     print(row.owner.user_id)
#     return row
    id = row['owner']['user_id']
    print(id)
#     print(id)
#     return 'User'

df.foreach(lambda x: get_user(x))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(Id) FROM raw_stackoverflow.posts -- LIMIT 30

# COMMAND ----------


