df = spark.read.format("delta").load("dbfs:/mnt/datalake/raw/stackoverflow/comments_delta")
df.show()
