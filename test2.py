df = spark.read.format("delta").load("dbfs:/mnt/datalake/raw/stackoverflow/posts_delta")
df.show()
