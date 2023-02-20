df = spark.read.format("delta").load("dbfs:/mnt/datalake/raw/stackoverflow/posts")
df.show()

