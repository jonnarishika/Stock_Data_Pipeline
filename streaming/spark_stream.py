from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("StockStreamingAnalytics") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()



spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock_prices") \
    .load()

# Parse JSON
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp
parsed = parsed.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Rolling average (5 min window)
metrics = parsed \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price")
    )


# Output to console (for now)

query = metrics.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/home/jovyan/output/processed") \
    .option("checkpointLocation", "/home/jovyan/output/checkpoints") \
    .start()


query.awaitTermination()
