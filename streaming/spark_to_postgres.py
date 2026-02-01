from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType


# -----------------------------
# 1. Spark Session with Drivers
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaToPostgresStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Streaming Started")


# -----------------------------
# 2. Define Schema
# -----------------------------
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", FloatType()),
    StructField("high", FloatType()),
    StructField("low", FloatType()),
    StructField("open", FloatType()),
    StructField("previous_close", FloatType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType())
])


# -----------------------------
# 3. Read from Kafka Topic
# -----------------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()


# -----------------------------
# 4. Parse JSON Messages
# -----------------------------
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp properly
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

print("✅ Kafka Messages Parsed")


# -----------------------------
# 5. Postgres Connection Config
# -----------------------------
jdbc_url = "jdbc:postgresql://postgres:5432/stock_db"

properties = {
    "user": "stock_user",
    "password": "stock_pass",
    "driver": "org.postgresql.Driver"
}


# -----------------------------
# 6. Write Each Microbatch into Postgres
# -----------------------------
def write_to_postgres(batch_df, batch_id):
    print(f"🚀 Writing batch {batch_id} to Postgres...")

    batch_df.write.jdbc(
        url=jdbc_url,
        table="stock_prices",
        mode="append",
        properties=properties
    )


query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
