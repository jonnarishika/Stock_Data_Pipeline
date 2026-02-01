from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType


# --------------------------------------------------------
#  1. Start Spark Session
# --------------------------------------------------------
spark = SparkSession.builder \
    .appName("KafkaToPostgresLive") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------------
#  2. Kafka Topic Config
# --------------------------------------------------------
KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP = "kafka:9092"


# --------------------------------------------------------
#  3. Postgres Config
# --------------------------------------------------------
POSTGRES_URL = "jdbc:postgresql://warehouse_postgres:5432/stock_db"
POSTGRES_TABLE = "stock_prices"

POSTGRES_PROPERTIES = {
    "user": "stock_user",
    "password": "stock_pass",
    "driver": "org.postgresql.Driver"
}


# --------------------------------------------------------
#  4. Define Schema (matches producer JSON)
# --------------------------------------------------------
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("open", FloatType()) \
    .add("previous_close", FloatType()) \
    .add("timestamp", StringType()) \
    .add("source", StringType())


# --------------------------------------------------------
#  5. Read Live Stream From Kafka
# --------------------------------------------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()


# Kafka value comes as binary → cast to string
json_df = kafka_df.selectExpr("CAST(value AS STRING)")


# --------------------------------------------------------
#  6. Parse JSON Into Columns
# --------------------------------------------------------
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")


# --------------------------------------------------------
#  7. FIX Timestamp Conversion (MOST IMPORTANT)
# --------------------------------------------------------
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)


# --------------------------------------------------------
#  8. Write Each Batch Into Postgres
# --------------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    print(f"🔥 Writing batch {batch_id} to Postgres...")

    batch_df.write.jdbc(
        url=POSTGRES_URL,
        table=POSTGRES_TABLE,
        mode="append",
        properties=POSTGRES_PROPERTIES
    )


# --------------------------------------------------------
# 9. Start Streaming Query
# --------------------------------------------------------
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()


print(" Kafka → Spark → Postgres Live Pipeline Started!")
query.awaitTermination()
