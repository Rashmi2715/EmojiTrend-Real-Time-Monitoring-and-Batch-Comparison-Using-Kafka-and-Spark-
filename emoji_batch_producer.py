from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EmojiBatchFromDB") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.2,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/emoji"
db_properties = {
    "user": "shiva",
    "password": "0819",
    "driver": "org.postgresql.Driver"
}

# Read from PostgreSQL table
emoji_df = spark.read \
    .jdbc(url=jdbc_url, table="emoji_reactions", properties=db_properties) \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Perform 30-second window aggregation
aggregated_df = emoji_df \
    .groupBy(
        window(col("timestamp"), "1 Hour"),
        col("emoji")
    ).count()

# Prepare data to send to Kafka
output_df = aggregated_df.select(
    col("emoji"),
    col("count"),
    col("window.start").alias("time_window")
).withColumn("value", to_json(struct("emoji", "count", "time_window"))) \
 .selectExpr("CAST(value AS STRING) AS value")

# Write to Kafka
output_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "emoji_batch_results") \
    .save()

print("✅ Emoji batch processed from PostgreSQL and pushed to Kafka.")

