'''from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, row_number, desc, from_json , to_json,struct
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.window import Window as W

# Start Spark session
spark = SparkSession.builder \
    .appName("EmojiTrendingStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.kafka:kafka-clients:3.5.1,"
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming JSON messages
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji", StringType()) \
    .add("timestamp", StringType()) \
    .add("meaning", StringType())

# Read streaming data from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_raw_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON messages and extract fields
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.withColumn("data", from_json("json_str", schema)) \
            .filter(col("data").isNotNull())

emoji_df = parsed_df.select(
    col("data.emoji").alias("emoji"),
    col("data.timestamp").cast(TimestampType()).alias("timestamp")
)

# Count emojis per 2-second window
emoji_counts = emoji_df.groupBy(
    window(col("timestamp"), "2 seconds"),
    col("emoji")
).count()

# Define processing logic using foreachBatch
def process_top_emoji(batch_df, epoch_id):
    if batch_df.isEmpty():
        return
    if not batch_df.isEmpty():
        print(f"⚙️ Processing Batch {epoch_id} — Rows: {batch_df.count()}")
        # Rank emojis by count per window
        window_spec = W.partitionBy("window").orderBy(desc("count"))
        ranked_df = batch_df.withColumn("rank", row_number().over(window_spec)) \
                            .filter(col("rank") == 1) \
                            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("emoji"),
                col("count")
            ) \
            .withColumn("value", to_json(struct("window_start", "window_end", "emoji", "count"))) \
            .selectExpr("CAST(value AS STRING) as value")
        # Write top emoji to Kafka topic
        ranked_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "emoji_trending") \
            .save()

# Apply foreachBatch to write the top emoji to another Kafka topic
query = emoji_counts.writeStream \
    .foreachBatch(process_top_emoji) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/emoji_streaming_checkpoint") \
    .start()

query.awaitTermination()
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, row_number, desc
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.window import Window as W

# Start Spark session
spark = SparkSession.builder \
    .appName("GlobalEmojiTrendingStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.kafka:kafka-clients:3.5.1,"
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema of the incoming JSON
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji", StringType()) \
    .add("timestamp", StringType()) \
    .add("meaning", StringType())

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_raw_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and extract emoji field
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.withColumn("data", from_json("json_str", schema)).filter(col("data").isNotNull())
emoji_df = parsed_df.select(col("data.emoji").alias("emoji"))

# Maintain global counts
emoji_counts = emoji_df.groupBy("emoji").count()

# Process batch to get top emoji
def write_top_emoji(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    print(f"⚙️ Epoch {epoch_id} — Total emojis seen: {batch_df.count()}")

    # Rank and filter top emoji
    window_spec = W.orderBy(desc("count"))
    top_df = batch_df.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select("emoji", "count") \
        .withColumn("value", to_json(struct("emoji", "count"))) \
        .selectExpr("CAST(value AS STRING) as value")

    # Write to Kafka
    top_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "emoji_trending") \
        .save()

# Start the query with a 5-second trigger
query = emoji_counts.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(write_top_emoji) \
    .option("checkpointLocation", "/tmp/global_emoji_trending_checkpoint") \
    .start()

query.awaitTermination()
