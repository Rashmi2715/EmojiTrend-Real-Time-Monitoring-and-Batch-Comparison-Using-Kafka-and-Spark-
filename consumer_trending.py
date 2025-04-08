from kafka import KafkaConsumer
import json
import psycopg2
import sys

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="emoji_stream_db",
        user="rashmi",
        password="2715",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL database.")
except Exception as e:
    print("❌ Failed to connect to PostgreSQL.")
    print(f"Error: {e}")
    sys.exit(1)

# Connect to Kafka
try:
    consumer = KafkaConsumer(
        "emoji_trending",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("📥 Listening to Kafka topic: emoji_trending")
except Exception as e:
    print("❌ Failed to connect to Kafka.")
    print(f"Error: {e}")
    sys.exit(1)

# Process messages
try:
    for message in consumer:
        try:
            value = message.value
            print(f"🔥 Trending Emoji: {value['emoji']} | Count: {value['count']}")

            # Insert into PostgreSQL
            cursor.execute("""
                INSERT INTO emoji_trending (emoji, count)
                VALUES (%s, %s)
            """, (value['emoji'], value['count']))
            conn.commit()

        except KeyError as e:
            print(f"⚠️ Missing expected field in message: {e}")
        except Exception as db_error:
            print(f"❌ Database error: {db_error}")
            conn.rollback()

except KeyboardInterrupt:
    print("\n🛑 Gracefully stopping...")

except Exception as e:
    print("❌ Unexpected error occurred.")
    print(f"Error: {e}")

finally:
    # Cleanup
    cursor.close()
    conn.close()
    consumer.close()
    print("🔌 Closed all connections.")
