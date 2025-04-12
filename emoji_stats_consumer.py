from kafka import KafkaConsumer
import json
import psycopg2

print("🚀 Starting Emoji Stats Consumer...")

# Step 1: Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="emoji",
        user="shiva",
        password="0819",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    print("🟢 Connected to PostgreSQL")
except Exception as e:
    print("❌ Failed to connect to PostgreSQL:", e)
    exit(1)

# Step 2: Connect to Kafka topic
try:
    consumer = KafkaConsumer(
        'emoji_batch_results',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    print("🟢 Subscribed to Kafka topic: emoji_batch_results")
except Exception as e:
    print("❌ Failed to subscribe to Kafka:", e)
    exit(1)

# Step 3: Consume messages from Kafka and insert into PostgreSQL
print("📥 Waiting for messages...\n")

for message in consumer:
    try:
        record = message.value
        print("📦 Received message:", record)

        emoji = record.get('emoji')
        count = record.get('count')
        time_window = record.get('time_window')  # Optional, if included

        if emoji and count is not None:
            cursor.execute("""
                INSERT INTO emoji_usage_stats (emoji, count, time_window)
                VALUES (%s, %s, %s)
            """, (emoji, count, time_window))
            conn.commit()
            print(f"✅ Inserted → Emoji: {emoji}, Count: {count}, Window: {time_window}")
        else:
            print("⚠️ Incomplete data, skipping:", record)

    except Exception as e:
        print("❌ Error while processing message:", e)

