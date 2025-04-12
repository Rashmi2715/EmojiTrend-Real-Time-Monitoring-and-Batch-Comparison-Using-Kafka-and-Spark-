# consumer.py
from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
import traceback

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'emoji_raw_stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='emoji_group'
)

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="emoji",
        user="shiva",
        password="0819",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    print("✅ Connected to PostgreSQL database.")
except OperationalError as e:
    print("❌ Database connection failed:")
    traceback.print_exc()
    exit()

print("🚀 Kafka consumer started. Waiting for emoji events...")

# Consume messages
try:
    for msg in consumer:
        try:
            data = msg.value
            user_id = data['user_id']
            emoji = data['emoji']
            timestamp = data['timestamp']
            meaning = data.get('meaning','Unknown')

            # Insert into PostgreSQL
            cur.execute("""
                INSERT INTO emoji_reactions (user_id, emoji, timestamp, meaning)
                VALUES (%s, %s, %s, %s)
            """, (user_id, emoji, timestamp, meaning))
            conn.commit()

            print(f"✅ Inserted: {data}")

        except Exception as e:
            print("❌ Error inserting emoji into DB:")
            traceback.print_exc()

except KeyboardInterrupt:
    print("🛑 Consumer stopped by user.")

finally:
    cur.close()
    conn.close()
    print("🔒 Database connection closed.")
