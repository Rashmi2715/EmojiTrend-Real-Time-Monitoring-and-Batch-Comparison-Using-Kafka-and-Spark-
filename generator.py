# generator.py
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
from emoji_dict import emoji_meanings

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

emojis = list(emoji_meanings.keys())
user_ids = [f'user_{i}' for i in range(1, 1000)]  # Simulated users

print("🚀 Emoji generator running...")

try:
    while True:
        for _ in range(200):  # 200 emoji events per second
            emoji = random.choice(emojis)
            meaning = emoji_meanings.get(emoji, "Unknown")
            data = {
                "user_id": random.choice(user_ids),
                "emoji": emoji,
                "timestamp": datetime.utcnow().isoformat(),
                "meaning": meaning
            }
            producer.send("emoji_raw_stream", value=data)
        time.sleep(1)
except KeyboardInterrupt:
    print("🛑 Generator stopped.")
