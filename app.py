from flask import Flask, render_template, request, jsonify
from kafka import KafkaProducer
from emoji_dict import emoji_meanings
import json
import random
from datetime import datetime

app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    data = request.get_json()
    emoji = data['emoji']
    user_id = f"user_{random.randint(100, 999)}"
    timestamp = datetime.utcnow().isoformat()
    meaning = emoji_meanings.get(emoji, "Unknown")

    message = {
        "user_id": user_id,
        "emoji": emoji,
        "timestamp": timestamp,
        "meaning": meaning
    }

    producer.send("emoji_raw_stream", message)
    return jsonify({"status": "success", "message": message})

if __name__ == '__main__':
    app.run(debug=True,port=5001)
