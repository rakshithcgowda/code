import webbrowser
from threading import Timer
from flask import Flask, render_template, jsonify
from confluent_kafka import Consumer, KafkaException
import json

app = Flask(__name__)

# Kafka consumer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your broker details
    'group.id': 'forex-data-consumer',
    'auto.offset.reset': 'earliest'
}

print("Initializing Kafka consumer...")
consumer = Consumer(kafka_conf)
consumer.subscribe(['forex-data'])
print("Subscribed to Kafka topic: forex-data")

# Function to fetch latest Kafka messages
def fetch_latest_kafka_data():
    messages = {}
    print("Fetching data from Kafka...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("No new messages from Kafka.")
                break
            if msg.error():
                raise KafkaException(msg.error())
            data = json.loads(msg.value().decode('utf-8'))
            print("Received Kafka message:", data)
            messages[data['symbol']] = data
    except Exception as e:
        print("Error fetching Kafka messages:", str(e))
    return messages

@app.route('/')
def index():
    print("Rendering index.html")
    return render_template('index.html')

@app.route('/latest-data')
def latest_data():
    print("Fetching latest data for visualization...")
    data = fetch_latest_kafka_data()
    latest_data = {}

    # Handle missing keys safely
    try:
        for symbol, symbol_data in data.items():
            latest_data[symbol] = {
                "close": symbol_data.get('close', None),  # Ensure fallback to None if data is missing
                "open_pct_change": symbol_data.get('open_pct_change', None),
                "high_pct_change": symbol_data.get('high_pct_change', None),
                "low_pct_change": symbol_data.get('low_pct_change', None),
                "prev_close_pct_change": symbol_data.get('prev_close_pct_change', None),
                "vwap_pct_change": symbol_data.get('vwap_pct_change', None),
                "mid": symbol_data.get('mid', None),
                "uhigh": symbol_data.get('uhigh', None),
                "llow": symbol_data.get('llow', None),
                "uuh": symbol_data.get('uuh', None),
                "lll": symbol_data.get('lll', None)
            }
        print("Latest data prepared for response:", latest_data)
    except Exception as e:
        print("Error processing data:", str(e))
        latest_data = {"error": "Data processing failed"}

    return jsonify(latest_data)

if __name__ == '__main__':
    try:
        print("Starting Flask web server...")
        Timer(1, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
        app.run(debug=True, use_reloader=False)
    except Exception as e:
        print("Error starting Flask server:", str(e))
    finally:
        consumer.close()
        print("Kafka consumer closed.")
