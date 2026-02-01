import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime, timezone

API_KEY = "d5sf0cpr01qgv0tm2en0d5sf0cpr01qgv0tm2eng"
BASE_URL = "https://finnhub.io/api/v1/quote"

SYMBOLS = ["AAPL", "MSFT", "TSLA"]
TOPIC_NAME = "stock_prices"

def create_producer_with_retry(max_retries=30, retry_delay=2):
    """Create Kafka producer with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers="localhost:29092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(0, 10, 1)
            )
            print("✅ Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not ready yet. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception("Failed to connect to Kafka after maximum retries")

def fetch_stock_price(symbol):
    params = {
        "symbol": symbol,
        "token": API_KEY
    }
    response = requests.get(BASE_URL, params=params, timeout=5)
    response.raise_for_status()
    data = response.json()

    return {
        "symbol": symbol,
        "price": data.get("c"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "previous_close": data.get("pc"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "finnhub"
    }

if __name__ == "__main__":
    # Wait for Kafka to be ready
    producer = create_producer_with_retry()
    
    print("Starting stock price streaming...")
    
    while True:
        for symbol in SYMBOLS:
            try:
                event = fetch_stock_price(symbol)
                producer.send(TOPIC_NAME, value=event)
                print(f"✓ Sent to Kafka: {symbol} @ ${event['price']}")
            except Exception as e:
                print(f"❌ Error fetching {symbol}: {e}")

        time.sleep(10)