import time
import json
import os
import requests
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'stock_prices' # New topic for prices
API_KEY = os.environ.get('STOCK_API_KEY') # We can reuse the same key if provider supports it, or mock it for now

# Stocks to track
STOCKS = ['AAPL', 'GOOGL', 'AMZN', 'TSLA', 'MSFT']

# Mock base prices for simulation (since free APIs have strict rate limits, simulating realistic price movement is often better for demos)
PRICES = {
    'AAPL': 150.0,
    'GOOGL': 2800.0,
    'AMZN': 3400.0,
    'TSLA': 700.0,
    'MSFT': 300.0
}

def generate_price(symbol):
    """
    Simulates a realistic stock price movement using a random walk.
    """
    current_price = PRICES[symbol]
    # Random percentage change between -0.5% and +0.5%
    change_percent = random.uniform(-0.005, 0.005)
    new_price = current_price * (1 + change_percent)
    PRICES[symbol] = new_price
    
    return {
        'timestamp': datetime.now().isoformat(),
        'stock_symbol': symbol,
        'price': round(new_price, 2)
    }

def main():
    print(f"Starting Price Producer...")
    
    # Kafka Connection
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Connected to Kafka!")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    print(f"Producing to topic: {TOPIC_NAME}")
    
    while True:
        for symbol in STOCKS:
            price_data = generate_price(symbol)
            print(f"Sending Price: {symbol} ${price_data['price']}")
            producer.send(TOPIC_NAME, value=price_data)
        
        # Send prices every 5 seconds
        time.sleep(5)

if __name__ == "__main__":
    main()
