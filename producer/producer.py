import time
import json
import os
import requests
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'stock_market_news')
API_KEY = os.environ.get('STOCK_API_KEY')
API_PROVIDER = os.environ.get('STOCK_API_PROVIDER', 'newsapi')

# NewsAPI Configuration
NEWSAPI_URL = "https://newsapi.org/v2/everything"
# Keywords to search for stock market news
SEARCH_QUERY = "stock market OR crypto OR economy OR finance"

def fetch_newsapi_data():
    """Fetches news from NewsAPI."""
    if not API_KEY or API_KEY == 'paste_your_key_here':
        print("Error: API Key is missing or invalid.")
        return []

    try:
        # NewsAPI 'everything' endpoint
        params = {
            'q': SEARCH_QUERY,
            'apiKey': API_KEY,
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 20 
        }
        response = requests.get(NEWSAPI_URL, params=params)
        
        if response.status_code == 200:
            return response.json().get('articles', [])
        else:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Exception during API call: {e}")
        return []

def format_news(news_item):
    """Formats the raw news item into our standard structure."""
    # NewsAPI returns: {source: {id, name}, author, title, description, url, urlToImage, publishedAt, content}
    
    # Try to extract a stock symbol from the title/description (very basic heuristic)
    text = (news_item.get('title') or '') + " " + (news_item.get('description') or '')
    symbol = "MARKET"
    if "Apple" in text: symbol = "AAPL"
    elif "Tesla" in text: symbol = "TSLA"
    elif "Google" in text: symbol = "GOOGL"
    elif "Amazon" in text: symbol = "AMZN"
    elif "Microsoft" in text: symbol = "MSFT"
    elif "Bitcoin" in text: symbol = "BTC"

    return {
        'timestamp': news_item.get('publishedAt'),
        'stock_symbol': symbol,
        'headline': news_item.get('title'),
        'source': news_item.get('source', {}).get('name', 'NewsAPI'),
        'url': news_item.get('url')
    }

def main():
    print(f"Starting Producer with Provider: {API_PROVIDER}")
    
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
    
    # Keep track of sent URLs to avoid duplicates (NewsAPI doesn't give unique IDs always)
    sent_urls = set()

    while True:
        print("Fetching news from NewsAPI...")
        articles = fetch_newsapi_data()
        
        count = 0
        for item in articles:
            url = item.get('url')
            if url and url not in sent_urls:
                formatted_news = format_news(item)
                print(f"Sending: {formatted_news['headline'][:50]}...")
                producer.send(TOPIC_NAME, value=formatted_news)
                sent_urls.add(url)
                count += 1
        
        print(f"Sent {count} new articles.")
        
        # Clean up sent_urls if it gets too big
        if len(sent_urls) > 5000:
            sent_urls.clear()

        # NewsAPI Developer Plan limit is 100 requests/day. 
        # Polling every 15 minutes (900 seconds) = 96 requests/day.
        print("Sleeping for 15 minutes to respect API limits...")
        time.sleep(900)

if __name__ == "__main__":
    main()
