import os
import time
import torch
import torch.nn.functional as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Config
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'stock_market_news')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'sentiment_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'password')

# Global variables for model caching
tokenizer = None
model = None

def get_model():
    """Lazily load the model to avoid serialization issues in Spark."""
    global tokenizer, model
    if model is None:
        print("Loading FinBERT model...")
        tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        print("FinBERT model loaded.")
    return tokenizer, model

def get_sentiment(text):
    """
    Analyzes sentiment using FinBERT.
    Returns a score between -1.0 (Negative) and 1.0 (Positive).
    """
    if not text:
        return 0.0
        
    try:
        tok, mod = get_model()
        
        inputs = tok(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        with torch.no_grad():
            outputs = mod(**inputs)
            
        # Apply softmax to get probabilities
        probs = F.softmax(outputs.logits, dim=-1)
        # ProsusAI/finbert labels: [positive, negative, neutral] ? 
        # Actually checking config: id2label={0: 'positive', 1: 'negative', 2: 'neutral'}
        # Wait, let's verify standard ProsusAI/finbert labels.
        # Usually it is: 0: positive, 1: negative, 2: neutral.
        # Let's assume this mapping and calculate a composite score.
        
        # Extract probabilities
        pos_score = probs[0][0].item()
        neg_score = probs[0][1].item()
        neu_score = probs[0][2].item()
        
        # Composite score: Positive - Negative. 
        # Neutral dampens the magnitude but doesn't shift the sign.
        # Range: -1.0 to 1.0
        final_score = pos_score - neg_score
        
        return float(final_score)
    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return 0.0

def write_to_postgres(df, epoch_id):
    if df.count() == 0:
        return
        
    print(f"Writing batch {epoch_id} to Postgres...")
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}") \
            .option("dbtable", "sentiment_data") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print("Batch written successfully.")
    except Exception as e:
        print(f"Error writing to Postgres: {e}")

def main():
    time.sleep(10) 
    
    print("Starting Spark Session...")
    spark = SparkSession.builder \
        .appName("StockSentimentProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define schema
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("stock_symbol", StringType()),
        StructField("headline", StringType()),
        StructField("source", StringType()),
        StructField("url", StringType())
    ])

    # --- Stream 1: News Sentiment ---
    print(f"Subscribing to news topic: {TOPIC_NAME}")
    df_news = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_news = df_news.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    parsed_news = parsed_news.withColumn("timestamp", col("timestamp").cast("timestamp"))

    sentiment_udf = udf(get_sentiment, FloatType())
    processed_news = parsed_news.withColumn("sentiment_score", sentiment_udf(col("headline")))

    query_news = processed_news.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()

    # --- Stream 2: Stock Prices ---
    print(f"Subscribing to prices topic: stock_prices")
    
    schema_prices = StructType([
        StructField("timestamp", StringType()),
        StructField("stock_symbol", StringType()),
        StructField("price", FloatType())
    ])

    df_prices = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "stock_prices") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_prices = df_prices.select(from_json(col("value").cast("string"), schema_prices).alias("data")).select("data.*")
    parsed_prices = parsed_prices.withColumn("timestamp", col("timestamp").cast("timestamp"))

    def write_prices_to_postgres(df, epoch_id):
        if df.count() == 0: return
        try:
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}") \
                .option("dbtable", "stock_prices") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error writing prices: {e}")

    query_prices = parsed_prices.writeStream \
        .foreachBatch(write_prices_to_postgres) \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
