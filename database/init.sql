CREATE TABLE IF NOT EXISTS sentiment_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stock_symbol VARCHAR(10),
    headline TEXT,
    sentiment_score FLOAT,
    source VARCHAR(50),
    url TEXT
);

CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stock_symbol VARCHAR(10),
    price FLOAT
);
