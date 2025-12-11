# Real-Time Stock Sentiment & Price Pipeline 🚀

A scalable, real-time data engineering project that processes live stock market news and price data to visualize market sentiment.

![Dashboard Screenshot](https://via.placeholder.com/800x400?text=Dashboard+Screenshot+Here)

## 🏗️ Architecture

**Kafka** → **Spark Streaming** → **PostgreSQL** → **Streamlit Dashboard**

1.  **Ingestion (Producer)**:
    *   Fetches live news from **NewsAPI** (every 15 mins).
    *   Generates simulated real-time stock prices (every 5 secs).
    *   Streams both to separate **Kafka** topics.
2.  **Message Broker (Kafka)**: Buffers the high-throughput data streams.
3.  **Processing (Spark)**:
    *   Stream 1: Consumes news, applies **FinBERT** (Financial BERT) for advanced sentiment analysis.
    *   Stream 2: Consumes and processes price ticks.
    *   Writes structured data to **PostgreSQL**.
4.  **Storage (PostgreSQL)**: Persists sentiment scores and price history.
5.  **Visualization (Streamlit)**: Live dashboard showing:
    *   Real-time Stock Prices 💰
    *   Live Sentiment Scores (Bullish/Bearish) 📈
    *   Price vs. Sentiment Trends.

## 🛠️ Tech Stack

*   **Language**: Python 3.9
*   **Streaming**: Apache Kafka, Zookeeper
*   **Processing**: Apache Spark (PySpark), Structured Streaming
*   **AI/ML**: HuggingFace Transformers (FinBERT), PyTorch
*   **Database**: PostgreSQL
*   **Visualization**: Streamlit, Plotly
*   **Containerization**: Docker, Docker Compose

## 🚀 How to Run

### Prerequisites
*   Docker & Docker Compose installed.
*   API Key from [NewsAPI.org](https://newsapi.org/) (Free).

### Steps
1.  **Clone the repo**:
    ```bash
    git clone https://github.com/yourusername/stock-sentiment-pipeline.git
    cd stock-sentiment-pipeline
    ```

2.  **Configure API Key**:
    Create a `.env` file in the root directory:
    ```properties
    STOCK_API_KEY=your_newsapi_key_here
    STOCK_API_PROVIDER=newsapi
    ```

3.  **Start the Pipeline**:
    ```bash
    docker-compose up --build
    ```
    *Note: The first run will take 5-10 minutes to download the FinBERT AI model.*

4.  **Access Dashboard**:
    Open [http://localhost:8501](http://localhost:8501) in your browser.

## 📂 Project Structure

*   `producer/`: Scripts to fetch NewsAPI data and generate price streams.
*   `processor/`: Spark job for sentiment analysis (FinBERT) and DB writing.
*   `dashboard/`: Streamlit app for real-time visualization.
*   `database/`: SQL schema initialization.
*   `docker-compose.yml`: Orchestration of all services.

## 🔮 Future Improvements
*   Replace simulated prices with real AlphaVantage/Yahoo Finance API.
*   Add email alerts for extreme sentiment drops.
*   Deploy to AWS (EC2/EMR).
