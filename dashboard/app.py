import streamlit as st
import pandas as pd
import time
import os
from sqlalchemy import create_engine
import plotly.express as px
from datetime import datetime

# Config
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'sentiment_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'password')

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"

st.set_page_config(
    page_title="Stock Sentiment Pipeline",
    layout="wide",
    page_icon="📈"
)

# Sidebar for Context
with st.sidebar:
    st.title("ℹ️ Project Guide")
    st.markdown("""
    **What is this?**
    This is a real-time data engineering pipeline that processes live stock market news to gauge market sentiment.
    
    **How it works:**
    1. **Ingestion**: NewsAPI fetches live stock news.
    2. **Streaming**: Kafka buffers the data streams.
    3. **Processing**: Spark analyzes the text sentiment.
    4. **Storage**: Postgres saves the structured results.
    5. **Visualization**: This dashboard shows the live insights.
    
    **Sentiment Score Guide:**
    - **+1.0**: Very Positive (Bullish) 🟢
    - **0.0**: Neutral ⚪
    - **-1.0**: Very Negative (Bearish) 🔴
    """)
    st.divider()
    st.caption("Architecture: Kafka → Spark Streaming → PostgreSQL → Streamlit")

st.title("📈 Real-Time Stock Market Sentiment")
st.markdown("Monitoring live news streams to gauge market mood for **AAPL, TSLA, GOOGL, AMZN, MSFT**.")

@st.cache_resource
def get_db_connection():
    return create_engine(DB_URL)

engine = get_db_connection()

def load_data():
    try:
        # Load Sentiment Data
        query_sentiment = "SELECT * FROM sentiment_data ORDER BY timestamp DESC LIMIT 1000"
        df_sentiment = pd.read_sql(query_sentiment, engine)
        
        # Load Price Data
        query_prices = "SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 1000"
        df_prices = pd.read_sql(query_prices, engine)
        
        return df_sentiment, df_prices
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame()

placeholder = st.empty()

while True:
    df, df_prices = load_data()
    
    with placeholder.container():
        if not df.empty:
            # Calculate stats
            latest_time = pd.to_datetime(df['timestamp']).max()
            
            # Status Bar
            st.success(f"🟢 Pipeline Active | Last Data Received: {latest_time}")
            
            # Metrics Row
            st.subheader("📊 Live Sentiment & Prices")
            
            avg_sentiment = df.groupby('stock_symbol')['sentiment_score'].mean().reset_index()
            latest_prices = df_prices.sort_values('timestamp').groupby('stock_symbol').tail(1)[['stock_symbol', 'price']] if not df_prices.empty else pd.DataFrame()
            
            # Merge metrics
            metrics_df = avg_sentiment
            if not latest_prices.empty:
                metrics_df = pd.merge(avg_sentiment, latest_prices, on='stock_symbol', how='left')
            
            # Display metrics in columns
            num_stocks = len(metrics_df)
            if num_stocks > 0:
                cols = st.columns(min(num_stocks, 5))
                for idx, row in metrics_df.iterrows():
                    if idx < 5: # Limit to 5 columns
                        with cols[idx]:
                            score = row['sentiment_score']
                            price = row.get('price', 0.0)
                            
                            # Determine color/arrow
                            delta_color = "normal"
                            if score > 0.1: delta_color = "normal" 
                            elif score < -0.1: delta_color = "inverse"
                            
                            st.metric(
                                label=row['stock_symbol'],
                                value=f"${price:.2f}",
                                delta=f"Sentiment: {score:.2f}",
                                delta_color=delta_color
                            )

            # Charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📉 Sentiment Trend")
                fig_line = px.line(df, x='timestamp', y='sentiment_score', color='stock_symbol', 
                                 title="Sentiment Over Time")
                st.plotly_chart(fig_line, use_container_width=True, key=f"trend_chart_{time.time()}")

            with col2:
                st.markdown("### 💰 Price Trend")
                if not df_prices.empty:
                    fig_price = px.line(df_prices, x='timestamp', y='price', color='stock_symbol',
                                      title="Stock Prices Over Time")
                    st.plotly_chart(fig_price, use_container_width=True, key=f"price_chart_{time.time()}")
                else:
                    st.info("Waiting for price data...")

            # Data Table
            st.subheader("📰 Latest News Feed")
            st.dataframe(
                df[['timestamp', 'stock_symbol', 'headline', 'sentiment_score', 'url']].head(10),
                column_config={
                    "url": st.column_config.LinkColumn("Read Article"),
                    "sentiment_score": st.column_config.NumberColumn("Sentiment", format="%.2f"),
                    "timestamp": st.column_config.DatetimeColumn("Time", format="D MMM, HH:mm:ss")
                },
                use_container_width=True
            )
            
        else:
            st.warning("⏳ Waiting for data... The pipeline is starting up.")
            
    time.sleep(2)
