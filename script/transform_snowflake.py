import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE')
)

cursor = conn.cursor()

try:
    print("🔹 Running analytics transformations...")
    
    # Use analytics schema
    cursor.execute("USE SCHEMA analytics")
    
    # Transform 1: Daily prices
    print("🔹 Transforming daily prices...")
    cursor.execute("""
    INSERT INTO daily_prices (date, symbol, open, high, low, close, volume, price_change, price_change_pct)
    SELECT 
        date, symbol, open, high, low, close, volume,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) as price_change,
        ROUND(((close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / 
               LAG(close) OVER (PARTITION BY symbol ORDER BY date)) * 100, 2) as price_change_pct
    FROM financial_market_db.staging.stg_stock_prices
    WHERE date NOT IN (SELECT date FROM daily_prices WHERE symbol = 'AAPL')
    ORDER BY date
    """)
    print(f"✅ Daily prices: {cursor.rowcount} rows")
    
    # Transform 2: Daily sentiment
    print("🔹 Transforming daily sentiment...")
    cursor.execute("""
    INSERT INTO daily_sentiment (date, symbol, avg_sentiment, positive_count, negative_count, neutral_count, total_articles)
    SELECT 
        DATE(date) as date, symbol,
        ROUND(AVG(sentiment_score), 3) as avg_sentiment,
        SUM(CASE WHEN sentiment_score > 0.1 THEN 1 ELSE 0 END) as positive_count,
        SUM(CASE WHEN sentiment_score < -0.1 THEN 1 ELSE 0 END) as negative_count,
        SUM(CASE WHEN sentiment_score BETWEEN -0.1 AND 0.1 THEN 1 ELSE 0 END) as neutral_count,
        COUNT(*) as total_articles
    FROM financial_market_db.staging.stg_sentiment
    WHERE DATE(date) NOT IN (SELECT date FROM daily_sentiment WHERE symbol = 'AAPL')
    GROUP BY DATE(date), symbol
    """)
    print(f"✅ Daily sentiment: {cursor.rowcount} rows")
    
    # Transform 3: Price-sentiment correlation
    print("🔹 Creating price-sentiment correlation...")
    cursor.execute("""
    INSERT INTO price_sentiment_correlation (date, symbol, close_price, price_change_pct, avg_sentiment, sentiment_category)
    SELECT 
        p.date, p.symbol, p.close as close_price, p.price_change_pct,
        COALESCE(s.avg_sentiment, 0) as avg_sentiment,
        CASE 
            WHEN COALESCE(s.avg_sentiment, 0) > 0.1 THEN 'Bullish'
            WHEN COALESCE(s.avg_sentiment, 0) < -0.1 THEN 'Bearish'
            ELSE 'Neutral'
        END as sentiment_category
    FROM daily_prices p
    LEFT JOIN daily_sentiment s ON p.date = s.date AND p.symbol = s.symbol
    WHERE p.date NOT IN (SELECT date FROM price_sentiment_correlation WHERE symbol = 'AAPL')
    """)
    print(f"✅ Correlation table: {cursor.rowcount} rows")
    
    print("✅ All transformations complete!")
    
except Exception as e:
    print(f"❌ Error running transformations: {e}")
    
finally:
    cursor.close()
    conn.close()
