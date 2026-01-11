import snowflake.connector
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

cursor = conn.cursor()
today = datetime.now().strftime("%Y-%m-%d")

try:
    print("🔹 Loading stock data from S3...")
    
    # Load stock data
    stock_sql = f"""
    INSERT INTO stg_stock_prices (date, symbol, open, high, low, close, volume)
    SELECT 
        TO_DATE(f.key::STRING) as date,
        'AAPL' as symbol,
        f.value:"1. open"::FLOAT as open,
        f.value:"2. high"::FLOAT as high,
        f.value:"3. low"::FLOAT as low,
        f.value:"4. close"::FLOAT as close,
        f.value:"5. volume"::BIGINT as volume
    FROM @s3_stage s,
        LATERAL FLATTEN(input => s.$1:"Time Series (Daily)") f
    WHERE s.metadata$filename LIKE '%stock_AAPL_{today}%'
    AND TO_DATE(f.key::STRING) = '{today}'
    """
    cursor.execute(stock_sql)
    print(f"✅ Stock data loaded: {cursor.rowcount} rows")
    
    print("🔹 Loading sentiment data from S3...")
    
    # Load sentiment data
    sentiment_sql = f"""
    COPY INTO stg_sentiment (date, symbol, headline, sentiment_score)
    FROM (
      SELECT 
        TO_TIMESTAMP($1:date::STRING),
        'AAPL',
        $1:headline::STRING,
        $1:sentiment_score::FLOAT
      FROM @s3_stage
    )
    PATTERN = '.*sentiment_AAPL_{today}.*json'
    FILE_FORMAT = sentiment_json_format
    ON_ERROR = 'CONTINUE'
    """
    cursor.execute(sentiment_sql)
    print(f"✅ Sentiment data loaded: {cursor.rowcount} rows")
    
    print("✅ All data loaded to Snowflake staging tables!")
    
except Exception as e:
    print(f"❌ Error loading to Snowflake: {e}")
    
finally:
    cursor.close()
    conn.close()
