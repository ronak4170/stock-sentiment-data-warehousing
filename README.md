# Financial Market Sentiment & Pricing Analytics

## 🎯 Project Overview
End-to-end data pipeline analyzing stock prices and market sentiment correlation using modern data engineering tools.

## 🏗️ Architecture
```
APIs (Alpha Vantage, NewsAPI) 
    → Airflow Orchestration 
    → AWS S3 (Raw Data Lake) 
    → Snowflake (Data Warehouse) 
    → SQL Transformations 
    → Tableau Dashboards
```

## 🛠️ Tech Stack
- **Orchestration**: Apache Airflow
- **Cloud Storage**: AWS S3
- **Data Warehouse**: Snowflake
- **APIs**: Alpha Vantage (stock prices), NewsAPI (sentiment)
- **Languages**: Python, SQL
- **Visualization**: Tableau

## 📊 Data Flow
1. **Extract**: Fetch daily stock prices and news sentiment via APIs
2. **Load**: Upload raw JSON to AWS S3
3. **Transform**: Load to Snowflake staging → Analytics layer with SQL
4. **Analyze**: Daily price changes, sentiment aggregation, correlation analysis

## 🗂️ Project Structure
```
financial-market-sentiment/
├── scripts/
│   ├── fetch_stock_data.py        # API data collection
│   ├── fetch_sentiment_data.py
│   ├── upload_to_s3.py            # Cloud upload
│   ├── load_to_snowflake.py       # Warehouse loading
│   └── transform_snowflake.py     # Analytics transformations
├── airflow/
│   └── dags/
│       └── daily_market_pipeline.py  # Orchestration
├── snowflake/
│   └── sql/                       # SQL transformations
├── data/                          # Local raw data
└── README.md
```

## 🚀 Key Features
- Automated daily data pipeline
- Real-time stock price tracking
- News sentiment analysis using NLP
- Price vs sentiment correlation
- Cloud-native architecture
- Production-ready error handling

## 📈 Analytics Layer
- **Daily Prices**: OHLC data with price change calculations
- **Daily Sentiment**: Aggregated positive/negative/neutral counts
- **Correlation**: Combined price movements with sentiment scores

## 🔄 Pipeline Schedule
Runs daily at 9:00 AM UTC via Airflow

## 📊 Sample Insights
- Track how market sentiment correlates with price movements
- Identify bullish vs bearish days
- Volatility analysis
- Trend detection
