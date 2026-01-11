from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Get project directory
PROJECT_DIR = "/Users/ronak/Documents/financial sentiment"

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 10),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'daily_market_sentiment_pipeline',
    default_args=default_args,
    description='Complete ETL: APIs -> S3 -> Snowflake -> Analytics',
    schedule='0 9 * * *',  # Daily at 9 AM
    catchup=False,
    tags=['market', 'sentiment', 's3', 'snowflake']
) as dag:
    
    # Task 1: Fetch stock data from API
    fetch_stock = BashOperator(
        task_id='fetch_stock_data',
        bash_command=f'cd "{PROJECT_DIR}" && source venv/bin/activate && python scripts/fetch_stock_data.py',
    )
    
    # Task 2: Fetch sentiment data from API
    fetch_sentiment = BashOperator(
        task_id='fetch_sentiment_data',
        bash_command=f'cd "{PROJECT_DIR}" && source venv/bin/activate && python scripts/fetch_sentiment_data.py',
    )
    
    # Task 3: Upload raw data to S3
    upload_s3 = BashOperator(
        task_id='upload_to_s3',
        bash_command=f'cd "{PROJECT_DIR}" && source venv/bin/activate && python scripts/upload_to_s3.py',
    )
    
    # Task 4: Load data from S3 to Snowflake staging
    load_snowflake = BashOperator(
        task_id='load_to_snowflake',
        bash_command=f'cd "{PROJECT_DIR}" && source venv/bin/activate && python scripts/load_to_snowflake.py',
    )
    
    # Task 5: Run Snowflake transformations (analytics)
    transform_snowflake = BashOperator(
        task_id='transform_snowflake',
        bash_command=f'cd "{PROJECT_DIR}" && source venv/bin/activate && python scripts/transform_snowflake.py',
    )
    
    # Define task dependencies (pipeline flow)
    # Fetch both data sources in parallel -> Upload to S3 -> Load to Snowflake -> Transform
    [fetch_stock, fetch_sentiment] >> upload_s3 >> load_snowflake >> transform_snowflake
