import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

bucket_name = os.getenv('S3_BUCKET_NAME')
today = datetime.now().strftime("%Y-%m-%d")

# Upload files
data_folder = "data"
files_to_upload = [
    f"stock_AAPL_{today}.json",
    f"sentiment_AAPL_{today}.json"
]

for filename in files_to_upload:
    local_path = os.path.join(data_folder, filename)
    
    if os.path.exists(local_path):
        s3_key = f"raw/{today}/{filename}"
        
        try:
            s3_client.upload_file(local_path, bucket_name, s3_key)
            print(f"✅ Uploaded {filename} to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"❌ Error uploading {filename}: {e}")
    else:
        print(f"⚠️  File not found: {local_path}")

print("Upload complete!")
