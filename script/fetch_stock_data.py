import requests
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
SYMBOL = "AAPL"

url = (
    "https://www.alphavantage.co/query?"
    f"function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={API_KEY}"
)

response = requests.get(url)
data = response.json()

today = datetime.now().strftime("%Y-%m-%d")

output_path = f"data/stock_{SYMBOL}_{today}.json"

with open(output_path, "w") as f:
    json.dump(data, f)

print(f"Stock data saved to {output_path}")
