import requests
import json
from datetime import datetime
import os
from textblob import TextBlob
from dotenv import load_dotenv

# Load API keys
load_dotenv()
NEWS_API_KEY = os.getenv("NEWSAPI_KEY")

# Folder to store JSON
output_dir = "data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Define query and endpoint
query = "AAPL"  # You can change this to any stock
url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&apiKey={NEWS_API_KEY}"

# Fetch data
response = requests.get(url)
news_data = response.json()

# Extract headlines and calculate sentiment
sentiment_results = []
for article in news_data.get("articles", []):
    headline = article.get("title", "")
    if headline:
        score = TextBlob(headline).sentiment.polarity  # -1 to 1
        sentiment_results.append({
            "date": article.get("publishedAt", ""),
            "headline": headline,
            "sentiment_score": score
        })

# Save sentiment JSON
today = datetime.now().strftime("%Y-%m-%d")
output_path = os.path.join(output_dir, f"sentiment_{query}_{today}.json")

with open(output_path, "w") as f:
    json.dump(sentiment_results, f, indent=2)

print(f"Sentiment data saved to {output_path}")
