import requests
import time
from datetime import datetime, timezone

API_KEY = "d5sf0cpr01qgv0tm2en0d5sf0cpr01qgv0tm2eng"
BASE_URL = "https://finnhub.io/api/v1/quote"

SYMBOLS = ["AAPL", "MSFT", "TSLA"]

def fetch_stock_price(symbol):
    params = {
        "symbol": symbol,
        "token": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    event = {
        "symbol": symbol,
        "price": data.get("c"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "previous_close": data.get("pc"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "finnhub"
    }

    return event

if __name__ == "__main__":
    while True:
        for symbol in SYMBOLS:
            try:
                event = fetch_stock_price(symbol)
                print(event)
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

        time.sleep(10)  # fetch every 10 seconds
