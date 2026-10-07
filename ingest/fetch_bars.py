import time
import requests
import polars as pl
import yaml
import logging
from pathlib import Path

'''
A bad response body crashes oddly. If data["chart"]["result"] is None, line 27 gives a cryptic TypeError. Check for None or an empty timestamp list and raise a clear message instead.
The fake ticker still waits about 14 seconds because 404 is retried. You can stop retrying on 404 and 422 later.
'''

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",handlers=[logging.FileHandler(LOG_DIR / "ingest.log"),logging.StreamHandler(),],)

def fetch_bars(ticker, config):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"range": config["period"], "interval": config["interval"], "includePrePost": "false"}
    headers = {"User-Agent": "Mozilla/5.0"}
    success = False
    network_error = False

    for attempt in range(config["max_retries"] + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                success = True
                break
            logging.warning(f"Attempt {attempt + 1} failed for {ticker}. Status code: {response.status_code}. Retrying...")
            time.sleep(config["backoff_base_s"] * 2 ** attempt)
        except requests.exceptions.RequestException as e:
            if attempt < config["max_retries"]:
                logging.warning(f"Attempt {attempt + 1} failed for {ticker}. Exception: {e}. Retrying...")
                time.sleep(config["backoff_base_s"] * 2 ** attempt)
            if attempt == config["max_retries"]:
                logging.error(f"Max retries exceeded for {ticker}. Exception: {e}")
                network_error = True

    if success:
        data = response.json()
        result = data["chart"]["result"][0]
        quote = result["indicators"]["quote"][0]
        df = pl.DataFrame({
            "timestamp": result["timestamp"],
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["close"],
            "volume": quote["volume"]
        })
        df = df.with_columns(pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC").dt.convert_time_zone("America/New_York"),
        pl.lit(ticker).alias("symbol"))
        logging.info(f"Fetched {len(df)} bars for {ticker}")
        return df
    else:
        if network_error:
            logging.error(f"Network error occurred while fetching data for {ticker}. Max retries exceeded.")
            raise Exception(f"Network error occurred while fetching data for {ticker}. Max retries exceeded.")
        logging.error(f"Failed to fetch data for {ticker} after {config['max_retries']} attempts. Status code: {response.status_code}")
        raise Exception(f"Failed to fetch data for {ticker} after {config['max_retries']} attempts. Status code: {response.status_code}")

if __name__ == "__main__":
    results_perticker = {}
    failed = []
    with open(Path(__file__).parent / "config.yaml") as file:
        config = yaml.safe_load(file)

    for ticker in config["tickers"]:
        try:
            results_perticker[ticker] = fetch_bars(ticker, config)
        except Exception as e:
            failed.append(ticker)
            logging.error(f"Error fetching data for {ticker}: {e}")
        time.sleep(0.5)
        
    logging.info(f"SUCCESS: {results_perticker.keys()}")
    logging.info(f"FAILED: {failed}")