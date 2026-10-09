import os
import argparse
import sys
import time
import requests
import polars as pl
import yaml
import logging
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",handlers=[logging.FileHandler(LOG_DIR / "ingest.log", mode="w"),logging.StreamHandler(),],)

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
            if response.status_code in [404, 422]:
                logging.error(f"Received status code {response.status_code} for {ticker}. Not retrying.")
                break
            if attempt < config["max_retries"]:
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
        return df
    else:
        if network_error:
            logging.error(f"Network error occurred while fetching data for {ticker}. Max retries exceeded.")
            raise Exception(f"Network error occurred while fetching data for {ticker}. Max retries exceeded.")
        logging.error(f"Failed to fetch data for {ticker} after {config['max_retries']} attempts. Status code: {response.status_code}")
        raise Exception(f"Failed to fetch data for {ticker} after {config['max_retries']} attempts. Status code: {response.status_code}")

def clean_bars(df, config):
    open_time = dtime.fromisoformat(config["market_open"])
    close_time = dtime.fromisoformat(config["market_close"])
    timestamp = pl.col("timestamp")

    #I know I'm gonna get confused when I check back in here, but this is basically saying "if it's before 4:15PM EST, then skip today's data""
    now_EST = datetime.now(ZoneInfo(config["timezone"]))
    skip_before = dtime.fromisoformat(config["skip_today_before"])
    if now_EST.time() < skip_before:
        df = df.filter(pl.col("timestamp").dt.date() != now_EST.date())
    return df.drop_nulls().filter((timestamp.dt.time() >= open_time) & (timestamp.dt.time() < close_time) & (timestamp.dt.second() == 0))

def validate_bars(fetched_df):
    bad = (
        (pl.col("high") < pl.max_horizontal("open", "close"))
        | (pl.col("low") > pl.min_horizontal("open", "close"))
        | (pl.min_horizontal("open", "high", "low", "close") <= 0)
        | (pl.col("volume") < 0)
        | ~pl.struct("symbol", "timestamp").is_first_distinct()
    )
    good = fetched_df.filter(~bad)
    rejected = fetched_df.filter(bad)
    return good, rejected

def write_bars(df, ticker, base_dir):
    days = df["timestamp"].dt.date().unique()
    for day in days:
        day_df = df.filter(pl.col("timestamp").dt.date() == day)
        folder = base_dir / f"date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        #If partial file, only break temp parquet file, then rename to final parquet file.
        final_path = folder / f"{ticker}.parquet"
        temp_path = folder / f"{ticker}.parquet.tmp"

        day_df.write_parquet(temp_path)
        os.replace(temp_path, final_path)

if __name__ == "__main__":
    results_perticker = {}
    cleaned_results = {}
    failed = []
    with open(Path(__file__).parent / "config.yaml") as file:
        config = yaml.safe_load(file)
    #custom commands logic below
    #python3 -m ingest.fetch_bars --tickers AAPL MSFT <-- lets you specify tickers on the command line, otherwise it will use the config.yaml tickers
    #python3 -m ingest.fetch_bars --tickers AAPL --dry-run <-- lets you specify tickers on the command line, but not write to a paraquet file, it only logs the results
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    tickers = args.tickers or config["tickers"]

    base_dir = Path(__file__).parent.parent / config["data_dir"]
    run_ts = datetime.now(ZoneInfo(config["timezone"]))
    run_id = run_ts.strftime("%Y%m%d_%H%M%S")
    log_rows = []

    for ticker in tickers:
        try:
            results_perticker[ticker] = fetch_bars(ticker, config)
            cleaned_results[ticker] = clean_bars(results_perticker[ticker], config)
            logging.info(f"{ticker}: {len(results_perticker[ticker])} -> {len(cleaned_results[ticker])} observations after cleaning")
            #Validate
            good, rejected = validate_bars(cleaned_results[ticker])
            logging.info(f"{ticker}: {len(good)} good observations, {len(rejected)} rejected observations")
            #Parquet write
            log_rows.append({"run_id": run_id,"run_ts": run_ts.isoformat(),"symbol": ticker,"status": "ok","rows_written": len(good),"rows_rejected": len(rejected),"error": "",})
            if not args.dry_run:
                write_bars(good, ticker, base_dir / "bars")
                logging.info(f"{ticker}: wrote {len(good)} rows to Parquet")
                if len(rejected) > 0:
                    write_bars(rejected, ticker, base_dir / "rejects")
        except Exception as e:
            log_rows.append({"run_id": run_id,"run_ts": run_ts.isoformat(),"symbol": ticker,"status": "failed","rows_written": 0,"rows_rejected": 0,"error": str(e),})
            failed.append(ticker)
            logging.error(f"Error fetching data for {ticker}: {e}")
        time.sleep(0.5)
    if not args.dry_run:
        log_dir = base_dir / "ingest_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(log_rows).write_parquet(log_dir / f"{run_id}.parquet")

    logging.info(f"SUCCESS: {list(cleaned_results.keys())}")
    logging.info(f"FAILED: {failed}")

    if failed:
        sys.exit(1)