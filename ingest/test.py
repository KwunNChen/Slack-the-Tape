import requests, polars as pl

#wassup this is just scratch work; EDA stuff, please ignore this

ticker = "MSFT"
url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
params = {"range": "7d", "interval": "1m", "includePrePost": "false"}
headers = {"User-Agent": "Mozilla/5.0"}
r = requests.get(url, params=params, headers=headers, timeout=10)
data = r.json()

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

print(df.filter(pl.col("close").is_null()))

print(df.filter(pl.col("timestamp").dt.second() != 0))

print(
    df.group_by(pl.col("timestamp").dt.date().alias("date"))
    .agg(pl.len().alias("bars"))
    .sort("date")
)

clean = df.drop_nulls().filter(pl.col("timestamp").dt.second() == 0)

print(
    clean.group_by(pl.col("timestamp").dt.date().alias("date"))
    .agg(pl.len().alias("bars"))
    .sort("date")
)

def probe(sym, params, headers):
    r = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
        params=params, headers=headers, timeout=10,
    )
    print(sym, params, "->", r.status_code)
    try:
        body = r.json()
        print("  error:", body["chart"]["error"], "| result is None:", body["chart"]["result"] is None)
    except Exception as e:
        print("  not JSON:", r.text[:100], e)

good = {"range": "7d", "interval": "1m", "includePrePost": "false"}
ua = {"User-Agent": "Mozilla/5.0"}

probe("ZZZZFAKE", good, ua)
probe("MSFT", good, {})
probe("MSFT", {**good, "range": "60d"}, ua)