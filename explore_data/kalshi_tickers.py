import duckdb
import pathlib
import pandas as pd

# config
SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "data"
KALSHI_TRADES = DATA_ROOT / "kalshi" / "trades"
OUTPUT_FILE = "kalshi_unique_tickers.txt"

# connect to temporary DuckDB in-memory DB
con = duckdb.connect()

# get all parquet files, skip hidden ones
parquet_files = [f for f in KALSHI_TRADES.glob("*.parquet") if not f.name.startswith("._")]

if not parquet_files:
    print("No parquet files found in kalshi/trades folder.")
    exit()

# register each parquet file as a temporary table in DuckDB
for i, file in enumerate(parquet_files):
    table_name = f"trades_{i}"
    con.execute(f"CREATE TEMPORARY TABLE {table_name} AS SELECT * FROM read_parquet('{file}')")

# union all trades tables and get unique tickers
union_query = " UNION ALL ".join([f"SELECT ticker FROM trades_{i}" for i in range(len(parquet_files))])

unique_underlyings_query = f"""
SELECT DISTINCT SPLIT_PART(ticker, '-', 1) AS underlying
FROM ({union_query})
ORDER BY underlying
"""

# fetch results
unique_underlyings = con.execute(unique_underlyings_query).fetchdf()

# save to file
unique_underlyings.to_csv(OUTPUT_FILE, index=False)
print(f"Found {len(unique_underlyings)} unique underlyings. Saved to {OUTPUT_FILE}")

con.close()