#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

# =============== ARGUMENTS ===============
parser = argparse.ArgumentParser(description="Yahoo Finance → Wave59 data downloader")
parser.add_argument('--mode', choices=['daily', '15min'], required=True)
parser.add_argument('--years', type=int, default=20, help="Years of daily history (daily mode only)")
parser.add_argument('--output-dir', type=str, 
                    default="/Users/ronjones/Documents/YahooFinance")
args = parser.parse_args()

OUTPUT_DIR = Path(args.output_dir)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"Saving all files to: {OUTPUT_DIR}")
print(f"Mode: {args.mode.upper()} – Started at {datetime.now():%Y-%m-%d %H:%M:%S}\n")

# =============== LOAD TICKERS FROM symbols.txt ===============
SYMBOLS_FILE = Path(__file__).parent / "symbols.txt"  # Assumes symbols.txt is in same folder as script
if not SYMBOLS_FILE.exists():
    print(f"ERROR: symbols.txt not found at {SYMBOLS_FILE}")
    print("     Download it with: curl -o symbols.txt https://raw.githubusercontent.com/2RJJ/stock-data-updater/master/symbols.txt")
    sys.exit(1)

TICKERS = []
with open(SYMBOLS_FILE, 'r') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):  # Skip empty lines and comments
            TICKERS.append(line)

if not TICKERS:
    print("ERROR: No tickers found in symbols.txt – add some (one per line)!")
    sys.exit(1)

print(f"Loaded {len(TICKERS)} tickers from symbols.txt: {', '.join(TICKERS[:10])}{'...' if len(TICKERS) > 10 else ''}\n")

failed = []

for ticker in TICKERS:
    try:
        print(f"{ticker.ljust(10)} → downloading... ", end="")
        t = yf.Ticker(ticker)

        if args.mode == "daily":
            start = (datetime.today() - timedelta(days=args.years * 365)).strftime("%Y-%m-%d")
            df = t.history(start=start, interval="1d")
            filename = OUTPUT_DIR / f"{ticker}_daily.csv"
        else:  # 15min
            df = t.history(period="60d", interval="15m")
            filename = OUTPUT_DIR / f"{ticker}_15m.csv"

        if df.empty:
            print("EMPTY")
            failed.append(ticker)
            continue

        df = df.drop(columns=["Dividends", "Stock Splits"], errors="ignore")
        df.to_csv(filename)
        print(f"✓ {len(df):,} rows")

        time.sleep(0.5)  # be nice to Yahoo
    except Exception as e:
        print(f"✗ FAILED ({e})")
        failed.append(ticker)

print("\n" + "="*60)
print(f"Finished {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"Success: {len(TICKERS)-len(failed)} / {len(TICKERS)}")
if failed:
    print("Failed:", ", ".join(failed))
print("="*60)
