#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta
import argparse

# === CONFIG ===
TICKERS = ['SPY', 'QQQ', 'AAPL', 'MSFT', 'NVDA', '^GSPC', '^DJI']
BASE_DIR = "Wave59_Yahoo_Data"
RATE_LIMIT_SEC = 2  # Be kind to Yahoo

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['daily', '15min'], default='15min')
    parser.add_argument('--years', type=int, default=5)
    return parser.parse_args()

def safe_ticker(t):
    return t.replace('^', '')

def download_and_save(ticker, interval, period=None, start=None, end=None):
    print(f"  → {ticker} [{interval}]")
    try:
        data = yf.download(ticker, period=period, start=start, end=end, interval=interval, progress=False)
        if data.empty:
            print(f"    No data for {ticker}")
            return None
        return data
    except Exception as e:
        print(f"    Error: {e}")
        return None

def main():
    args = parse_args()
    os.makedirs(BASE_DIR, exist_ok=True)
    today = datetime.now().date()

    for i, ticker in enumerate(TICKERS):
        if i > 0:
            time.sleep(RATE_LIMIT_SEC)

        safe_name = safe_ticker(ticker)
        ticker_dir = os.path.join(BASE_DIR, safe_name)
        os.makedirs(ticker_dir, exist_ok=True)
        csv_path = os.path.join(ticker_dir, f"{safe_name}_15m.csv")

        if args.mode == 'daily':
            # DAILY: Full history (once per day)
            data = download_and_save(ticker, interval="1d", period=f"{args.years}y")
            if data is not None:
                data.to_csv(csv_path.replace('_15m', '_daily'))
                print(f"    Saved daily → {csv_path.replace('_15m', '_daily')}")

        else:
            # 15-MIN: Only new bars
            existing = pd.read_csv(csv_path, index_col=0, parse_dates=True) if os.path.exists(csv_path) else pd.DataFrame()
            last_date = existing.index.max().date() if not existing.empty else (today - timedelta(days=7))

            # Only get data after last known bar
            start = last_date + timedelta(days=1) if last_date < today else last_date
            data = download_and_save(ticker, interval="15m", start=start)

            if data is not None and not data.empty:
                if not existing.empty:
                    data = pd.concat([existing, data]).drop_duplicates()
                data.to_csv(csv_path)
                print(f"    Updated → {len(data)} rows")
            else:
                print(f"    No new 15m data")

    print(f"All done → {BASE_DIR}/")

if __name__ == "__main__":
    main()
