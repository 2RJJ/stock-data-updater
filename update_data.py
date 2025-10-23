import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import argparse

def update_stock_data(symbols_file, days=60):
    with open(symbols_file, 'r') as f:
        symbols = [line.strip() for line in f if line.strip()]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    for symbol in symbols:
        try:
            data = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), 
                             end=end_date.strftime('%Y-%m-%d'), interval='15m', 
                             auto_adjust=False)
            if not data.empty:
                data.index = data.index.strftime('%d/%m/%Y %H:%M:%S')
                filename = f"{symbol.replace('.', '_')}_15min.txt"
                data.to_string(filename, index=True, header=True)
                print(f"Saved {filename}")
        except Exception as e:
            print(f"Error with {symbol}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update stock data for 15-minute intervals.")
    parser.add_argument("--days", type=int, default=60, help="Number of days to download (max 60)")
    args = parser.parse_args()
    update_stock_data("symbols.txt", args.days)