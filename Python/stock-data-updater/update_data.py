import yfinance as yf
import pandas as pd
import argparse
import os
from datetime import datetime, timedelta

def fetch_and_append(symbol, days_back=60, output_file=None):
    if output_file is None:
        output_file = f"{symbol}_15min.txt"
    days_back = min(days_back, 60)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    try:
        data = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval='15m', auto_adjust=False)
        if data.empty:
            print(f"No data for {symbol}")
            return
        data = data.reset_index()
        data['Datetime'] = pd.to_datetime(data['Datetime']).dt.strftime('%d/%m/%Y %H:%M:%S')
        data = data[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        if os.path.exists(output_file):
            existing = pd.read_csv(output_file, names=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], parse_dates=['Date'])
            existing['Date'] = pd.to_datetime(existing['Date'], format='%d/%m/%Y %H:%M:%S')
            data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y %H:%M:%S')
            new_data = data[~data['Date'].isin(existing['Date'])]
            if not new_data.empty:
                updated = pd.concat([existing, new_data], ignore_index=True).sort_values('Date')
                updated.to_csv(output_file, index=False, date_format='%d/%m/%Y %H:%M:%S', header=False, sep=',')
                print(f"Appended {len(new_data)} new rows to {output_file}")
            else:
                print(f"No new data for {symbol}")
        else:
            data.to_csv(output_file, index=False, date_format='%d/%m/%Y %H:%M:%S', header=False, sep=',')
            print(f"Created {output_file} with {len(data)} rows")
    except Exception as e:
        print(f"Error downloading {symbol}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update 15-min stock data")
    parser.add_argument('--days', type=int, default=7, help="Days back to fetch (max 60)")
    args = parser.parse_args()
    try:
        with open('symbols.txt', 'r') as f:
            symbols = [line.strip() for line in f if line.strip()]
        for symbol in symbols:
            fetch_and_append(symbol, args.days)
    except FileNotFoundError:
        print("Error: symbols.txt not found")