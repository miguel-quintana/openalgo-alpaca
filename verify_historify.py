import os
import pandas as pd
from datetime import datetime, timezone, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from dotenv import load_dotenv

# Import your local Historify DB module
from database.historify_db import get_ohlcv

# Load API keys from .env
load_dotenv()
API_KEY = os.getenv("BROKER_API_KEY")
SECRET_KEY = os.getenv("BROKER_API_SECRET")

SYMBOL = "MU"
EXCHANGE = "US" # Assuming US for Alpaca
INTERVAL = "D"  # Test Daily aggregation first
DAYS_BACK = 5

def fetch_alpaca_ground_truth(symbol, start_dt, end_dt):
    """Fetch exact historical data straight from Alpaca API."""
    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    
    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Day, # Request native daily bars
        start=start_dt,
        end=end_dt,
        feed="sip" # or "iex" depending on your subscription
    )
    
    bars = client.get_stock_bars(request_params).df
    if bars.empty:
        return pd.DataFrame()
        
    bars = bars.reset_index()
    # Convert timestamp to epoch seconds for direct comparison with Historify
    bars['timestamp'] = bars['timestamp'].apply(lambda x: int(x.timestamp()))
    bars = bars[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    bars.set_index('timestamp', inplace=True)
    return bars

def main():
    print(f"--- Validating Historify Data for {SYMBOL} ---")
    
    # Define time window (Last 5 days)
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=DAYS_BACK)
    
    # 1. Fetch Alpaca Ground Truth
    print("\nFetching Ground Truth from Alpaca...")
    alpaca_df = fetch_alpaca_ground_truth(SYMBOL, start_dt, end_dt)
    
    if alpaca_df.empty:
        print("No data returned from Alpaca. Check your API keys and subscription.")
        return

    # 2. Fetch Historify Data (from DuckDB)
    print("Fetching Local Data from Historify DB...")
    start_epoch = int(start_dt.timestamp())
    end_epoch = int(end_dt.timestamp())
    
    historify_df = get_ohlcv(
        symbol=SYMBOL, 
        exchange=EXCHANGE, 
        interval=INTERVAL, 
        start_timestamp=start_epoch, 
        end_timestamp=end_epoch
    )
    
    if historify_df.empty:
        print("No data found in Historify DB. Did the download job complete successfully?")
        return
        
    historify_df.set_index('timestamp', inplace=True)
    
    # 3. Compare the DataFrames
    print("\n--- Comparison Results ---")
    comparison = alpaca_df.join(historify_df, lsuffix='_alpaca', rsuffix='_local', how='inner')
    
    if comparison.empty:
        print("Dataframes do not overlap on timestamps! Check timezone alignments in the DB.")
        return
        
    # Calculate Volume Difference
    comparison['volume_diff'] = comparison['volume_local'] - comparison['volume_alpaca']
    
    # Format dates for readability
    comparison.index = pd.to_datetime(comparison.index, unit='s').strftime('%Y-%m-%d')
    
    print(comparison[['volume_alpaca', 'volume_local', 'volume_diff', 'close_alpaca', 'close_local']])
    
    # Summary
    total_mismatch = (comparison['volume_diff'] != 0).sum()
    if total_mismatch == 0:
        print("\n✅ SUCCESS: Local Historify volume perfectly matches Alpaca ground truth.")
    else:
        print(f"\n❌ FAILED: Found {total_mismatch} days with mismatched volume.")
        if comparison['volume_diff'].mean() > 0:
            print("Diagnosis: Historify volume is HIGHER. You are likely aggregating extended hours 1m bars into 1D.")
        else:
            print("Diagnosis: Historify volume is LOWER. You may be missing 1m bars in your database for those days.")

if __name__ == "__main__":
    main()