import os
from dotenv import load_dotenv
import time
from pprint import pprint

# 1. Mock DB functions to avoid needing a live SQLite database connection just for testing
import database.token_db
database.token_db.get_br_symbol = lambda ts, ex: ts  # Bypass DB symbol lookup

from broker.alpaca.api.order_api import (
    place_order_api, 
    get_order_book, 
    get_positions, 
    modify_order, 
    cancel_order, 
    cancel_all_orders_api, 
    close_all_positions,
    get_open_position
)

load_dotenv()

# 2. Insert your Alpaca Paper Trading Credentials
AUTH_TOKEN = os.getenv("BROKER_API_KEY")
os.environ["BROKER_API_SECRET"] = os.getenv("BROKER_API_SECRET")

# Ensure Alpaca base URL points to Paper (https://paper-api.alpaca.markets)
os.environ["ALPACA_BASE_URL"] = "https://paper-api.alpaca.markets"

def run_tests():
    print("--- 1. Placing a Limit Order (AAPL) ---")
    limit_order_data = {
        "symbol": "AAPL",
        "exchange": "US",
        "action": "BUY",
        "quantity": "1",
        "pricetype": "LIMIT",
        "price": "100.00",  # Low price so it doesn't fill immediately
        "product": "CNC"
    }
    resp, resp_dict, order_id = place_order_api(limit_order_data, AUTH_TOKEN)
    print(f"Status Code: {resp.status_code}")
    pprint(resp_dict)
    
    print("\n--- 2. Fetching Order Book ---")
    time.sleep(2) # Give Alpaca a moment to register
    order_book = get_order_book(AUTH_TOKEN)
    print(f"Orders found today: {len(order_book)}")
    
    if order_id:
        print("\n--- 3. Modifying the Limit Order ---")
        modify_data = limit_order_data.copy()
        modify_data["orderid"] = order_id
        modify_data["price"] = "105.00"
        mod_resp, mod_status = modify_order(modify_data, AUTH_TOKEN)
        pprint(mod_resp)
        
        print("\n--- 4. Canceling the Limit Order ---")
        time.sleep(1)
        cancel_resp, cancel_status = cancel_order(order_id, AUTH_TOKEN)
        pprint(cancel_resp)

    print("\n--- 5. Placing a Market Order (AAPL) ---")
    market_order_data = {
        "symbol": "AAPL",
        "exchange": "US",
        "action": "BUY",
        "quantity": "1",
        "pricetype": "MARKET",
        "product": "CNC"
    }
    _, mkt_dict, mkt_order_id = place_order_api(market_order_data, AUTH_TOKEN)
    pprint(mkt_dict)
    
    print("\n--- 6. Checking Positions ---")
    time.sleep(4) # Market orders take a second to fill and reflect in positions
    positions = get_positions(AUTH_TOKEN)
    print(f"Open Positions: {len(positions)}")
    
    # Test our net position size helper
    net_qty = get_open_position("AAPL", "US", "CNC", AUTH_TOKEN)
    print(f"Net AAPL Position Size: {net_qty}")
    
    print("\n--- 7. Placing Multiple Limit Orders for Bulk Cancel Test ---")
    place_order_api(limit_order_data, AUTH_TOKEN)
    place_order_api(limit_order_data, AUTH_TOKEN)
    time.sleep(2)
    
    print("\n--- 8. Canceling All Open Orders ---")
    canceled_ids, failed = cancel_all_orders_api({}, AUTH_TOKEN)
    print(f"Canceled IDs: {canceled_ids}")
    
    print("\n--- 9. Liquidating All Open Positions ---")
    close_resp, close_status = close_all_positions(AUTH_TOKEN, AUTH_TOKEN)
    pprint(close_resp)
    
    print("\n✅ All Alpaca tests complete.")

if __name__ == "__main__":
    run_tests()