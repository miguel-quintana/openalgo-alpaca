import requests
import json
import time
import threading
import websocket

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BASE_URL = "http://127.0.0.1:5000/api/v1"
WS_URL = "ws://127.0.0.1:8765" 
API_KEY = "db2551dd26829e4ff35ff106b2649e7cac18a957d44b98e29d4f0e375c889630"  # Replace with your OpenAlgo API Key

# ==============================================================================
# REST API HELPER
# ==============================================================================
def post_api(endpoint, payload=None):
    """OpenAlgo heavily utilizes POST to keep API keys secure in the JSON body."""
    if payload is None:
        payload = {}
        
    if isinstance(payload, dict):
        payload['apikey'] = API_KEY
        
    try:
        response = requests.post(f"{BASE_URL}{endpoint}", json=payload)
        return response.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ==============================================================================
# 1. REST API TESTS
# ==============================================================================
def run_rest_tests():
    print("\n" + "="*50)
    print("🚀 STARTING REST API TESTS (ALPACA)")
    print("="*50)

    # 1. Check Funds (Using POST)
    print("\n--- 1. Fetching Funds ---")
    funds = post_api("/funds")
    print(json.dumps(funds, indent=2))

# 2. Margin Calculator
    print("\n--- 2. Testing Margin Calculator ---")
    margin_payload = {
        "positions": [
            {
                "symbol": "AAPL",
                "exchange": "US",
                "action": "BUY",
                "quantity": 10,
                "price": 150.00,
                "product": "CNC",
                "pricetype": "LIMIT"
            }
        ]
    }
    margin_resp = post_api("/margin", payload=margin_payload)
    
    # 3. Place a Limit Order
    print("\n--- 3. Placing Limit Order (AAPL) ---")
    order_payload = {
        "symbol": "AAPL",
        "exchange": "US",
        "action": "BUY",
        "quantity": "1",
        "pricetype": "LIMIT",
        "price": "100.00",
        "product": "CNC",
        "strategy": "alpaca_test_script"
    }
    order_resp = post_api("/placeorder", payload=order_payload)
    print(json.dumps(order_resp, indent=2))
    
    order_id = order_resp.get("orderid")
    time.sleep(2)

    # 4. Fetch Orderbook (Using POST)
    print("\n--- 4. Fetching Orderbook ---")
    orderbook = post_api("/orderbook")
    print(json.dumps(orderbook, indent=2))
    
    # 5. Modify Order
    if order_id:
        print(f"\n--- 5. Modifying Order {order_id} ---")
        modify_payload = {
            "orderid": order_id,
            "price": "105.00",
            "quantity": "1",
            "exchange": "US",
            "symbol": "AAPL",
            "product": "CNC",
            "pricetype": "LIMIT",
            "action": "BUY",
            "strategy": "alpaca_test_script",
            "disclosed_quantity": "0",  # Required by OpenAlgo schema
            "trigger_price": "0"        # Required by OpenAlgo schema
        }
        modify_resp = post_api("/modifyorder", payload=modify_payload)
        print(json.dumps(modify_resp, indent=2))
        time.sleep(1)
        
        # 6. Cancel Order
        print(f"\n--- 6. Canceling Order {order_id} ---")
        cancel_payload = {
            "orderid": order_id,
            "strategy": "alpaca_test_script"
        }
        cancel_resp = post_api("/cancelorder", payload=cancel_payload)
        print(json.dumps(cancel_resp, indent=2))

    # 7. Check Positions (Using POST)
    print("\n--- 7. Fetching Open Positions ---")
    positions = post_api("/positionbook")
    print(json.dumps(positions, indent=2))

    # 8. Cancel All Open Orders
    print("\n--- 8. Canceling All Open Orders ---")
    cancel_all_payload = {"strategy": "alpaca_test_script"}
    cancel_all_resp = post_api("/cancelallorder", payload=cancel_all_payload)
    print(json.dumps(cancel_all_resp, indent=2))


# ==============================================================================
# 2. WEBSOCKET TESTS
# ==============================================================================
def on_ws_message(ws, message):
    print(f"\n[WS MESSAGE] {message}")

def on_ws_error(ws, error):
    print(f"\n[WS ERROR] {error}")

def on_ws_close(ws, close_status_code, close_msg):
    print("\n[WS CLOSED] Connection terminated.")

def on_ws_open(ws):
    print(f"\n[WS OPEN] Connected to {WS_URL}.")
    
    # Step 1: Send strict authentication action
    print("Sending authentication...")
    auth_payload = {
        "action": "authenticate",
        "apikey": API_KEY
    }
    ws.send(json.dumps(auth_payload))
    
# Step 2: Send subscription request
    time.sleep(1)
    print("Sending subscription for US:AAPL...")
    sub_payload = {
        "action": "subscribe",
        "symbols": [
            {
                "symbol": "AAPL",
                "exchange": "US"
            }
        ]
    }
    ws.send(json.dumps(sub_payload))
    
def run_websocket_tests():
    print("\n" + "="*50)
    print("🔌 STARTING WEBSOCKET TESTS")
    print("="*50)
    
    ws = websocket.WebSocketApp(WS_URL,
                              on_open=on_ws_open,
                              on_message=on_ws_message,
                              on_error=on_ws_error,
                              on_close=on_ws_close)

    ws_thread = threading.Thread(target=ws.run_forever)
    ws_thread.daemon = True
    ws_thread.start()

    time.sleep(6)
    print("\n[WS] Closing WebSocket after stream verification...")
    ws.close()

if __name__ == "__main__":
    if API_KEY == "YOUR_OPENALGO_API_KEY":
        print("❌ ERROR: Please update API_KEY before running the test.")
    else:
        run_rest_tests()
        run_websocket_tests()
        print("\n✅ ALL TESTS COMPLETED.")