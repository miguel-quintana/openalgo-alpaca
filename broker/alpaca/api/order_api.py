# broker/alpaca/api/order_api.py
# Developed and refined with assistance from Google's Gemini AI.

import json
import os
import threading
import time
from datetime import datetime
import pytz

from broker.alpaca.api.baseurl import get_auth_headers, get_url
from broker.alpaca.mapping.transform_data import (
    map_exchange_type,
    map_product_type,
    transform_data,
    transform_modify_order_data,
)
from database.token_db import get_br_symbol, get_oa_symbol, get_symbol
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)


def get_api_response(endpoint, auth, method="GET", payload="", params=None):
    """
    Executes an authenticated HTTP request to Alpaca Trading API.

    Args:
        endpoint: REST route (e.g. "/v2/orders")
        auth: API Key (BROKER_API_KEY from OpenAlgo DB)
        method: HTTP Verb (GET, POST, PATCH, DELETE)
        payload: JSON body string
        params: Query string parameters

    Returns:
        Parsed JSON dictionary or list from Alpaca API.
    """
    api_secret = os.getenv("BROKER_API_SECRET", "")

    query_string = ""
    if params:
        query_string = "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))

    body = payload if payload else ""
    url = get_url(endpoint) + query_string

    headers = get_auth_headers(
        method=method.upper(),
        path=endpoint,
        query_string=query_string,
        payload=body,
        api_key=auth,
        api_secret=api_secret,
    )

    client = get_httpx_client()
    logger.debug(f"[Alpaca] {method.upper()} {url}")

    try:
        m = method.upper()
        if m == "GET":
            response = client.get(url, headers=headers)
        elif m == "POST":
            response = client.post(url, headers=headers, content=body)
        elif m == "PATCH":
            response = client.patch(url, headers=headers, content=body)
        elif m == "DELETE":
            response = client.request("DELETE", url, headers=headers, content=body)
        else:
            response = client.request(m, url, headers=headers, content=body)

        if response.status_code in (200, 201, 204, 207):
            if not response.text.strip():
                return {"status": "success"}
            return response.json()

        logger.error(f"[Alpaca] HTTP {response.status_code}: {response.text}")
        return {"status": "error", "message": response.text, "code": response.status_code}

    except Exception as e:
        logger.error(f"[Alpaca] Request failure on {endpoint}: {e}")
        return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# Order book / Trade book
# ---------------------------------------------------------------------------

def get_order_book(auth):
    """Fetch orders for today's trading session (New York Time)."""
    try:
        tz_ny = pytz.timezone("America/New_York")
        today_date = datetime.now(tz_ny).date()

        # Fetch open and recently closed orders from Alpaca
        orders = get_api_response("/v2/orders", auth, method="GET", params={"status": "all", "limit": "500"})
        if not isinstance(orders, list):
            return []

        today_orders = []
        for order in orders:
            created_at = order.get("created_at")
            if created_at:
                try:
                    dt_utc = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.UTC)
                    if dt_utc.astimezone(tz_ny).date() == today_date:
                        today_orders.append(order)
                except Exception as e:
                    logger.warning(f"[Alpaca] Date parsing error ({created_at}): {e}")

        return today_orders
    except Exception as e:
        logger.error(f"[Alpaca] Exception in get_order_book: {e}")
        return []


def get_trade_book(auth):
    """Fetch filled trade executions for today's session."""
    try:
        tz_ny = pytz.timezone("America/New_York")
        today_date = datetime.now(tz_ny).date()

        # Retrieve account fill activities
        fills = get_api_response("/v2/account/activities/FILL", auth, method="GET")
        if not isinstance(fills, list):
            return []

        today_trades = []
        for fill in fills:
            transaction_time = fill.get("transaction_time")
            if transaction_time:
                try:
                    dt_utc = datetime.strptime(transaction_time[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.UTC)
                    if dt_utc.astimezone(tz_ny).date() == today_date:
                        today_trades.append(fill)
                except Exception as e:
                    logger.warning(f"[Alpaca] Fill time parsing error ({transaction_time}): {e}")

        return today_trades
    except Exception as e:
        logger.error(f"[Alpaca] Exception in get_trade_book: {e}")
        return []


# ---------------------------------------------------------------------------
# Positions & Holdings
# ---------------------------------------------------------------------------

def get_positions(auth):
    """Fetch all open positions from Alpaca GET /v2/positions."""
    positions = get_api_response("/v2/positions", auth, method="GET")
    if isinstance(positions, list):
        return positions
    return []


def get_holdings(auth):
    """Alpaca unifies positions and equity holdings in GET /v2/positions."""
    return []


_symbol_locks = {}
_symbol_locks_lock = threading.Lock()
_position_cache = {}
_position_cache_lock = threading.Lock()
_POSITION_CACHE_TTL = 1.0


def _get_symbol_lock(symbol, exchange, product):
    key = f"{symbol}:{exchange}:{product}"
    with _symbol_locks_lock:
        if key not in _symbol_locks:
            _symbol_locks[key] = threading.Lock()
        return _symbol_locks[key]


def _get_cached_positions(auth):
    with _position_cache_lock:
        now = time.monotonic()
        cached = _position_cache.get(auth)
        if cached and (now - cached["timestamp"]) < _POSITION_CACHE_TTL:
            return cached["data"]

    positions_data = get_positions(auth)
    with _position_cache_lock:
        _position_cache[auth] = {"data": positions_data, "timestamp": time.monotonic()}
    return positions_data


def _invalidate_position_cache(auth):
    with _position_cache_lock:
        _position_cache.pop(auth, None)


def get_open_position(tradingsymbol, exchange, product, auth):
    """Return net position size (string) for symbol. Positive=Long, Negative=Short, '0'=Flat."""
    br_symbol = get_br_symbol(tradingsymbol, exchange) or tradingsymbol
    positions = _get_cached_positions(auth)

    if not isinstance(positions, list):
        return "0"

    for pos in positions:
        if isinstance(pos, dict) and pos.get("symbol") == br_symbol:
            qty = float(pos.get("qty", 0))
            side = pos.get("side", "long")
            return str(qty if side == "long" else -qty)

    return "0"


# ---------------------------------------------------------------------------
# Order placement & Execution
# ---------------------------------------------------------------------------

def place_order_api(data, auth):
    """
    Submit a new order to Alpaca via POST /v2/orders.

    Returns:
        (response_shim, response_dict, orderid)
    """
    transformed = transform_data(data)
    payload = json.dumps(transformed)

    result = get_api_response("/v2/orders", auth, method="POST", payload=payload)

    orderid = None
    if isinstance(result, dict) and result.get("id"):
        orderid = result.get("id")
        response_dict = {"orderid": orderid, "status": "success"}
        status_code = 200
    else:
        msg = result.get("message") if isinstance(result, dict) else "Order placement failed"
        response_dict = {"status": "error", "message": msg}
        status_code = 400

    class _Resp:
        def __init__(self, code):
            self.status_code = code
            self.status = code

    return _Resp(status_code), response_dict, orderid


def place_bracket_order_api(data, auth):
    """Convenience wrapper for bracket orders via place_order_api."""
    return place_order_api(data, auth)


def place_smartorder_api(data, auth):
    """Smart Order: Automatically aligns position to reach target position_size."""
    symbol = data.get("symbol")
    exchange = data.get("exchange")
    product = data.get("product")

    symbol_lock = _get_symbol_lock(symbol, exchange, product)

    with symbol_lock:
        target_size = float(data.get("position_size", "0"))
        current_position = float(
            get_open_position(symbol, exchange, map_product_type(product), auth)
        )

        if target_size == current_position:
            return None, {"status": "success", "message": "Position matches target size"}, None

        action = None
        quantity = 0

        if target_size == 0 and current_position > 0:
            action, quantity = "SELL", abs(current_position)
        elif target_size == 0 and current_position < 0:
            action, quantity = "BUY", abs(current_position)
        elif current_position == 0:
            action = "BUY" if target_size > 0 else "SELL"
            quantity = abs(target_size)
        elif target_size > current_position:
            action, quantity = "BUY", target_size - current_position
        elif target_size < current_position:
            action, quantity = "SELL", current_position - target_size

        if action:
            order_data = data.copy()
            order_data["action"] = action
            order_data["quantity"] = str(quantity)
            res = place_order_api(order_data, auth)
            _invalidate_position_cache(auth)
            return res

    return None, {"status": "success", "message": "No action required"}, None


# ---------------------------------------------------------------------------
# Order Cancellation & Modification
# ---------------------------------------------------------------------------

def cancel_order(orderid, auth):
    """Cancel an open order via DELETE /v2/orders/{order_id}."""
    endpoint = f"/v2/orders/{orderid}"
    result = get_api_response(endpoint, auth, method="DELETE")

    if isinstance(result, dict) and result.get("status") == "error":
        return {"status": "error", "message": result.get("message")}, 400

    return {"status": "success", "orderid": orderid}, 200


def cancel_all_orders_api(data, auth):
    """Bulk cancel all open orders via DELETE /v2/orders."""
    result = get_api_response("/v2/orders", auth, method="DELETE")

    if isinstance(result, list):
        cancelled_ids = [o.get("id") for o in result if isinstance(o, dict) and o.get("id")]
        return cancelled_ids, []

    return [], ["all"]


def modify_order(data, auth):
    """Modify open order via PATCH /v2/orders/{order_id}."""
    orderid = data["orderid"]
    transformed = transform_modify_order_data(data)
    payload = json.dumps(transformed)

    endpoint = f"/v2/orders/{orderid}"
    result = get_api_response(endpoint, auth, method="PATCH", payload=payload)

    if isinstance(result, dict) and result.get("id"):
        return {"status": "success", "orderid": orderid}, 200

    msg = result.get("message") if isinstance(result, dict) else "Modification failed"
    return {"status": "error", "message": msg}, 400


def close_all_positions(current_api_key, auth):
    """Liquidate all open positions using Alpaca native DELETE /v2/positions."""
    result = get_api_response("/v2/positions?cancel_orders=true", auth, method="DELETE")

    if isinstance(result, list):
        return {"status": "success", "message": "All open positions liquidated"}, 200

    return {"status": "error", "message": "Could not liquidate all positions"}, 400