# broker/alpaca/mapping/transform_data.py
# Mapping OpenAlgo API Request to Alpaca Trading API v2 Parameters
# Alpaca Docs: https://docs.alpaca.markets/reference/postorder
# Developed and refined with assistance from Google's Gemini AI.

from database.token_db import get_br_symbol, get_symbol_info
from utils.logging import get_logger

logger = get_logger(__name__)


def map_order_type(pricetype: str) -> str:
    """Maps OpenAlgo pricetype to Alpaca order type string."""
    mapping = {
        "MARKET": "market",
        "LIMIT": "limit",
        "SL": "stop_limit",
        "SL-M": "stop",
    }
    return mapping.get(str(pricetype).upper(), "market")


def map_time_in_force(validity: str) -> str:
    """Maps OpenAlgo validity parameter to Alpaca time_in_force string."""
    mapping = {
        "DAY": "day",
        "GTC": "gtc",
        "IOC": "ioc",
        "FOK": "fok",
    }
    return mapping.get(str(validity).upper(), "gtc")


def _format_qty(quantity, symbol, exchange):
    """
    Formats order quantity as float or int depending on asset specifications.
    Alpaca supports fractional shares for eligible US stocks and crypto.
    """
    try:
        qty_float = float(quantity)
        # If whole number, format as integer to satisfy non-fractional symbol constraints
        if qty_float.is_integer():
            return int(qty_float)
        return qty_float
    except (ValueError, TypeError):
        return 0


def transform_data(data: dict, token: str = None) -> dict:
    """
    Transforms OpenAlgo order request to Alpaca POST /v2/orders payload.

    Alpaca POST /v2/orders parameters:
        symbol          (str)  - Asset symbol e.g., "AAPL" or "BTC/USD"
        qty             (str/float) - Number of shares or units
        side            (str)  - "buy" or "sell"
        type            (str)  - "market" | "limit" | "stop" | "stop_limit" | "trailing_stop"
        time_in_force   (str)  - "day" | "gtc" | "ioc" | "fok"
        limit_price     (str)  - Required for limit / stop_limit orders
        stop_price      (str)  - Required for stop / stop_limit orders
        client_order_id (str)  - Optional custom order ID
        order_class     (str)  - "simple" | "bracket" | "oco" | "oto"
        take_profit     (dict) - {"limit_price": ...}
        stop_loss       (dict) - {"stop_price": ..., "limit_price": ...}
    """
    symbol = get_br_symbol(data["symbol"], data["exchange"]) or data["symbol"]
    side = data["action"].lower()  # "buy" or "sell"
    order_type = map_order_type(data["pricetype"])
    tif = map_time_in_force(data.get("validity", "GTC"))
    qty = _format_qty(data["quantity"], data["symbol"], data["exchange"])

    transformed = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side,
        "type": order_type,
        "time_in_force": tif,
    }

    # Limit and Stop prices
    price = str(data.get("price", "0"))
    trigger_price = str(data.get("trigger_price", "0"))

    if order_type in ("limit", "stop_limit") and price != "0":
        transformed["limit_price"] = price

    if order_type in ("stop", "stop_limit") and trigger_price != "0":
        transformed["stop_price"] = trigger_price

    # Trailing Stop orders
    if data.get("trail_amount"):
        transformed["type"] = "trailing_stop"
        transformed["trail_price"] = str(data["trail_amount"])

    # Client Order ID mapping
    if data.get("client_order_id"):
        transformed["client_order_id"] = str(data["client_order_id"])

    # Extended Hours trading flag
    if data.get("extended_hours") is True or data.get("product") == "MIS":
        transformed["extended_hours"] = True

    # Bracket Orders (Take-Profit / Stop-Loss legs)
    tp_price = data.get("bracket_take_profit_price")
    sl_trigger = data.get("bracket_stop_loss_price")
    sl_limit = data.get("bracket_stop_loss_limit_price")

    if tp_price or sl_trigger:
        transformed["order_class"] = "bracket"

        if tp_price:
            transformed["take_profit"] = {"limit_price": str(tp_price)}

        if sl_trigger:
            sl_payload = {"stop_price": str(sl_trigger)}
            if sl_limit:
                sl_payload["limit_price"] = str(sl_limit)
            transformed["stop_loss"] = sl_payload

    logger.debug(f"[Alpaca] Transformed order payload: {transformed}")
    return transformed


def transform_modify_order_data(data: dict) -> dict:
    """
    Transforms OpenAlgo modify order request to Alpaca PATCH /v2/orders/{order_id} payload.

    Alpaca PATCH /v2/orders/{order_id} accepts:
        qty             (str)  - Updated quantity
        time_in_force   (str)  - Updated TIF
        limit_price     (str)  - Updated limit price
        stop_price      (str)  - Updated stop price
        client_order_id (str)  - Updated custom ID
    """
    transformed = {}

    if data.get("quantity"):
        qty = _format_qty(data["quantity"], data.get("symbol"), data.get("exchange"))
        transformed["qty"] = str(qty)

    if data.get("price") and str(data["price"]) != "0":
        transformed["limit_price"] = str(data["price"])

    if data.get("trigger_price") and str(data["trigger_price"]) != "0":
        transformed["stop_price"] = str(data["trigger_price"])

    if data.get("validity"):
        transformed["time_in_force"] = map_time_in_force(data["validity"])

    if data.get("client_order_id"):
        transformed["client_order_id"] = str(data["client_order_id"])

    return transformed


def map_product_type(product: str) -> str:
    """Maps product types (CNC/NRML/MIS) to generic context."""
    return product


def reverse_map_product_type(br_product: str) -> str:
    """Default mapping for Alpaca positions."""
    return "CNC"


def map_exchange(br_exchange: str) -> str:
    """Maps Alpaca exchange identifiers to OpenAlgo exchange string."""
    return br_exchange or "US"


def map_exchange_type(exchange: str) -> str:
    """Maps OpenAlgo exchange identifiers to broker exchange context."""
    return exchange or "US"