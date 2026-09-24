# Developed and refined with assistance from Google's Gemini AI.

import json

from utils.logging import get_logger

logger = get_logger(__name__)


def get_exchange_segment(asset_class: str) -> str:
    """Helper to map Alpaca's asset_class to OpenAlgo exchange segments."""
    ac = str(asset_class).lower() if asset_class else ""
    if ac == "crypto":
        return "CRYPTO"
    elif ac == "us_option":
        return "OPRA"
    return "US"


def map_order_data(order_data):
    """
    Normalises a list of Alpaca order dicts to the OpenAlgo internal format.

    Alpaca order fields used:
        id               – raw UUID order ID
        symbol           – asset symbol (e.g. "AAPL", "AAPL230616C00150000")
        asset_class      – "us_equity" | "us_option" | "crypto"
        side             – "buy" | "sell"
        type             – "market" | "limit" | "stop" | "stop_limit" | "trailing_stop"
        status           – "new" | "filled" | "canceled" | "rejected" | etc.
        qty              – total order size (float string)
        filled_qty       – filled quantity
        limit_price      – limit price (float string)
        stop_price       – stop trigger price (float string, optional)
        updated_at       – last update timestamp
        created_at       – creation timestamp
    """
    try:
        if order_data is None:
            return []
        if isinstance(order_data, dict) and "message" in order_data:
            logger.error(f"Error in order data: {order_data.get('message', 'Unknown error')}")
            return []
        if isinstance(order_data, str):
            logger.error(f"Received string instead of order list: {order_data[:200]}")
            return []
        if not isinstance(order_data, list):
            logger.warning(f"Expected list, got {type(order_data)}")
            return []

        for order in order_data:
            if not isinstance(order, dict):
                continue

            order["orderId"] = str(order.get("id", ""))
            order["tradingSymbol"] = order.get("symbol", "")

            # Exchange and product type
            order["exchangeSegment"] = get_exchange_segment(order.get("asset_class"))
            order["productType"] = "CNC"

            # Transaction type
            order["transactionType"] = order.get("side", "").upper()  # BUY / SELL

            # Order type mapping
            raw_ot = order.get("type", "").lower()
            if raw_ot == "stop_limit":
                order["orderType"] = "SL"
            elif raw_ot == "stop":
                order["orderType"] = "SL-M"
            elif raw_ot == "trailing_stop":
                order["orderType"] = "TRAILING_STOP"
            else:
                order["orderType"] = raw_ot.upper()

            # Status mapping
            state = order.get("status", "").lower()
            if state in [
                "new",
                "accepted",
                "pending_new",
                "accepted_for_bidding",
                "calculated",
                "partially_filled",
                "pending_replace",
                "suspended",
            ]:
                order["orderStatus"] = "open"
            elif state in ["filled", "done_for_day"]:
                order["orderStatus"] = "complete"
            elif state in ["canceled", "pending_cancel", "expired", "replaced"]:
                order["orderStatus"] = "cancelled"
            elif state in ["rejected", "stopped"]:
                order["orderStatus"] = "rejected"
            else:
                order["orderStatus"] = state

            # Numeric fields
            order["quantity"] = float(order.get("qty") or 0.0)
            order["filledQuantity"] = float(order.get("filled_qty") or 0.0)
            order["price"] = float(order.get("limit_price") or 0.0)
            order["triggerPrice"] = float(order.get("stop_price") or 0.0)
            order["averagePrice"] = float(order.get("filled_avg_price") or 0.0)

            order["updateTime"] = order.get("updated_at") or order.get("created_at", "")
            order["clientOrderId"] = order.get("client_order_id", "")

        return order_data

    except Exception as e:
        logger.error(f"Exception in map_order_data: {e}")
        return []


def calculate_order_statistics(order_data):
    """
    Calculates statistics from order data, including totals for buy orders, sell orders,
    completed orders, open orders, and rejected orders.
    """
    try:
        total_buy_orders = total_sell_orders = 0
        total_completed_orders = total_open_orders = total_rejected_orders = 0

        if not order_data or not isinstance(order_data, list):
            return {
                "total_buy_orders": 0,
                "total_sell_orders": 0,
                "total_completed_orders": 0,
                "total_open_orders": 0,
                "total_rejected_orders": 0,
            }

        for order in order_data:
            if not isinstance(order, dict):
                continue

            if order.get("transactionType") == "BUY":
                total_buy_orders += 1
            elif order.get("transactionType") == "SELL":
                total_sell_orders += 1

            status = order.get("orderStatus", "").lower()
            if status in ("complete", "closed", "filled"):
                total_completed_orders += 1
                order["orderStatus"] = "complete"
            elif status in ("open", "pending", "partially_filled"):
                total_open_orders += 1
                order["orderStatus"] = "open"
            elif status == "rejected":
                total_rejected_orders += 1
                order["orderStatus"] = "rejected"
            elif status == "cancelled":
                order["orderStatus"] = "cancelled"

        return {
            "total_buy_orders": total_buy_orders,
            "total_sell_orders": total_sell_orders,
            "total_completed_orders": total_completed_orders,
            "total_open_orders": total_open_orders,
            "total_rejected_orders": total_rejected_orders,
        }
    except Exception as e:
        logger.error(f"Exception in calculate_order_statistics: {e}")
        return {
            "total_buy_orders": 0,
            "total_sell_orders": 0,
            "total_completed_orders": 0,
            "total_open_orders": 0,
            "total_rejected_orders": 0,
        }


def transform_order_data(orders):
    try:
        if orders is None:
            return []

        if isinstance(orders, dict):
            orders = [orders]

        if not isinstance(orders, list):
            logger.warning(f"Expected list or dict but got {type(orders)}")
            return []

        transformed_orders = []

        for order in orders:
            if not isinstance(order, dict):
                continue

            order_type = order.get("orderType", "").upper()
            if order_type == "MARKET":
                order["orderType"] = "MARKET"
            elif order_type == "LIMIT":
                order["orderType"] = "LIMIT"
            elif order_type == "STOP_LOSS":
                order["orderType"] = "SL"
            elif order_type == "STOP_LOSS_MARKET":
                order["orderType"] = "SL-M"
            elif order_type == "OCO":
                order["orderType"] = "OCO"

            transformed_order = {
                "symbol": order.get("tradingSymbol", ""),
                "exchange": order.get("exchangeSegment", ""),
                "action": order.get("transactionType", ""),
                "quantity": order.get("quantity", 0),
                "price": order.get("price", 0.0),
                "trigger_price": order.get("triggerPrice", 0.0),
                "pricetype": order.get("orderType", ""),
                "product": order.get("productType", ""),
                "orderid": order.get("orderId", ""),
                "order_status": order.get("orderStatus", ""),
                "timestamp": order.get("updateTime", ""),
            }

            transformed_orders.append(transformed_order)

        return transformed_orders
    except Exception as e:
        logger.error(f"Exception in transform_order_data: {e}")
        return []


def map_trade_data(trade_data):
    """
    Normalises a list of Alpaca Account Activities (FILL) dicts to the OpenAlgo internal format.
    """
    try:
        if trade_data is None:
            logger.info("No trade data available.")
            return []

        if isinstance(trade_data, dict) and "message" in trade_data:
            logger.error(f"Error in trade data: {trade_data.get('message', 'Unknown error')}")
            return []

        if isinstance(trade_data, str):
            logger.error(f"Received string response instead of trade data: {trade_data[:200]}...")
            return []

        if not isinstance(trade_data, list):
            logger.warning(f"Expected list but got {type(trade_data)}: {trade_data}")
            return []

        for trade in trade_data:
            if not isinstance(trade, dict):
                continue

            trade["tradingSymbol"] = trade.get("symbol", "")
            trade["exchangeSegment"] = get_exchange_segment(trade.get("asset_class"))
            trade["productType"] = "CNC"
            trade["orderId"] = str(trade.get("order_id", ""))

            trade["tradedQuantity"] = float(trade.get("qty") or 0.0)
            trade["tradedPrice"] = float(trade.get("price") or 0.0)
            trade["transactionType"] = trade.get("side", "").upper()
            trade["updateTime"] = trade.get("transaction_time", "")

        return trade_data

    except Exception as e:
        logger.error(f"Exception in map_trade_data: {e}")
        return []


def transform_tradebook_data(tradebook_data):
    """
    Transform Alpaca fill/trade data to OpenAlgo standard format.
    """
    try:
        if tradebook_data is None:
            return []

        if not isinstance(tradebook_data, list):
            logger.warning(f"Expected list but got {type(tradebook_data)}")
            return []

        transformed_data = []
        for trade in tradebook_data:
            if not isinstance(trade, dict):
                continue

            try:
                quantity = float(trade.get("tradedQuantity", 0) or 0)
            except (TypeError, ValueError):
                quantity = 0.0

            try:
                price = float(trade.get("tradedPrice", 0.0) or 0.0)
            except (TypeError, ValueError):
                price = 0.0

            transformed_trade = {
                "symbol": trade.get("tradingSymbol", ""),
                "exchange": trade.get("exchangeSegment", ""),
                "product": trade.get("productType", ""),
                "action": trade.get("transactionType", ""),
                "quantity": quantity,
                "average_price": price,
                "trade_value": quantity * price,
                "orderid": trade.get("orderId", ""),
                "timestamp": trade.get("updateTime", ""),
            }
            transformed_data.append(transformed_trade)
        return transformed_data
    except Exception as e:
        logger.error(f"Exception in transform_tradebook_data: {e}")
        return []


def map_position_data(position_data):
    """
    Normalises a list of Alpaca position dicts to the OpenAlgo internal format.
    """
    try:
        if position_data is None:
            logger.info("No position data available.")
            return []

        if isinstance(position_data, dict) and "message" in position_data:
            logger.error(f"Error in position data: {position_data.get('message', 'Unknown error')}")
            return []

        if isinstance(position_data, str):
            logger.error(f"Received string response instead of position data: {position_data[:200]}...")
            return []

        if not isinstance(position_data, list):
            logger.warning(f"Unexpected position data format: {type(position_data)}")
            return []

        processed_positions = []

        for position in position_data:
            if not isinstance(position, dict):
                continue

            position["tradingSymbol"] = position.get("symbol", "")
            position["exchangeSegment"] = get_exchange_segment(position.get("asset_class"))
            position["productType"] = "CNC"

            try:
                net_qty = float(position.get("qty", 0))
            except (ValueError, TypeError):
                net_qty = 0.0
            position["netQty"] = net_qty

            position["avgCostPrice"] = float(position.get("avg_entry_price") or 0.0)
            position["lastTradedPrice"] = float(position.get("current_price") or 0.0)
            position["marketValue"] = float(position.get("market_value") or 0.0)
            position["pnlAbsolute"] = float(position.get("unrealized_pl") or 0.0)

            position["lot_size"] = 1.0
            position["multiplier"] = 1
            position["positionType"] = "open" if net_qty != 0 else "closed"

            processed_positions.append(position)

        return processed_positions

    except Exception as e:
        logger.error(f"Exception in map_position_data: {e}")
        return []


def transform_positions_data(positions_data):
    """
    Transform positions data to OpenAlgo standard format.
    """
    try:
        if positions_data is None:
            return []

        if not isinstance(positions_data, list):
            logger.warning(f"Expected list but got {type(positions_data)}")
            return []

        transformed_data = []
        for position in positions_data:
            if not isinstance(position, dict):
                continue

            transformed_position = {
                "symbol": position.get("tradingSymbol", ""),
                "exchange": position.get("exchangeSegment", ""),
                "product": position.get("productType", ""),
                "quantity": position.get("netQty", 0),
                "average_price": float(position.get("avgCostPrice", 0.0)),
                "ltp": float(position.get("lastTradedPrice", 0.0)),
                "pnl": float(position.get("pnlAbsolute", 0.0)),
                "lot_size": float(position.get("lot_size", 1.0)),
            }
            transformed_data.append(transformed_position)
        return transformed_data
    except Exception as e:
        logger.error(f"Exception in transform_positions_data: {e}")
        return []


def transform_holdings_data(holdings_data):
    try:
        if holdings_data is None:
            return []

        if not isinstance(holdings_data, list):
            logger.warning(f"Expected list but got {type(holdings_data)}")
            return []

        transformed_data = []
        for holding in holdings_data:
            if not isinstance(holding, dict):
                continue

            transformed_holding = {
                "symbol": holding.get("tradingSymbol", holding.get("symbol", "")),
                "exchange": holding.get("exchangeSegment", "US"),
                "quantity": holding.get("totalQty", holding.get("qty", 0)),
                "product": "CNC",
                "pnl": holding.get("pnlAbsolute", 0.0),
                "pnlpercent": holding.get("pnlPercent", 0.0),
            }
            transformed_data.append(transformed_holding)
        return transformed_data
    except Exception as e:
        logger.error(f"Exception in transform_holdings_data: {e}")
        return []


def map_portfolio_data(portfolio_data):
    """
    Processes and modifies a list of Portfolio dictionaries based on specific conditions.
    """
    try:
        if portfolio_data is None:
            logger.info("No portfolio data available.")
            return []

        if isinstance(portfolio_data, dict) and "message" in portfolio_data:
            logger.error(f"Error in portfolio data: {portfolio_data.get('message', 'Unknown error')}")
            return []

        if isinstance(portfolio_data, str):
            logger.error(f"Received string response instead of portfolio data: {portfolio_data[:200]}...")
            return []

        if not isinstance(portfolio_data, list):
            logger.warning(f"Expected list but got {type(portfolio_data)}: {portfolio_data}")
            return []

        if portfolio_data:
            for holding in portfolio_data:
                if not isinstance(holding, dict):
                    continue

                holding["tradingSymbol"] = holding.get("symbol", "")
                holding["exchangeSegment"] = get_exchange_segment(holding.get("asset_class"))
                holding["totalQty"] = float(holding.get("qty") or 0.0)
                holding["avgCostPrice"] = float(holding.get("avg_entry_price") or 0.0)
                holding["lastTradedPrice"] = float(holding.get("current_price") or 0.0)
                holding["marketValue"] = float(holding.get("market_value") or 0.0)
                holding["pnlAbsolute"] = float(holding.get("unrealized_pl") or 0.0)
                holding["pnlPercent"] = float(holding.get("unrealized_plpc") or 0.0) * 100

        return portfolio_data

    except Exception as e:
        logger.error(f"Exception in map_portfolio_data: {e}")
        return []


def calculate_portfolio_statistics(holdings_data):
    try:
        if not holdings_data or not isinstance(holdings_data, list):
            return {
                "totalholdingvalue": 0.0,
                "totalinvvalue": 0.0,
                "totalprofitandloss": 0.0,
                "totalpnlpercentage": 0.0,
            }

        totalholdingvalue = 0.0
        totalinvvalue = 0.0
        totalprofitandloss = 0.0

        for holding in holdings_data:
            if not isinstance(holding, dict):
                continue

            total_qty = holding.get("totalQty", 0.0)
            avg_price = holding.get("avgCostPrice", 0.0)
            market_price = holding.get("lastTradedPrice", avg_price)

            investment_value = total_qty * avg_price
            market_value = total_qty * market_price
            pnl = market_value - investment_value

            totalholdingvalue += market_value
            totalinvvalue += investment_value
            totalprofitandloss += pnl

        totalpnlpercentage = (
            (totalprofitandloss / totalinvvalue * 100) if totalinvvalue > 0 else 0.0
        )

        return {
            "totalholdingvalue": round(totalholdingvalue, 2),
            "totalinvvalue": round(totalinvvalue, 2),
            "totalprofitandloss": round(totalprofitandloss, 2),
            "totalpnlpercentage": round(totalpnlpercentage, 2),
        }
    except Exception as e:
        logger.error(f"Exception in calculate_portfolio_statistics: {e}")
        return {
            "totalholdingvalue": 0.0,
            "totalinvvalue": 0.0,
            "totalprofitandloss": 0.0,
            "totalpnlpercentage": 0.0,
        }