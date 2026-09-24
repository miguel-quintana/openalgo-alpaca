# api/funds.py
# Alpaca wallet balance → OpenAlgo margin format
# Endpoints:
#   GET /v2/account     → available cash, blocked margin
#   GET /v2/positions   → unrealized PnL across all open positions
#
# Note: Alpaca's REST API does not expose a single field for daily session realized P&L 
# on the account or open positions endpoints. 
# Developed and refined with assistance from Google's Gemini AI.

import os

from broker.alpaca.api.baseurl import BASE_URL, get_auth_headers
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_MARGIN_RESPONSE = {
    "availablecash": "0.00",
    "collateral": "0.00",
    "m2mrealized": "0.00",
    "m2munrealized": "0.00",
    "utiliseddebits": "0.00",
}


def _f(value):
    """Safe float conversion from string or number."""
    try:
        return float(value or 0)
    except (ValueError, TypeError):
        return 0.0


def _get_positions_pnl(api_key, api_secret):
    """
    Fetch open positions and return (total_realized_pnl, total_unrealized_pnl).
    Alpaca's /v2/positions endpoint returns a list of active position objects directly.
    """
    path = "/v2/positions"
    url = BASE_URL + path
    try:
        headers = get_auth_headers(
            method="GET",
            path=path,
            query_string="",
            payload="",
            api_key=api_key,
            api_secret=api_secret,
        )
        client = get_httpx_client()
        response = client.get(url, headers=headers, timeout=30.0)
        
        if response.status_code != 200:
            logger.warning(
                f"[Alpaca] positions HTTP {response.status_code}: "
                f"{response.text[:200]}"
            )
            return 0.0, 0.0
            
        positions = response.json()
        
        # Alpaca directly returns a list, not a wrapped "result" object
        if not isinstance(positions, list):
            logger.warning("[Alpaca] positions API did not return a list.")
            return 0.0, 0.0
            
        # Alpaca uses 'unrealized_pl' for open positions
        unrealized = sum(
            _f(p.get("unrealized_pl")) for p in positions if isinstance(p, dict)
        )
        
        # Realized P&L is not tracked for open positions via this endpoint
        realized = 0.0 
        
        return realized, unrealized
    except Exception as e:
        logger.warning(f"[Alpaca] Could not fetch positions P&L: {e}")
        return 0.0, 0.0


def get_margin_data(auth_token):
    """
    Fetch wallet balance from Alpaca and return it in OpenAlgo margin format.

    Endpoint: GET /v2/account
    Authentication: "APCA-API-KEY-ID" and "APCA-API-SECRET-KEY" in headers

    Alpaca account object fields used:
        cash            – free balance, immediately tradeable
        initial_margin  – total margin locked by open positions

    P&L is sourced from /v2/positions:
        m2munrealized  ← sum of unrealized_pl across all open positions

    OpenAlgo field mapping:
        availablecash  ← cash
        collateral     ← 0.00 (not applicable for Alpaca in this context)
        utiliseddebits ← initial_margin

    Args:
        auth_token (str): api_key stored in OpenAlgo auth DB after login.

    Returns:
        dict: OpenAlgo standard margin dict, or DEFAULT_MARGIN_RESPONSE on failure.
    """
    api_key = auth_token
    api_secret = os.getenv("BROKER_API_SECRET", "")

    if not api_key or not api_secret:
        logger.error("[Alpaca] BROKER_API_KEY / BROKER_API_SECRET not set")
        return DEFAULT_MARGIN_RESPONSE

    path = "/v2/account"
    url = BASE_URL + path

    try:
        headers = get_auth_headers(
            method="GET",
            path=path,
            query_string="",
            payload="",
            api_key=api_key,
            api_secret=api_secret,
        )

        client = get_httpx_client()
        response = client.get(url, headers=headers, timeout=30.0)

        if response.status_code != 200:
            logger.error(
                f"[Alpaca] account HTTP {response.status_code}: "
                f"{response.text[:200]}"
            )
            return DEFAULT_MARGIN_RESPONSE

        data = response.json()
        logger.debug("[Alpaca] account response received")

        # Alpaca /v2/account responds with the account dictionary directly 
        if not isinstance(data, dict) or "id" not in data:
            logger.error("[Alpaca] Unexpected account response format")
            return DEFAULT_MARGIN_RESPONSE

        # Map Alpaca fields to OpenAlgo standard
        available_cash = _f(data.get("cash", 0))
        blocked_margin = _f(data.get("initial_margin", 0))
        collateral = 0.0

        # P&L comes from positions
        total_realized_pnl, total_unrealized_pnl = _get_positions_pnl(api_key, api_secret)

        result = {
            "availablecash": f"{available_cash:.2f}",
            "collateral": f"{collateral:.2f}",
            "m2mrealized": f"{total_realized_pnl:.2f}",
            "m2munrealized": f"{total_unrealized_pnl:.2f}",
            "utiliseddebits": f"{blocked_margin:.2f}",
        }

        logger.debug(
            f"[Alpaca] Wallet: available={result['availablecash']} "
            f"blocked={result['utiliseddebits']} "
            f"realized={result['m2mrealized']} unrealized={result['m2munrealized']}"
        )
        return result

    except Exception as e:
        logger.error(f"[Alpaca] Unexpected error in get_margin_data: {e}", exc_info=True)
        return DEFAULT_MARGIN_RESPONSE