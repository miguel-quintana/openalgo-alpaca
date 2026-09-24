# Alpaca Exchange API Base URL Configuration
# Developed and refined with assistance from Google's Gemini AI.

import hashlib
import hmac
import os
import time

# Base URL for Alpaca Exchange India REST API (Production).
# Override via ALPACA_BASE_URL env var to point at the testnet.
PAPER = os.getenv("ALPACA_PAPER", "True")
if PAPER.lower() == "true":
    BASE_URL = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")
    ROOT_URL = os.getenv("ALPACA_PAPER_ROOT_URL", "https://paper-api.alpaca.markets/v2/account")
else:
    BASE_URL = os.getenv("ALPACA_LIVE_BASE_URL", "https://api.alpaca.markets")
    ROOT_URL = os.getenv("ALPACA_LIVE_ROOT_URL", "https://api.alpaca.markets/v2/account")


def get_url(endpoint):
    """
    Constructs a full URL by combining the base URL and the endpoint.

    Args:
        endpoint (str): The API endpoint path (should start with '/')

    Returns:
        str: The complete URL
    """
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    return BASE_URL + endpoint


def generate_signature(api_secret: str, message: str) -> str:
    """
    Generate HMAC-SHA256 signature for Alpaca Exchange API requests.

    Args:
        api_secret: The API secret key
        message: Prehash string: METHOD + timestamp + path + query_string + body

    Returns:
        Hex-encoded HMAC-SHA256 digest
    """
    return hmac.new(
        bytes(api_secret, "utf-8"),
        bytes(message, "utf-8"),
        hashlib.sha256,
    ).hexdigest()


def get_auth_headers(
    method: str,
    path: str,
    query_string: str = "",
    payload: str = "",
    api_key: str = None,
    api_secret: str = None,
) -> dict:
    """
    Build signed authentication headers for a Alpaca Exchange API request.

    Signature prehash: METHOD + timestamp + path + query_string + payload
    Note: query_string must include the leading '?' when present,
          e.g. '?product_id=27&state=open'

    Args:
        method:       HTTP method in uppercase (GET, POST, DELETE, ...)
        path:         Endpoint path, e.g. '/v2/orders'
        query_string: Raw query string including '?' prefix, or '' if none
        payload:      Request body as a JSON string, or '' for GET requests
        api_key:      API key override (falls back to BROKER_API_KEY env var)
        api_secret:   API secret override (falls back to BROKER_API_SECRET env var)

    Returns:
        dict of headers ready to pass to httpx / requests
    """
    key = api_key or os.getenv("BROKER_API_KEY", "")
    secret = api_secret or os.getenv("BROKER_API_SECRET", "")

    timestamp = str(int(time.time()))
    signature_data = method.upper() + timestamp + path + query_string + payload
    signature = generate_signature(secret, signature_data)

    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "timestamp": timestamp,
        "signature": signature,
        "User-Agent": "openalgo-python-client",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
