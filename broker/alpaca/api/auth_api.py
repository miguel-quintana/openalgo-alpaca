# Developed and refined with assistance from Google's Gemini AI.

import os
from broker.alpaca.api.baseurl import BASE_URL, get_auth_headers, get_url
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)


def authenticate_broker(code):
    """
    Authenticate with Alpaca using API Key + Secret.

    Alpaca does NOT use an OAuth flow — credentials are provided once
    via environment variables. This function validates that both vars are
    present and includes KEY and Secret on the headers of the GET /v2/account call.

    Args:
        code: Not used for Alpaca (kept for interface compatibility).

    Returns:
        (api_key, None)         on success
        (None, error_message)   on failure
    """
    try:
        api_key = os.getenv("BROKER_API_KEY", "").strip()
        api_secret = os.getenv("BROKER_API_SECRET", "").strip()

        if not api_key:
            return None, "BROKER_API_KEY is not set in environment variables"
        if not api_secret:
            return None, "BROKER_API_SECRET is not set in environment variables"

        # Verify credentials with a live request to GET /v2/account
        path = "/v2/account"
        headers = get_auth_headers(
            method="GET",
            path=path,
            query_string="",
            payload="",
            api_key=api_key,
            api_secret=api_secret,
        )

        url = get_url(path)
        client = get_httpx_client()

        logger.info("Verifying Alpaca credentials via GET /v2/account")
        response = client.get(url, headers=headers)

        logger.debug(f"Profile response status: {response.status_code}")
        logger.debug(f"Profile response body: {response.text}")

        if response.status_code == 200:
            data = response.json()
            if data.get("id"):
                logger.info(
                    f"Alpaca authentication successful for account: "
                    f"{data.get('account_number', 'unknown')}"
                )
                return api_key, None
            else:
                error = data.get("message", {})
                msg = f"Alpaca API error: {error}"
                logger.error(msg)
                return None, msg

        elif response.status_code == 401:
            msg = "Invalid API key or signature — check BROKER_API_KEY and BROKER_API_SECRET"
            logger.error(msg)
            return None, msg

        else:
            msg = f"Unexpected HTTP {response.status_code} from Alpaca: {response.text}"
            logger.error(msg)
            return None, msg

    except Exception as e:
        msg = f"An exception occurred during Alpaca authentication: {str(e)}"
        logger.exception(msg)
        return None, msg


def get_auth_token():
    """
    Retrieve the active API key to be used as the auth token 
    for OpenAlgo execution and order services.
    """
    return os.getenv("BROKER_API_KEY", "").strip()