# broker/alpaca/api/margin_api.py

import json
from broker.alpaca.mapping.margin_data import transform_margin_positions
from utils.logging import get_logger

logger = get_logger(__name__)


def calculate_margin_api(positions, auth):
    """
    Calculate estimated margin requirement for a basket of positions for Alpaca.
    
    Note: Alpaca does not provide a pre-trade margin calculation endpoint like Indian brokers.
    This function estimates the initial margin based on standard US Reg T rules:
    - 50% for overnight equity positions (CNC)
    - 25% for intraday equity positions (MIS) assuming Pattern Day Trader status
    
    Args:
        positions: List of positions in OpenAlgo format
        auth: Authentication token for Alpaca
        
    Returns:
        Tuple of (response_shim, response_data)
    """
    transformed_positions = transform_margin_positions(positions)

    if not transformed_positions:
        error_response = {
            "status": "error",
            "message": "No valid positions to calculate margin.",
        }
        
        class MockResponse:
            status_code = 400
            status = 400

        return MockResponse(), error_response

    total_margin = 0
    
    for pos in transformed_positions:
        # If price is 0 (e.g., market order), we use 0 to avoid breaking. 
        # OpenAlgo passes the price for LIMIT orders.
        notional_value = pos["quantity"] * pos["price"]
        
        # Simple US Reg T approximation
        if pos["product"] == "MIS":
            # Intraday margin (up to 4x buying power)
            margin = notional_value * 0.25
        else:
            # Overnight Reg T margin (up to 2x buying power)
            margin = notional_value * 0.5
            
        total_margin += margin

    logger.info(f"[Alpaca] Estimated Reg T margin for {len(transformed_positions)} position(s): ${total_margin:,.2f}")

    class MockResponse:
        status_code = 200
        status = 200

    response_data = {
        "status": "success",
        "data": {
            "total_margin_required": total_margin,
            "span_margin": 0,  # SPAN is not applicable for US Equities
            "exposure_margin": total_margin,
        },
        "message": "Estimated Reg T margin (Alpaca does not have a native pre-trade margin API)"
    }

    return MockResponse(), response_data