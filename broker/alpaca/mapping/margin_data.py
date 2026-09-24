# broker/alpaca/mapping/margin_data.py

from database.token_db import get_br_symbol
from utils.logging import get_logger

logger = get_logger(__name__)


def transform_margin_positions(positions):
    """
    Transform OpenAlgo margin position format for Alpaca local estimation.

    Args:
        positions: List of positions in OpenAlgo format

    Returns:
        List of simplified dictionaries for margin calculation
    """
    transformed_positions = []
    skipped_positions = []

    for position in positions:
        try:
            symbol = position.get("symbol")
            exchange = position.get("exchange")

            # Get the broker symbol for Alpaca, fallback to raw symbol
            br_symbol = get_br_symbol(symbol, exchange) or symbol

            # Extract the raw values needed for a Reg T estimation
            transformed_position = {
                "symbol": br_symbol,
                "exchange": exchange,
                "action": position.get("action", "BUY").upper(),
                "product": position.get("product", "CNC"),
                "quantity": int(position.get("quantity", 0)),
                "price": float(position.get("price", 0))
            }

            transformed_positions.append(transformed_position)

        except Exception as e:
            logger.error(f"[Alpaca] Error transforming margin position: {position}, Error: {e}")
            skipped_positions.append(f"{position.get('symbol', 'unknown')} - Error: {str(e)}")
            continue

    if skipped_positions:
        logger.warning(
            f"[Alpaca] Skipped {len(skipped_positions)} position(s) in margin calc: {', '.join(skipped_positions)}"
        )

    return transformed_positions