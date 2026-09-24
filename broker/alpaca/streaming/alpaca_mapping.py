# Developed and refined with assistance from Google's Gemini AI.
"""
alpaca_mapping.py
Exchange / mode / capability mappings for Alpaca WebSocket adapter.
"""

class AlpacaExchangeMapper:
    """Maps OpenAlgo exchange codes to Alpaca equivalents."""

    EXCHANGE_SEGMENTS = {
        "US": "US",
        "CRYPTO": "CRYPTO",
        "OPRA": "OPRA",
    }

    @staticmethod
    def get_segment(exchange: str) -> str:
        return AlpacaExchangeMapper.EXCHANGE_SEGMENTS.get(exchange, "US")

    @staticmethod
    def get_channel_symbol(br_symbol: str) -> str:
        return br_symbol


class AlpacaModeMapper:
    """Maps OpenAlgo subscription mode integers to Alpaca channel names based on exchange."""

    @staticmethod
    def get_channels(mode: int, exchange: str = "US") -> tuple[str, ...]:
        ex = str(exchange).upper()
        if mode == 1:
            return ("trades",)
        elif mode == 2:
            return ("trades", "quotes", "bars")
        elif mode == 3:
            # US stocks do not support orderbooks; fallback to quotes/trades/bars
            if ex == "US":
                return ("trades", "quotes", "bars")
            # Crypto and supported segments can include orderbooks if applicable
            return ("trades", "quotes", "bars", "orderbooks")
        return ("trades", "quotes")

    @staticmethod
    def get_mode_str(mode: int) -> str:
        return {1: "LTP", 2: "QUOTE", 3: "DEPTH"}.get(mode, "LTP")


class AlpacaCapabilityRegistry:
    exchanges = ["US", "CRYPTO", "OPRA"]
    subscription_modes = [1, 2, 3]

    depth_support = {
        "US": [1, 2],  # US equities restricted to LTP/Quote modes (no native orderbook feed)
        "CRYPTO": [1, 5],
        "OPRA": [1, 5],
    }

    @classmethod
    def get_supported_depth_levels(cls, exchange: str) -> list:
        return cls.depth_support.get(exchange, [1])

    @classmethod
    def is_depth_level_supported(cls, exchange: str, depth_level: int) -> bool:
        return depth_level in cls.get_supported_depth_levels(exchange)

    @classmethod
    def get_fallback_depth_level(cls, exchange: str, requested_depth: int) -> int:
        supported = cls.get_supported_depth_levels(exchange)
        if requested_depth in supported:
            return requested_depth
        return max(supported)

    @classmethod
    def supports_mode(cls, mode: int) -> bool:
        return mode in cls.subscription_modes