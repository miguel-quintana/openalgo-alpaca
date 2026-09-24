# broker/alpaca/api/data.py
# Alpaca Market Data API — Equities, Crypto, and Options
# Reference: https://docs.alpaca.markets/docs/market-data-api
# Developed and refined with assistance from Google's Gemini AI.

import os
from datetime import datetime, timezone

import httpx
import pandas as pd

from database.token_db import get_br_symbol
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)

DATA_URL = "https://data.alpaca.markets"


def _f(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def _i(value, default=0):
    try:
        return int(float(value)) if value is not None else default
    except (ValueError, TypeError):
        return default


class BrokerData:
    """
    Alpaca Market Data provider for US Equities, Crypto, and Options.
    """

    TIMEFRAME_MAP = {
        "1m":  "1Min", "3m":  "3Min", "5m":  "5Min", "15m": "15Min",
        "30m": "30Min", "1h":  "1Hour", "2h":  "2Hour", "4h":  "4Hour",
        "1d":  "1Day", "D":   "1Day", "1w":  "1Week", "W":   "1Week",
    }

    def __init__(self, auth_token: str):
        self.auth_token = auth_token
        self.api_secret = os.getenv("BROKER_API_SECRET", "")
        self.timeframe_map = self.TIMEFRAME_MAP

    def _get_headers(self):
        return {
            "APCA-API-KEY-ID": self.auth_token,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Accept": "application/json"
        }

    def _get_asset_route(self, exchange: str, br_symbol: str) -> dict:
        """Determines the correct Alpaca API routing based on the exchange."""
        exch = str(exchange).upper()
        
        # Heuristic fallback: Option contracts are typically 15+ chars long (e.g., AAPL250117C00150000)
        is_option = exch == "OPRA" or len(br_symbol) > 14
        
        if exch == "CRYPTO":
            return {
                "snapshot": f"{DATA_URL}/v1beta3/crypto/us/snapshots?symbols={br_symbol}",
                "bars": f"{DATA_URL}/v1beta3/crypto/us/bars",
                "is_plural_snap": True
            }
        elif is_option:
            return {
                "snapshot": f"{DATA_URL}/v1beta1/options/snapshots?symbols={br_symbol}",
                "bars": f"{DATA_URL}/v1beta1/options/bars",
                "is_plural_snap": True
            }
        else:
            # Default to US Equities
            return {
                "snapshot": f"{DATA_URL}/v2/stocks/{br_symbol}/snapshot",
                "bars": f"{DATA_URL}/v2/stocks/bars", 
                "is_plural_snap": False
            }

    def _fetch_snapshot(self, symbol: str, exchange: str) -> dict:
        """Fetches and normalizes the snapshot response across all asset classes."""
        br_symbol = get_br_symbol(symbol, exchange) or symbol
        route = self._get_asset_route(exchange, br_symbol)
        
        client = get_httpx_client()
        resp = client.get(route["snapshot"], headers=self._get_headers(), timeout=10.0)
        
        if resp.status_code != 200:
            logger.error(f"[Alpaca] Snapshot HTTP {resp.status_code}: {resp.text}")
            return {}
            
        data = resp.json()
        
        # Crypto and Options use a plural wrapper: {"snapshots": {"BTC/USD": {...}}}
        if route["is_plural_snap"]:
            return data.get("snapshots", {}).get(br_symbol, {})
            
        # Equities return the object directly
        return data

    def _empty_quote(self):
        return {
            "ltp": 0.0, "open": 0.0, "high": 0.0, "low": 0.0,
            "volume": 0, "prev_close": 0.0, "oi": 0.0,
            "bid": 0.0, "ask": 0.0,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Quotes & Depth
    # ──────────────────────────────────────────────────────────────────────────

    def get_quotes(self, symbol: str, exchange: str) -> dict:
        """Fetch real-time snapshot for Equities, Crypto, or Options."""
        try:
            snapshot = self._fetch_snapshot(symbol, exchange)
            if not snapshot:
                return self._empty_quote()
            
            latest_trade = snapshot.get("latestTrade", {})
            latest_quote = snapshot.get("latestQuote", {})
            daily_bar = snapshot.get("dailyBar", {})
            prev_daily_bar = snapshot.get("prevDailyBar", {})
            
            return {
                "ltp":        _f(latest_trade.get("p", 0)),
                "open":       _f(daily_bar.get("o", 0)),
                "high":       _f(daily_bar.get("h", 0)),
                "low":        _f(daily_bar.get("l", 0)),
                "volume":     _i(daily_bar.get("v", 0)),
                "prev_close": _f(prev_daily_bar.get("c", 0)),
                "oi":         _f(snapshot.get("openInterest", 0)), # Options specific
                "bid":        _f(latest_quote.get("bp", 0)),
                "ask":        _f(latest_quote.get("ap", 0)),
            }
        except Exception as e:
            logger.error(f"[Alpaca] get_quotes error for {symbol}: {e}")
            return self._empty_quote()

    def get_depth(self, symbol: str, exchange: str) -> dict:
        """Synthesizes a 5-level market depth structure for OpenAlgo UI."""
        try:
            snapshot = self._fetch_snapshot(symbol, exchange)
            
            def _empty_levels():
                return [{"price": 0.0, "quantity": 0} for _ in range(5)]
                
            if not snapshot:
                return {
                    "bids": _empty_levels(), "asks": _empty_levels(),
                    "ltp": 0.0, "ltq": 0, "volume": 0, "open": 0.0,
                    "high": 0.0, "low": 0.0, "prev_close": 0.0, "oi": 0.0,
                    "totalbuyqty": 0, "totalsellqty": 0,
                }
                
            latest_trade = snapshot.get("latestTrade", {})
            latest_quote = snapshot.get("latestQuote", {})
            daily_bar = snapshot.get("dailyBar", {})
            prev_daily_bar = snapshot.get("prevDailyBar", {})
            
            bids = _empty_levels()
            asks = _empty_levels()
            
            bids[0] = {"price": _f(latest_quote.get("bp", 0)), "quantity": _i(latest_quote.get("bs", 0))}
            asks[0] = {"price": _f(latest_quote.get("ap", 0)), "quantity": _i(latest_quote.get("as", 0))}
            
            return {
                "bids": bids,
                "asks": asks,
                "ltp":          _f(latest_trade.get("p", 0)),
                "ltq":          _i(latest_trade.get("s", 0)),
                "volume":       _i(daily_bar.get("v", 0)),
                "open":         _f(daily_bar.get("o", 0)),
                "high":         _f(daily_bar.get("h", 0)),
                "low":          _f(daily_bar.get("l", 0)),
                "prev_close":   _f(prev_daily_bar.get("c", 0)),
                "oi":           _f(snapshot.get("openInterest", 0)),
                "totalbuyqty":  bids[0]["quantity"],
                "totalsellqty": asks[0]["quantity"],
            }
        except Exception as e:
            logger.error(f"[Alpaca] get_depth error for {symbol}: {e}")
            raise Exception(f"Error fetching market depth for {symbol}: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # History
    # ──────────────────────────────────────────────────────────────────────────

    def get_history(self, symbol: str, exchange: str, interval: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch OHLCV historical candles uniformly across all asset classes."""
        try:
            if interval not in self.TIMEFRAME_MAP:
                raise Exception(f"Unsupported interval '{interval}'")
                
            resolution = self.TIMEFRAME_MAP[interval]
            br_symbol = get_br_symbol(symbol, exchange) or symbol
            route = self._get_asset_route(exchange, br_symbol)
            
            from datetime import time as _time
            if isinstance(start_date, str):
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                start_dt = datetime.combine(start_date, _time.min).replace(tzinfo=timezone.utc)

            if isinstance(end_date, str):
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
            else:
                end_dt = datetime.combine(end_date, _time.max).replace(tzinfo=timezone.utc)
                
            all_candles = []
            page_token = None
            
            client = get_httpx_client()
            headers = self._get_headers()
            
            while True:
                params = {
                    "symbols": br_symbol,
                    "timeframe": resolution,
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "limit": 10000, 
                }
                if page_token:
                    params["page_token"] = page_token
                    
                resp = client.get(route["bars"], headers=headers, params=params, timeout=30.0)
                
                if resp.status_code != 200:
                    raise Exception(f"History HTTP {resp.status_code} for {br_symbol}: {resp.text}")
                    
                data = resp.json()
                
                # All three Alpaca asset APIs return the same nested structure when queried via ?symbols=
                bars = data.get("bars", {}).get(br_symbol, [])
                
                if not bars:
                    break
                    
                for bar in bars:
                    try:
                        dt = datetime.strptime(bar["t"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                        all_candles.append({
                            "timestamp": int(dt.timestamp()),
                            "open":      _f(bar.get("o", 0)),
                            "high":      _f(bar.get("h", 0)),
                            "low":       _f(bar.get("l", 0)),
                            "close":     _f(bar.get("c", 0)),
                            "volume":    _i(bar.get("v", 0)),
                            "oi":        0.0
                        })
                    except Exception as e:
                        logger.warning(f"[Alpaca] Skipping unparseable bar: {bar} - {e}")
                
                page_token = data.get("next_page_token")
                if not page_token:
                    break
                    
            if all_candles:
                df = pd.DataFrame(all_candles)
                df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
                logger.info(f"[Alpaca] History: {len(df)} candles for {br_symbol} @ {resolution}")
                return df
                
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])

        except Exception as e:
            logger.error(f"[Alpaca] get_history error for {symbol}: {e}")
            raise Exception(f"Error fetching historical data for {symbol}: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Options Chain
    # ──────────────────────────────────────────────────────────────────────────

    def get_option_chain(self, underlying: str, exchange: str = "OPRA", expiry: str | None = None) -> list[dict]:
        """
        Return all call and put options for a given underlying from the OpenAlgo master DB.
        Run master_contract_download() first to populate the DB.
        """
        from broker.alpaca.database.master_contract_db import SymToken

        try:
            query = SymToken.query.filter(
                SymToken.exchange == exchange,
                SymToken.instrumenttype.in_(["CE", "PE"]),
                SymToken.symbol.ilike(f"{underlying}%"),
            )
            if expiry:
                query = query.filter(SymToken.expiry == expiry.upper())

            rows = query.all()

            result = [
                {
                    "symbol":         r.symbol,
                    "brsymbol":       r.brsymbol,
                    "token":          r.token,
                    "instrumenttype": r.instrumenttype,
                    "expiry":         r.expiry,
                    "strike":         r.strike,
                    "lotsize":        r.lotsize,
                    "tick_size":      r.tick_size,
                }
                for r in rows
            ]

            def _expiry_sort_key(expiry_str):
                try:
                    return datetime.strptime(expiry_str, "%d-%b-%y").date()
                except (ValueError, TypeError):
                    return datetime.max.date()

            result.sort(key=lambda x: (x["instrumenttype"], _expiry_sort_key(x["expiry"]), x["strike"]))
            return result

        except Exception as exc:
            logger.error(f"[Alpaca] get_option_chain error: {exc}")
            return []

    def get_intervals(self) -> list:
        return list(self.TIMEFRAME_MAP.keys())