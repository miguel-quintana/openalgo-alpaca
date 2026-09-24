# Developed and refined with assistance from Google's Gemini AI.
"""
alpaca_adapter.py
OpenAlgo WebSocket adapter for Alpaca.
Maintains separated upstream connections for US Equities, Crypto, Options, and Trade Events.
"""

import json
import os
import threading
import time
from typing import Any

from broker.alpaca.streaming.alpaca_websocket import AlpacaWebSocket
from broker.alpaca.streaming.alpaca_mapping import (
    AlpacaCapabilityRegistry,
    AlpacaModeMapper,
)
from database.auth_db import get_auth_token
from database.token_db import get_br_symbol
from utils.logging import get_logger

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from websocket_proxy.base_adapter import BaseBrokerWebSocketAdapter
from websocket_proxy.mapping import SymbolMapper

class AlpacaWebSocketAdapter(BaseBrokerWebSocketAdapter):
    def __init__(self):
        super().__init__()
        self.logger = get_logger("alpaca_websocket_adapter")
        self.clients = {}  
        self.user_id = None
        self.broker_name = "alpaca"
        self.running = False
        self._lock = threading.Lock()
        self.last_values: dict[str, dict] = {}

        self.subscription_queue: list[tuple[str, str, str]] = []
        self.unsubscription_queue: list[tuple[str, str, str]] = []
        self.batch_timer = None
        self.batch_delay = 0.5

    def initialize(self, broker_name: str, user_id: str, auth_data: dict | None = None) -> None:
            self.user_id = user_id
            self.broker_name = broker_name

            if auth_data:
                api_key = auth_data.get("api_key") or auth_data.get("access_token", "")
                api_secret = auth_data.get("api_secret", "")
            else:
                api_key = get_auth_token(user_id, bypass_cache=True) or os.getenv("ALPACA_API_KEY") or os.getenv("BROKER_API_KEY", "")
                api_secret = os.getenv("ALPACA_API_SECRET") or os.getenv("BROKER_API_SECRET", "")

            if not api_key or not api_secret:
                raise ValueError(f"Missing API credentials for user {user_id} in Alpaca adapter")

            is_paper = "paper" in str(api_key).lower() or api_key.startswith("PK") or os.getenv("ALPACA_PAPER", "True").lower() == "true"
            trade_url = "wss://paper-api.alpaca.markets/stream" if is_paper else "wss://api.alpaca.markets/stream"

            self.clients["US"] = AlpacaWebSocket(
                api_key=api_key, api_secret=api_secret, url="wss://stream.data.alpaca.markets/v2/sip", name="us",
                use_msgpack=False, on_message=self._on_data, on_error=self._on_error, on_close=self._on_close
            )
            self.clients["CRYPTO"] = AlpacaWebSocket(
                api_key=api_key, api_secret=api_secret, url="wss://stream.data.alpaca.markets/v1beta3/crypto/us", name="crypto",
                use_msgpack=False, on_message=self._on_data, on_error=self._on_error, on_close=self._on_close
            )
            self.clients["OPRA"] = AlpacaWebSocket(
                api_key=api_key, api_secret=api_secret, url="wss://stream.data.alpaca.markets/v1beta1/opra", name="opra",
                use_msgpack=True, on_message=self._on_data, on_error=self._on_error, on_close=self._on_close
            )
            self.clients["TRADE"] = AlpacaWebSocket(
                api_key=api_key, api_secret=api_secret, url=trade_url, name="trade",
                use_msgpack=False, on_open=self._on_trade_open, on_message=self._on_data, on_error=self._on_error, on_close=self._on_close
            )

            self.running = True
        
    def connect(self) -> None:
        for client in self.clients.values():
            threading.Thread(target=client.connect, daemon=True).start()

    def disconnect(self) -> None:
        self.running = False
        with self._lock:
            if self.batch_timer:
                self.batch_timer.cancel()
                self.batch_timer = None
            self.subscription_queue.clear()
            self.unsubscription_queue.clear()

        for client in self.clients.values():
            client.forget_subscriptions()
            client.close_connection()
        self.cleanup_zmq()

    def subscribe(self, symbol: str, exchange: str, mode: int = 2, depth_level: int = 1) -> dict[str, Any]:
            if not AlpacaCapabilityRegistry.supports_mode(mode):
                return self._create_error_response("INVALID_MODE", f"Mode {mode} not supported")

            br_symbol = get_br_symbol(symbol, exchange) or symbol
            
            # Strip exchange suffixes (e.g., AMD:US -> AMD) for Alpaca data streams
            if exchange in ("US", "CRYPTO") and br_symbol:
                br_symbol = br_symbol.split(":")[0].split(".")[0]

            channels = AlpacaModeMapper.get_channels(mode)
            corr_id = f"{symbol}_{exchange}_{mode}"

            with self._lock:
                self.subscriptions[corr_id] = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "br_symbol": br_symbol,
                    "mode": mode,
                    "channels": channels,
                }
                for channel in channels:
                    self._queue_op(self.subscription_queue, self.unsubscription_queue, exchange, channel, br_symbol)

            return self._create_success_response(f"Subscribed {symbol}.{exchange}", symbol=symbol, exchange=exchange, mode=mode)
    
    def unsubscribe(self, symbol: str, exchange: str, mode: int = 2) -> dict[str, Any]:
        channels = AlpacaModeMapper.get_channels(mode)
        corr_id = f"{symbol}_{exchange}_{mode}"

        with self._lock:
            stored = self.subscriptions.pop(corr_id, None)
            br_symbol = (stored or {}).get("br_symbol") or symbol
            remaining = list(self.subscriptions.values())
            
            stale_channels = [
                ch for ch in channels
                if not any(s.get("br_symbol") == br_symbol and s.get("exchange") == exchange and ch in s.get("channels", ()) for s in remaining)
            ]

            cache_key = f"{symbol}_{exchange}"
            if not any(s.get("symbol") == symbol and s.get("exchange") == exchange for s in remaining):
                self.last_values.pop(cache_key, None)

            for channel in stale_channels:
                self._queue_op(self.unsubscription_queue, self.subscription_queue, exchange, channel, br_symbol)

        return self._create_success_response(f"Unsubscribed {symbol}.{exchange}", symbol=symbol, exchange=exchange, mode=mode)

    def _queue_op(self, queue: list, opposite: list, exchange: str, channel: str, br_symbol: str) -> None:
        entry = (exchange, channel, br_symbol)
        if entry in opposite:
            opposite.remove(entry)
            return
        if entry not in queue:
            queue.append(entry)
        if self.batch_timer is None:
            self.batch_timer = threading.Timer(self.batch_delay, self._process_batch)
            self.batch_timer.daemon = True
            self.batch_timer.start()

    def _process_batch(self) -> None:
        with self._lock:
            self.batch_timer = None
            subs = list(self.subscription_queue)
            unsubs = list(self.unsubscription_queue)
            self.subscription_queue.clear()
            self.unsubscription_queue.clear()

        def _group(lst):
            res = {}
            for ex, ch, sym in lst:
                res.setdefault(ex, {}).setdefault(ch, []).append(sym)
            return res

        for ex, ch_map in _group(unsubs).items():
            client = self.clients.get(ex)
            if client:
                for ch, syms in ch_map.items():
                    client.unsubscribe_channel(ch, syms)

        for ex, ch_map in _group(subs).items():
            client = self.clients.get(ex)
            if client:
                for ch, syms in ch_map.items():
                    client.subscribe_channel(ch, syms)

    def _on_trade_open(self, wsapp) -> None:
        client = self.clients.get("TRADE")
        if client:
            client.subscribe_trade_updates()

    def _on_error(self, wsapp, error) -> None:
        self.logger.error("AlpacaWS error: %s", error)

    def _on_close(self, wsapp) -> None:
        pass

    def _on_data(self, wsapp, msg: dict) -> None:
        try:
            m_type = msg.get("T") or msg.get("stream")
            
            # Catch Account-Level Order/Execution Updates
            if m_type == "trade_updates":
                topic = "alpaca_orders"
                payload = dict(msg)
                payload["timestamp"] = int(time.time() * 1000)
                self.publish_market_data(topic, payload)
                return

            if m_type not in ("t", "q", "b", "o"): 
                return

            br_symbol = msg.get("S")
            if not br_symbol:
                return

            fields = {}
            if m_type == "t":
                fields["ltp"] = float(msg.get("p", 0.0))
            elif m_type == "q":
                fields["bid_price"] = float(msg.get("bp", 0.0))
                fields["bid_qty"] = int(msg.get("bs", 0))
                fields["ask_price"] = float(msg.get("ap", 0.0))
                fields["ask_qty"] = int(msg.get("as", 0))
            elif m_type == "b":
                fields["open"] = float(msg.get("o", 0.0))
                fields["high"] = float(msg.get("h", 0.0))
                fields["low"] = float(msg.get("l", 0.0))
                fields["close"] = float(msg.get("c", 0.0))
                fields["volume"] = int(msg.get("v", 0))
            elif m_type == "o":
                bids = [{"price": float(b.get("p", 0)), "quantity": int(b.get("s", 0))} for b in msg.get("b", [])]
                asks = [{"price": float(a.get("p", 0)), "quantity": int(a.get("s", 0))} for a in msg.get("a", [])]
                while len(bids) < 5: bids.append({"price": 0.0, "quantity": 0})
                while len(asks) < 5: asks.append({"price": 0.0, "quantity": 0})
                fields["depth"] = {"buy": bids[:5], "sell": asks[:5]}
                fields["totalbuyqty"] = sum(lvl["quantity"] for lvl in bids)
                fields["totalsellqty"] = sum(lvl["quantity"] for lvl in asks)

            ts = int(time.time() * 1000)
            subscriptions = self._find_subscriptions_by_br_symbol(br_symbol)
            if not subscriptions:
                return

            cache_key = f"{subscriptions[0]['symbol']}_{subscriptions[0]['exchange']}"
            merged = self._merge_into_cache(cache_key, fields)

            for sub in subscriptions:
                oa_symbol = sub["symbol"]
                oa_exchange = sub["exchange"]
                oa_mode = sub["mode"]
                mode_str = AlpacaModeMapper.get_mode_str(oa_mode)
                topic = f"{oa_exchange}_{oa_symbol}_{mode_str}"

                market_data = dict(merged)
                market_data.update({
                    "symbol": oa_symbol,
                    "exchange": oa_exchange,
                    "mode": oa_mode,
                    "timestamp": ts,
                })
                self.publish_market_data(topic, market_data)

        except Exception as exc:
            self.logger.error("_on_data error: %s", exc)

    def _merge_into_cache(self, cache_key: str, fields: dict) -> dict:
        with self._lock:
            cached = self.last_values.setdefault(cache_key, {})
            cached.update(fields)
            return dict(cached)

    def _find_subscriptions_by_br_symbol(self, br_symbol: str) -> list[dict]:
        with self._lock:
            return [s for s in self.subscriptions.values() if s.get("br_symbol") == br_symbol]