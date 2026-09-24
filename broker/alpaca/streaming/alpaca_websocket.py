# Developed and refined with assistance from Google's Gemini AI.

"""
alpaca_websocket.py
Low-level WebSocket client for Alpaca supporting JSON and MessagePack codecs 
with robust credential validation, authentication gating, and message decoding.
"""

import json
import ssl
import threading
import time
import msgpack
import websocket
from utils.logging import get_logger

logger = get_logger("alpaca_websocket")

class AlpacaWebSocket:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        url: str,
        name: str = "alpaca",
        authenticate: bool = True,
        use_msgpack: bool = False,
        on_message=None,
        on_error=None,
        on_open=None,
        on_close=None,
        max_retry_attempt: int = 5,
        retry_delay: int = 5,
        retry_multiplier: int = 2,
    ):
        self.api_key = str(api_key or "").strip()
        self.api_secret = str(api_secret or "").strip()
        self.url = url
        self.name = name
        self.authenticate = authenticate
        self.use_msgpack = use_msgpack

        if self.authenticate and (not self.api_key or not self.api_secret):
            logger.error("AlpacaWS[%s] initialized with missing API key or secret!", self.name)

        self.on_message = on_message or (lambda ws, msg: None)
        self.on_error = on_error or (lambda ws, err: None)
        self.on_open = on_open or (lambda ws: None)
        self.on_close = on_close or (lambda ws: None)

        self.max_retry_attempt = max_retry_attempt
        self.retry_delay = retry_delay
        self.retry_multiplier = retry_multiplier

        self.wsapp = None
        self._lock = threading.Lock()
        self._connected = False
        self._authenticated = False
        self._stop_flag = False
        self._connect_succeeded = False

        self._active_symbols: dict[str, set[str]] = {}
        self._active_private: list[dict] = []

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _send_msg(self, payload: dict) -> None:
        if not self._connected or not self.wsapp:
            return
        try:
            if self.use_msgpack:
                packed = msgpack.packb(payload)
                self.wsapp.send(packed, opcode=websocket.ABNF.OPCODE_BINARY)
            else:
                self.wsapp.send(json.dumps(payload))
        except Exception as exc:
            logger.error("AlpacaWS[%s] _send error: %s", self.name, exc)

    def _flush_subscriptions_locked(self) -> None:
        channel_map = {
            c: list(syms) for c, syms in self._active_symbols.items() 
            if syms and any(s and str(s).strip() for s in syms)
        }
        if channel_map:
            self._send_msg({"action": "subscribe", **channel_map})
        for priv_msg in self._active_private:
            self._send_msg(priv_msg)

    def _subscribe(self, channel: str, symbols: list[str]) -> None:
        clean_symbols = [s for s in symbols if s and str(s).strip()]
        if not clean_symbols:
            return
        with self._lock:
            self._active_symbols.setdefault(channel, set()).update(clean_symbols)
            if self._connected and self._authenticated:
                self._send_msg({"action": "subscribe", channel: clean_symbols})

    def _unsubscribe(self, channel: str, symbols: list[str]) -> None:
        clean_symbols = [s for s in symbols if s and str(s).strip()]
        if not clean_symbols:
            return
        with self._lock:
            active = self._active_symbols.get(channel)
            if active:
                active.difference_update(clean_symbols)
            if self._connected and self._authenticated:
                self._send_msg({"action": "unsubscribe", channel: clean_symbols})

    def subscribe_channel(self, channel: str, symbols: list[str]) -> None:
        self._subscribe(channel, symbols)

    def unsubscribe_channel(self, channel: str, symbols: list[str]) -> None:
        self._unsubscribe(channel, symbols)

    def subscribe_trade_updates(self) -> None:
        payload = {
            "action": "listen",
            "data": {"streams": ["trade_updates"]}
        }
        with self._lock:
            if payload not in self._active_private:
                self._active_private.append(payload)
            if self._connected and self._authenticated:
                self._send_msg(payload)

    def forget_subscriptions(self) -> None:
        with self._lock:
            self._active_symbols.clear()
            self._active_private.clear()

    def connect(self) -> None:
        self._stop_flag = False
        retry_attempts = 0
        delay = self.retry_delay

        headers = {"Content-Type": "application/msgpack"} if self.use_msgpack else None

        while not self._stop_flag and retry_attempts <= self.max_retry_attempt:
            try:
                logger.info("AlpacaWS[%s] connecting to %s", self.name, self.url)
                self.wsapp = websocket.WebSocketApp(
                    self.url,
                    header=headers,
                    on_open=self._ws_on_open,
                    on_message=self._ws_on_message,
                    on_error=self._ws_on_error,
                    on_close=self._ws_on_close,
                )
                self.wsapp.run_forever(
                    sslopt={"cert_reqs": ssl.CERT_REQUIRED},
                    ping_interval=30,
                    ping_timeout=10,
                )
                if self._stop_flag:
                    break
                if self._connect_succeeded:
                    self._connect_succeeded = False
                    retry_attempts = 0
                    delay = self.retry_delay
                retry_attempts += 1
                time.sleep(delay)
                delay = min(delay * self.retry_multiplier, 60)
            except Exception as exc:
                logger.error("AlpacaWS[%s] connect error: %s", self.name, exc)
                retry_attempts += 1
                time.sleep(delay)
                delay = min(delay * self.retry_multiplier, 60)

    def close_connection(self) -> None:
        self._stop_flag = True
        if self.wsapp:
            try:
                self.wsapp.close()
            except Exception:
                pass

    def _ws_on_open(self, wsapp) -> None:
        logger.info("AlpacaWS[%s] connected", self.name)
        with self._lock:
            self._connected = True
            self._authenticated = False
            self._connect_succeeded = True

        if self.authenticate:
            try:
                auth_payload = {
                    "action": "auth",
                    "key": self.api_key,
                    "secret": self.api_secret
                }
                if self.use_msgpack:
                    packed = msgpack.packb(auth_payload)
                    wsapp.send(packed, opcode=websocket.ABNF.OPCODE_BINARY)
                else:
                    wsapp.send(json.dumps(auth_payload))
            except Exception as exc:
                logger.error("AlpacaWS[%s] auth send error: %s", self.name, exc)
        else:
            with self._lock:
                self._authenticated = True
                self._flush_subscriptions_locked()

        self.on_open(wsapp)

    def _ws_on_message(self, wsapp, raw) -> None:
        try:
            if self.use_msgpack:
                if isinstance(raw, bytes):
                    msg = msgpack.unpackb(raw, raw=False)
                else:
                    msg = msgpack.unpackb(str(raw).encode('utf-8'), raw=False)
            else:
                if isinstance(raw, bytes):
                    msg = json.loads(raw.decode('utf-8', errors='ignore'))
                else:
                    msg = json.loads(raw)
        except Exception as exc:
            logger.error("AlpacaWS[%s] decode error: %s", self.name, exc)
            return

        msgs = msg if isinstance(msg, list) else [msg]

        for m in msgs:
            m_type = m.get("T") or m.get("stream")
            
            if m_type == "success" and m.get("msg") == "authenticated":
                logger.info("AlpacaWS[%s] authentication successful", self.name)
                with self._lock:
                    self._authenticated = True
                    self._flush_subscriptions_locked()
            elif m_type == "authorization" and m.get("data", {}).get("status") == "authorized":
                logger.info("AlpacaWS[%s] authorization successful", self.name)
                with self._lock:
                    self._authenticated = True
                    for priv_msg in self._active_private:
                        self._send_msg(priv_msg)
            elif m_type == "error":
                logger.error("AlpacaWS[%s] server error frame: %s", self.name, m)

            self.on_message(wsapp, m)

    def _ws_on_error(self, wsapp, error) -> None:
        logger.error("AlpacaWS[%s] error: %s", self.name, error)
        self.on_error(wsapp, error)

    def _ws_on_close(self, wsapp, *args) -> None:
        logger.info("AlpacaWS[%s] closed", self.name)
        with self._lock:
            self._connected = False
            self._authenticated = False
        self.on_close(wsapp)