"""
Bitunix USDT-M futures REST + private WebSocket client.

All Bitunix-specific calls and field names live here. trade_manager.py and
main.py only ever see these normalized dicts:

  position: {symbol, position_id, side ("LONG"/"SHORT"), size, avg_price,
             unrealized_pnl, leverage, stop_loss, sl_prices}
            stop_loss is the SL price when the whole position is covered by
            an SL, else 0. sl_prices lists every resting SL price.
  order:    {order_id, client_id, symbol, side ("BUY"/"SELL"), order_type,
             status, qty, price, reduce_only, stop_loss}
  order event (WS):    {symbol, order_id, client_id, event, status, side,
                        order_type, qty, avg_price, reduce_only}
  position event (WS): {symbol, position_id, event, side, size}

Docs: https://www.bitunix.com/api-docs/futures/  (there is no testnet)
"""
import email.utils
import functools
import hashlib
import json
import logging
import threading
import time
import uuid

import requests
import websocket

import config

log = logging.getLogger("bitunix_client")

BASE_URL = "https://fapi.bitunix.com"
WS_URL = "wss://fapi.bitunix.com/private/"
MARGIN_COIN = "USDT"
STOP_TYPE = "MARK_PRICE"

RATE_LIMIT_CODES = {10005, 10006}
SIGNATURE_ERROR = 10007
ORDER_NOT_FOUND = 20007
SYMBOL_NOT_TRADABLE = 20015
DUPLICATE_CLIENT_ID = 30042


class BitunixError(Exception):
    """Bitunix answered HTTP 200 with a non-zero business code."""

    def __init__(self, code, msg, path):
        super().__init__(f"Bitunix {path} error {code}: {msg}")
        try:
            self.code = int(code)
        except (TypeError, ValueError):
            self.code = code
        self.msg = msg


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _f(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_transient(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, BitunixError):
        return e.code in RATE_LIMIT_CODES
    msg = str(e).lower()
    return any(s in msg for s in ("timeout", "timed out", "too many requests", "rate limit", "connection"))


def _retry(max_attempts=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not _is_transient(e) or attempt == max_attempts - 1:
                        raise
                    log.warning("Retrying %s after: %s (attempt %d/%d)",
                                func.__name__, e, attempt + 1, max_attempts)
                    time.sleep(delay * (attempt + 1))
        return wrapper
    return decorator


def _as_list(data, *keys) -> list:
    """Bitunix returns lists bare, wrapped in a dict, or as null depending on the endpoint."""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in keys:
            if isinstance(data.get(k), list):
                return data[k]
        return [data]
    return []


def _as_obj(data) -> dict:
    """Some endpoints document an object but return a one-element list."""
    if isinstance(data, list):
        return data[0] if data else {}
    return data or {}


class BitunixClient:
    # A JSON ping every 15s keeps the private socket alive. Bitunix allows at
    # most 5 messages/s per connection and may block IPs that keep breaking it.
    WS_PING_SECONDS = 15
    # No message (not even a pong) for this long means the socket is dead.
    WS_STALE_SECONDS = 60
    WS_MAX_BACKOFF = 30

    def __init__(self):
        self.api_key = config.BITUNIX_API_KEY
        self.api_secret = config.BITUNIX_API_SECRET
        self.session = requests.Session()
        self.session.verify = config.SSL_CA_PATH
        self.timeout = 30
        self._instrument_cache = {}  # symbol -> (info, timestamp)
        self._cache_ttl = 300
        self._ws_app = None
        self._ws_thread = None
        self._ws_stop = None
        self._ws_logged_in = threading.Event()
        self._ws_last_msg = 0.0
        self._ws_callbacks = None
        self._tpsl_triggers = {}  # symbol -> ("SL"|"TP"|"TP/SL", time) of the last fired TP/SL

    @staticmethod
    def _norm(symbol: str) -> str:
        return symbol.replace("/", "").replace(" ", "").upper()

    # ---------- REST plumbing ----------

    def _request(self, method: str, path: str, params: dict | None = None,
                 body: dict | None = None, auth: bool = True):
        # Bitunix signs the raw values; empty params must not be sent at all.
        params = {k: str(v) for k, v in (params or {}).items() if v is not None and v != ""}
        # The signature covers the exact body bytes, so sign and send the same string.
        body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = {"Content-Type": "application/json", "language": "en-US"}
        if auth:
            nonce = uuid.uuid4().hex
            timestamp = str(int(time.time() * 1000))
            query = "".join(f"{k}{params[k]}" for k in sorted(params))
            digest = _sha256(nonce + timestamp + self.api_key + query + body_str)
            headers.update({
                "api-key": self.api_key,
                "nonce": nonce,
                "timestamp": timestamp,
                "sign": _sha256(digest + self.api_secret),
            })
        resp = self.session.request(method, BASE_URL + path, params=params or None,
                                    data=body_str or None, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        code = payload.get("code")
        if code not in (0, "0"):
            raise BitunixError(code, payload.get("msg"), path)
        return payload.get("data")

    def _get(self, path: str, auth: bool = True, **params):
        return self._request("GET", path, params=params, auth=auth)

    def _post(self, path: str, body: dict):
        return self._request("POST", path, body=body)

    # ---------- diagnostics ----------

    def clock_drift_seconds(self) -> float | None:
        """Local clock minus Bitunix's HTTP Date header. Signed requests fail if this drifts."""
        try:
            resp = self.session.get(BASE_URL + "/api/v1/futures/market/tickers",
                                    params={"symbols": "BTCUSDT"}, timeout=10)
            server = email.utils.parsedate_to_datetime(resp.headers["Date"]).timestamp()
            return time.time() - server
        except Exception as e:
            log.warning("Could not measure clock drift against Bitunix: %s", e)
            return None

    def get_position_mode(self) -> str:
        try:
            data = _as_obj(self._get("/api/v1/futures/account/position_mode"))
            if data.get("positionMode"):
                return str(data["positionMode"]).upper()
        except Exception as e:
            log.warning("position_mode lookup failed (%s) — falling back to account info", e)
        return str(self._account().get("positionMode") or "UNKNOWN").upper()

    # ---------- account / instrument info ----------

    def _account(self) -> dict:
        data = _as_obj(self._get("/api/v1/futures/account", marginCoin=MARGIN_COIN))
        if not data:
            raise RuntimeError("Empty account response from Bitunix")
        return data

    def get_equity_usdt(self) -> float:
        """Wallet balance (excluding unrealized PnL): free + order-locked + position margin."""
        a = self._account()
        return _f(a.get("available")) + _f(a.get("frozen")) + _f(a.get("margin"))

    def get_wallet_info(self) -> dict:
        a = self._account()
        return {
            "equity": _f(a.get("available")) + _f(a.get("frozen")) + _f(a.get("margin")),
            "available": _f(a.get("available")),
            "unrealized_pnl": _f(a.get("crossUnrealizedPNL")) + _f(a.get("isolationUnrealizedPNL")),
        }

    def get_instrument_info(self, symbol: str) -> dict:
        symbol = self._norm(symbol)
        now = time.time()
        if symbol in self._instrument_cache:
            info, ts = self._instrument_cache[symbol]
            if now - ts < self._cache_ttl:
                return info
        try:
            data = self._get("/api/v1/futures/market/trading_pairs", auth=False, symbols=symbol)
        except BitunixError as e:
            if e.code == SYMBOL_NOT_TRADABLE:
                raise ValueError(f"{symbol} is not listed on Bitunix futures.") from e
            raise
        info = next((d for d in _as_list(data) if d.get("symbol") == symbol), None)
        if not info:
            raise ValueError(f"{symbol} is not listed on Bitunix futures.")
        self._instrument_cache[symbol] = (info, now)
        return info

    def check_tradable(self, symbol: str):
        """Raise ValueError (shown to the user) if Bitunix won't accept API orders on this symbol."""
        info = self.get_instrument_info(symbol)
        status = str(info.get("symbolStatus") or "OPEN").upper()
        if status != "OPEN":
            raise ValueError(f"{symbol} is not tradable on Bitunix right now (status {status}).")
        if info.get("isApiSupported") is False:
            raise ValueError(f"{symbol} does not support API trading on Bitunix.")

    def get_max_leverage(self, symbol: str) -> int:
        return int(_f(self.get_instrument_info(symbol).get("maxLeverage")) or config.DEFAULT_LEVERAGE)

    def _qty_decimals(self, symbol: str) -> int:
        return int(self.get_instrument_info(symbol).get("basePrecision") or 0)

    def _price_decimals(self, symbol: str) -> int:
        return int(self.get_instrument_info(symbol).get("quotePrecision") or 0)

    def round_qty(self, symbol: str, qty: float) -> float:
        symbol = self._norm(symbol)
        decimals = self._qty_decimals(symbol)
        min_qty = _f(self.get_instrument_info(symbol).get("minTradeVolume"))
        rounded = round(qty, decimals)
        return max(rounded, min_qty)

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        return f"{qty:.{self._qty_decimals(symbol)}f}"

    def round_price(self, symbol: str, price: float) -> float:
        symbol = self._norm(symbol)
        decimals = self._price_decimals(symbol)
        result = round(price, decimals)
        if result == 0 and price > 0:
            log.warning("round_price(%s, %s, decimals=%s) rounded to 0 — keeping raw price",
                        symbol, price, decimals)
            result = price
        return result

    def _fmt_price(self, symbol: str, price: float) -> str:
        return f"{price:.{self._price_decimals(symbol)}f}"

    def get_tickers(self, symbols: list[str]) -> dict:
        symbols = [self._norm(s) for s in symbols]
        data = self._get("/api/v1/futures/market/tickers", auth=False, symbols=",".join(symbols))
        return {t.get("symbol"): t for t in _as_list(data)}

    def get_mark_prices(self, symbols: list[str]) -> dict:
        return {s: _f(t.get("markPrice")) for s, t in self.get_tickers(symbols).items()
                if _f(t.get("markPrice")) > 0}

    def get_mark_price(self, symbol: str) -> float:
        symbol = self._norm(symbol)
        mark = self.get_mark_prices([symbol]).get(symbol)
        if not mark:
            raise RuntimeError(f"No mark price for {symbol} from Bitunix")
        return mark

    def get_last_price(self, symbol: str) -> float:
        symbol = self._norm(symbol)
        t = self.get_tickers([symbol]).get(symbol) or {}
        last = _f(t.get("lastPrice")) or _f(t.get("last"))
        if not last:
            raise RuntimeError(f"No last price for {symbol} from Bitunix")
        return last

    # ---------- position / leverage setup ----------

    def _leverage_margin_mode(self, symbol: str) -> dict:
        try:
            return _as_obj(self._get("/api/v1/futures/account/get_leverage_margin_mode",
                                     symbol=symbol, marginCoin=MARGIN_COIN))
        except Exception as e:
            log.warning("Could not read leverage/margin mode for %s: %s", symbol, e)
            return {}

    def set_leverage(self, symbol: str, leverage: int):
        # Bitunix has no "not modified" error, so skip the call when nothing changes.
        symbol = self._norm(symbol)
        current = int(_f(self._leverage_margin_mode(symbol).get("leverage")))
        if current == leverage:
            return
        self._post("/api/v1/futures/account/change_leverage",
                   {"symbol": symbol, "marginCoin": MARGIN_COIN, "leverage": int(leverage)})

    def set_margin_mode(self, symbol: str, mode: str):
        symbol = self._norm(symbol)
        target = "ISOLATION" if mode.upper().startswith("ISOLAT") else "CROSS"
        current = str(self._leverage_margin_mode(symbol).get("marginMode") or "").upper()
        if current == target:
            return
        self._post("/api/v1/futures/account/change_margin_mode",
                   {"symbol": symbol, "marginCoin": MARGIN_COIN, "marginMode": target})

    def _raw_positions(self, symbol: str | None = None) -> list:
        data = self._get("/api/v1/futures/position/get_pending_positions",
                         symbol=self._norm(symbol) if symbol else None)
        return [p for p in _as_list(data, "positionList") if _f(p.get("qty")) > 0]

    def get_tpsl_orders(self, symbol: str, position_id: str | None = None) -> list:
        data = self._get("/api/v1/futures/tpsl/get_pending_orders",
                         symbol=self._norm(symbol), positionId=position_id)
        return _as_list(data, "orderList", "list")

    @staticmethod
    def _sl_orders(tpsl_orders: list) -> list:
        return [o for o in tpsl_orders if _f(o.get("slPrice")) > 0]

    @staticmethod
    def _sl_covers(sl_orders: list, size: float) -> bool:
        """A position-level SL (no slQty) covers everything; partial ones must add up to the size."""
        if any(not _f(o.get("slQty")) for o in sl_orders):
            return True
        return sum(_f(o.get("slQty")) for o in sl_orders) >= size * (1 - 1e-9)

    def _normalize_position(self, p: dict) -> dict:
        size = _f(p.get("qty"))
        side = str(p.get("side") or "").upper()
        side = {"BUY": "LONG", "SELL": "SHORT"}.get(side, side)
        avg = _f(p.get("avgOpenPrice")) or (_f(p.get("entryValue")) / size if size else 0.0)
        position_id = str(p.get("positionId") or "")
        sl_orders = self._sl_orders(self.get_tpsl_orders(p["symbol"], position_id))
        sl_prices = sorted({_f(o["slPrice"]) for o in sl_orders})
        return {
            "symbol": p.get("symbol"),
            "position_id": position_id,
            "side": side,
            "size": size,
            "avg_price": avg,
            "unrealized_pnl": _f(p.get("unrealizedPNL")),
            "leverage": int(_f(p.get("leverage")) or 1),
            "stop_loss": sl_prices[0] if sl_orders and self._sl_covers(sl_orders, size) else 0.0,
            "sl_prices": sl_prices,
        }

    def get_all_open_positions(self) -> list:
        return [self._normalize_position(p) for p in self._raw_positions()]

    def get_open_position(self, symbol: str) -> dict | None:
        symbol = self._norm(symbol)
        for p in self._raw_positions(symbol):
            if p.get("symbol") == symbol:
                return self._normalize_position(p)
        return None

    def has_open_orders_or_position(self, symbol: str) -> bool:
        if self.get_open_orders(symbol):
            return True
        return bool(self._raw_positions(symbol))

    # ---------- orders ----------

    @staticmethod
    def _normalize_order(o: dict) -> dict:
        return {
            "order_id": str(o.get("orderId") or o.get("id") or ""),
            "client_id": o.get("clientId"),
            "symbol": o.get("symbol"),
            "side": str(o.get("side") or "").upper(),
            "order_type": str(o.get("orderType") or o.get("type") or "").upper(),
            "status": str(o.get("status") or o.get("orderStatus") or "").upper(),
            "qty": _f(o.get("qty")),
            "price": _f(o.get("price")),
            "reduce_only": bool(o.get("reduceOnly") or o.get("reductionOnly")),
            "stop_loss": _f(o.get("slPrice")),
        }

    def get_open_orders(self, symbol: str) -> list:
        data = self._get("/api/v1/futures/trade/get_pending_orders",
                         symbol=self._norm(symbol), limit=100)
        return [self._normalize_order(o) for o in _as_list(data, "orderList")]

    def find_order(self, symbol: str, client_id: str) -> dict | None:
        """Look an order up by clientId (open or historical)."""
        try:
            data = _as_obj(self._get("/api/v1/futures/trade/get_order_detail", clientId=client_id))
            if data.get("orderId"):
                return self._normalize_order(data)
        except BitunixError as e:
            if e.code != ORDER_NOT_FOUND:
                log.warning("Order lookup failed for %s: %s", client_id, e)
        except Exception as e:
            log.warning("Order lookup failed for %s: %s", client_id, e)
        try:
            for o in self.get_open_orders(symbol):
                if o["client_id"] == client_id:
                    return o
        except Exception as e:
            log.warning("Open-order lookup failed for %s: %s", client_id, e)
        return None

    def _add_tpsl(self, body: dict, symbol: str, stop_loss: float | None, take_profit: float | None):
        if stop_loss is not None:
            body.update(slPrice=self._fmt_price(symbol, stop_loss), slStopType=STOP_TYPE, slOrderType="MARKET")
        if take_profit is not None:
            body.update(tpPrice=self._fmt_price(symbol, take_profit), tpStopType=STOP_TYPE, tpOrderType="MARKET")

    def _place_order_once(self, body: dict, max_attempts: int = 3) -> str:
        """
        Place an order with a client-side clientId so a retry can never
        create a second order: if a timed-out request actually reached
        Bitunix, the retry is rejected as a duplicate (or found by lookup) and
        the original order id is returned instead of placing another one.
        Returns the Bitunix orderId.
        """
        client_id = uuid.uuid4().hex
        body["clientId"] = client_id
        symbol = body["symbol"]
        for attempt in range(max_attempts):
            try:
                data = _as_obj(self._post("/api/v1/futures/trade/place_order", body))
                return str(data["orderId"])
            except Exception as e:
                if isinstance(e, BitunixError) and e.code == DUPLICATE_CLIENT_ID:
                    existing = self.find_order(symbol, client_id)
                    if existing:
                        log.warning("Order %s already accepted by Bitunix — not placing again", client_id)
                        return existing["order_id"]
                    raise
                if not _is_transient(e) or attempt == max_attempts - 1:
                    raise
                existing = self.find_order(symbol, client_id)
                if existing:
                    log.warning("Order %s reached Bitunix despite error (%s) — not placing again", client_id, e)
                    return existing["order_id"]
                log.warning("Retrying place_order %s after: %s (attempt %d/%d)",
                            client_id, e, attempt + 1, max_attempts)
                time.sleep(attempt + 1)

    def place_market_order(self, symbol: str, side: str, qty: float,
                           stop_loss: float | None = None,
                           take_profit: float | None = None) -> str:
        """Opening market order. side is BUY or SELL. Returns the order id."""
        symbol = self._norm(symbol)
        body = dict(symbol=symbol, side=side.upper(), tradeSide="OPEN",
                    orderType="MARKET", qty=self._fmt_qty(symbol, qty))
        self._add_tpsl(body, symbol, stop_loss, take_profit)
        return self._place_order_once(body)

    def place_limit_order(self, symbol: str, side: str, qty: float, price: float,
                          stop_loss: float | None = None,
                          take_profit: float | None = None) -> str:
        """Opening GTC limit order. side is BUY or SELL. Returns the order id."""
        symbol = self._norm(symbol)
        body = dict(symbol=symbol, side=side.upper(), tradeSide="OPEN", orderType="LIMIT",
                    qty=self._fmt_qty(symbol, qty), price=self._fmt_price(symbol, price), effect="GTC")
        self._add_tpsl(body, symbol, stop_loss, take_profit)
        return self._place_order_once(body)

    def amend_order_sl(self, symbol: str, order: dict, sl_price: float):
        """Change the SL attached to a resting (unfilled) order. Bitunix requires qty+price on every modify."""
        symbol = self._norm(symbol)
        self._post("/api/v1/futures/trade/modify_order", {
            "orderId": order["order_id"],
            "qty": self._fmt_qty(symbol, order["qty"]),
            "price": self._fmt_price(symbol, order["price"]),
            "slPrice": self._fmt_price(symbol, sl_price),
            "slStopType": STOP_TYPE,
            "slOrderType": "MARKET",
        })

    @_retry()
    def set_position_sl(self, symbol: str, sl_price: float):
        """
        Make every SL on the open position equal sl_price and make sure the
        whole position is covered. SLs attached to entry/DCA orders become
        separate TP/SL orders on fill, so there may be several to move.
        Raises on failure so callers can alert.
        """
        symbol = self._norm(symbol)
        if sl_price is None or sl_price <= 0:
            raise ValueError(f"Refusing to set invalid SL {sl_price} on {symbol}")
        raw = next((p for p in self._raw_positions(symbol) if p.get("symbol") == symbol), None)
        if not raw:
            raise RuntimeError(f"No open {symbol} position to attach an SL to")
        position_id = str(raw["positionId"])
        size = _f(raw.get("qty"))
        sl_str = self._fmt_price(symbol, sl_price)
        target = float(sl_str)
        tol = target * 1e-9

        sl_orders = self._sl_orders(self.get_tpsl_orders(symbol, position_id))
        for o in sl_orders:
            if abs(_f(o["slPrice"]) - target) <= tol:
                continue
            if not _f(o.get("slQty")):
                body = {"symbol": symbol, "positionId": position_id,
                        "slPrice": sl_str, "slStopType": STOP_TYPE}
                if _f(o.get("tpPrice")) > 0:
                    body.update(tpPrice=o["tpPrice"], tpStopType=o.get("tpStopType") or STOP_TYPE)
                self._post("/api/v1/futures/tpsl/position/modify_order", body)
            else:
                body = {"orderId": str(o.get("id") or o.get("orderId")), "slPrice": sl_str,
                        "slStopType": STOP_TYPE, "slOrderType": "MARKET", "slQty": o["slQty"]}
                if _f(o.get("tpPrice")) > 0:
                    body.update(tpPrice=o["tpPrice"], tpStopType=o.get("tpStopType") or STOP_TYPE,
                                tpOrderType=o.get("tpOrderType") or "MARKET", tpQty=o.get("tpQty"))
                    if o.get("tpOrderPrice"):
                        body["tpOrderPrice"] = o["tpOrderPrice"]
                self._post("/api/v1/futures/tpsl/modify_order", body)
            log.info("%s: moved SL order %s %s -> %s", symbol, o.get("id"), o.get("slPrice"), sl_str)

        if not self._sl_covers(sl_orders, size):
            self._post("/api/v1/futures/tpsl/position/place_order",
                       {"symbol": symbol, "positionId": position_id,
                        "slPrice": sl_str, "slStopType": STOP_TYPE})
            log.info("%s: placed position SL at %s (size %s)", symbol, sl_str, size)

    def cancel_order(self, symbol: str, order_id: str):
        symbol = self._norm(symbol)
        try:
            data = self._post("/api/v1/futures/trade/cancel_orders",
                              {"symbol": symbol, "orderList": [{"orderId": order_id}]})
            for failure in _as_obj(data).get("failureList") or []:
                log.warning("Cancel rejected for %s (%s): %s", order_id, symbol, failure)
        except Exception as e:
            log.warning("Cancel failed for %s (%s): %s", order_id, symbol, e)

    @_retry()
    def cancel_all(self, symbol: str):
        """Cancel all regular (entry/DCA/limit) orders on the symbol. Position TP/SL orders are untouched."""
        self._post("/api/v1/futures/trade/cancel_all_orders", {"symbol": self._norm(symbol)})

    def close_position_market(self, symbol: str) -> float:
        """Market-close the whole open position, if any. Returns the qty closed (0 if none)."""
        symbol = self._norm(symbol)
        raw = next((p for p in self._raw_positions(symbol) if p.get("symbol") == symbol), None)
        if not raw:
            return 0.0
        self._post("/api/v1/futures/trade/flash_close_position", {"positionId": str(raw["positionId"])})
        return _f(raw.get("qty"))

    # ---------- private websocket (orders / positions) ----------

    @staticmethod
    def _normalize_order_event(d: dict) -> dict:
        return {
            "symbol": d.get("symbol"),
            "order_id": str(d.get("orderId") or ""),
            "client_id": d.get("clientId"),
            "event": str(d.get("event") or "").upper(),
            "status": str(d.get("orderStatus") or d.get("status") or "").upper(),
            "side": str(d.get("side") or "").upper(),
            "order_type": str(d.get("type") or d.get("orderType") or "").upper(),
            "qty": _f(d.get("qty")),
            "avg_price": _f(d.get("averagePrice")) or _f(d.get("price")),
            "reduce_only": bool(d.get("reduceOnly") or d.get("reductionOnly")),
        }

    @staticmethod
    def _normalize_position_event(d: dict) -> dict:
        event = str(d.get("event") or "").upper()
        side = str(d.get("side") or "").upper()
        return {
            "symbol": d.get("symbol"),
            "position_id": str(d.get("positionId") or ""),
            "event": event,
            "side": {"BUY": "LONG", "SELL": "SHORT"}.get(side, side),
            "size": 0.0 if event == "CLOSE" else _f(d.get("qty")),
        }

    def start_private_ws(self, on_order, on_position):
        """Start the private socket in a background thread; waits up to 30s for the first login."""
        self._ws_callbacks = (on_order, on_position)
        self._ws_stop = threading.Event()
        self._ws_logged_in.clear()
        self._ws_last_msg = time.time()
        self._ws_thread = threading.Thread(target=self._ws_run, args=(self._ws_stop,),
                                           daemon=True, name="bitunix-ws")
        self._ws_thread.start()
        if not self._ws_logged_in.wait(30):
            log.warning("Bitunix private WebSocket not logged in after 30s — still retrying in the background")

    def _ws_run(self, stop: threading.Event):
        backoff = 1
        while not stop.is_set():
            connected_at = time.time()
            app = websocket.WebSocketApp(
                WS_URL,
                on_open=self._ws_on_open,
                on_message=self._ws_on_message,
                on_error=lambda _ws, e: log.warning("Bitunix WebSocket error: %s", e),
                on_close=lambda _ws, code, msg: log.warning("Bitunix WebSocket closed (%s %s)", code, msg),
            )
            self._ws_app = app
            try:
                # Protocol pings off: Bitunix wants JSON pings, sent by _ws_pinger.
                app.run_forever(ping_interval=0, sslopt={"ca_certs": config.SSL_CA_PATH})
            except Exception as e:
                log.warning("Bitunix WebSocket crashed: %s", e)
            self._ws_logged_in.clear()
            if stop.is_set():
                break
            if time.time() - connected_at > 60:
                backoff = 1
            log.warning("Bitunix WebSocket reconnecting in %ss", backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, self.WS_MAX_BACKOFF)

    def _ws_on_open(self, app):
        nonce = uuid.uuid4().hex
        timestamp = int(time.time())  # seconds for WS login, unlike REST's milliseconds
        sign = _sha256(_sha256(nonce + str(timestamp) + self.api_key) + self.api_secret)
        app.send(json.dumps({"op": "login", "args": [
            {"apiKey": self.api_key, "timestamp": timestamp, "nonce": nonce, "sign": sign}]}))
        threading.Thread(target=self._ws_pinger, args=(app,), daemon=True, name="bitunix-ws-ping").start()

    def _ws_pinger(self, app):
        while self._ws_app is app and not self._ws_stop.wait(self.WS_PING_SECONDS):
            try:
                app.send(json.dumps({"op": "ping", "ping": int(time.time())}))
            except Exception:
                return

    def _ws_on_message(self, app, raw):
        self._ws_last_msg = time.time()
        try:
            msg = json.loads(raw)
        except ValueError:
            log.warning("Non-JSON WebSocket message: %r", raw[:200])
            return
        op = msg.get("op")
        if op == "login":
            data = msg.get("data")
            failed = (isinstance(data, dict) and (data.get("result") is False or data.get("code") not in (None, 0, "0"))) \
                or msg.get("code") not in (None, 0, "0")
            if failed:
                log.error("Bitunix WebSocket login rejected: %s", msg)
                return
            log.info("Bitunix private WebSocket logged in")
            self._ws_logged_in.set()
            app.send(json.dumps({"op": "subscribe",
                                 "args": [{"ch": "order"}, {"ch": "position"}, {"ch": "tpsl"}]}))
            return
        if op in ("ping", "pong", "connect", "subscribe"):
            return

        ch = msg.get("ch")
        on_order, on_position = self._ws_callbacks
        for item in _as_list(msg.get("data")):
            try:
                if ch == "order":
                    on_order(self._normalize_order_event(item))
                elif ch == "position":
                    on_position(self._normalize_position_event(item))
                elif ch == "tpsl":
                    log.info("TP/SL update %s %s %s: sl=%s tp=%s", item.get("symbol"), item.get("event"),
                             item.get("status"), item.get("slPrice"), item.get("tpPrice"))
                    self._record_trigger(item)
            except Exception:
                log.exception("WebSocket %s handler failed for %s", ch, item)

    def _record_trigger(self, item: dict):
        # A fired TP/SL is pushed (CLOSE + FILLED) just before the position
        # closes; remember which side fired so the close can be labelled.
        if str(item.get("event")).upper() != "CLOSE" or str(item.get("status")).upper() != "FILLED":
            return
        sl, tp = _f(item.get("slPrice")) > 0, _f(item.get("tpPrice")) > 0
        kind = "SL" if sl and not tp else "TP" if tp and not sl else "TP/SL"
        self._tpsl_triggers[item.get("symbol")] = (kind, time.time())

    def recent_trigger(self, symbol: str, within: float = 30) -> str | None:
        """'SL', 'TP' or 'TP/SL' if a TP/SL order on symbol fired in the last `within` seconds."""
        kind, ts = self._tpsl_triggers.get(self._norm(symbol), (None, 0))
        return kind if time.time() - ts <= within else None

    def ws_is_healthy(self) -> bool:
        """False when the socket thread died or nothing (not even a pong) arrived recently."""
        thread = self._ws_thread
        if thread is None or not thread.is_alive():
            return False
        return time.time() - self._ws_last_msg < self.WS_STALE_SECONDS

    def restart_private_ws(self):
        """Tear down the private WebSocket and open a fresh one."""
        on_order, on_position = self._ws_callbacks
        self.stop_ws()
        self.start_private_ws(on_order, on_position)

    def stop_ws(self):
        if self._ws_stop:
            self._ws_stop.set()
        app = self._ws_app
        self._ws_app = None
        if app:
            try:
                app.close()
            except Exception as e:
                log.warning("WebSocket close error: %s", e)
        if self._ws_thread and self._ws_thread is not threading.current_thread():
            self._ws_thread.join(timeout=10)
