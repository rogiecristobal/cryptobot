"""
Bybit V5 USDT perpetual (category=linear) REST + private WebSocket client.

Same interface and normalized dicts as bitunix_client.py (see its docstring),
so trade_manager.py and main.py run unchanged on either exchange. Needs a
Unified Trading Account. Bybit-specific differences, all handled here:

  - The SL and TP sit on the position itself (tpslMode=Full): one SL and one
    TP per position, no separate TP/SL orders to keep in step. position_id is
    Bybit's positionIdx (0 one-way; 1 long / 2 short in hedge mode).
  - Margin mode is account-wide on a unified account, so it is read
    (account_margin_mode), never changed by the bot.
  - Prices snap to the symbol's tickSize and quantities to its qtyStep.

Docs: https://bybit-exchange.github.io/docs/v5/intro
"""
import email.utils
import functools
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import hashlib
import hmac
import json
import logging
import threading
import time
import uuid
from urllib.parse import urlencode

import requests
import websocket

import config

log = logging.getLogger("bybit_client")

BASE_URL = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/private"
CATEGORY = "linear"
SETTLE_COIN = "USDT"
TRIGGER_BY = "MarkPrice"
RECV_WINDOW = "10000"

RATE_LIMIT_CODES = {10006, 10018}
PARAMS_ERROR = 10001
NOT_MODIFIED = {34040, 110043}  # trading-stop / leverage already at that value
ORDER_NOT_FOUND = {110001}
DUPLICATE_LINK_ID = 110072

# Bybit orderStatus -> the status names main.py expects (Bitunix's).
_STATUS = {
    "New": "NEW", "PartiallyFilled": "PART_FILLED", "Filled": "FILLED", "Cancelled": "CANCELED",
    "PartiallyFilledCanceled": "PART_FILLED_CANCELED", "Rejected": "REJECTED",
    "Untriggered": "UNTRIGGERED", "Triggered": "TRIGGERED", "Deactivated": "DEACTIVATED",
}
_REGULAR_ORDER = ("", "UNKNOWN")  # stopOrderType of a plain entry/DCA/limit order


class BybitError(Exception):
    """Bybit answered HTTP 200 with a non-zero retCode."""

    def __init__(self, code, msg, path):
        super().__init__(f"Bybit {path} error {code}: {msg}")
        try:
            self.code = int(code)
        except (TypeError, ValueError):
            self.code = code
        self.msg = msg


def _f(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_transient(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, BybitError):
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


def _decimals(step: Decimal) -> int:
    """Decimal places of a tick/step size: 0.10 -> 1, 0.001 -> 3, 5 -> 0."""
    return max(0, -step.normalize().as_tuple().exponent)


class BybitClient:
    # Bybit drops a private socket that sends nothing for 10 minutes; it
    # recommends a ping every 20s.
    WS_PING_SECONDS = 20
    # No message (not even a pong) for this long means the socket is dead.
    WS_STALE_SECONDS = 60
    WS_MAX_BACKOFF = 30

    def __init__(self):
        self.api_key = config.BYBIT_API_KEY
        self.api_secret = config.BYBIT_API_SECRET
        self.session = requests.Session()
        self.session.verify = config.SSL_CA_PATH
        self.timeout = 30
        self._instrument_cache = {}  # symbol -> (info, timestamp)
        self._cache_ttl = 300
        self._hedge_cache = {}  # symbol -> (is_hedge_mode, timestamp)
        self._margin_mode_cache = (None, 0.0)
        self._ws_app = None
        self._ws_thread = None
        self._ws_stop = None
        self._ws_logged_in = threading.Event()
        self.ws_login_error = None  # Bybit's reply when it last rejected the login, else None
        self._ws_last_msg = 0.0
        self._ws_callbacks = None
        self._ws_sizes = {}  # (symbol, positionIdx) -> last pushed size
        self._tpsl_triggers = {}  # symbol -> ("SL"|"TP", time) of the last fired TP/SL

    @staticmethod
    def _norm(symbol: str) -> str:
        return symbol.replace("/", "").replace(" ", "").upper()

    # ---------- REST plumbing ----------

    def _sign(self, payload: str) -> str:
        return hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, path: str, params: dict | None = None,
                 body: dict | None = None, auth: bool = True):
        params = {k: str(v) for k, v in (params or {}).items() if v is not None and v != ""}
        # The signature covers the exact query string / body bytes: sign and send the same string.
        query = urlencode(params)
        body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = {"Content-Type": "application/json"}
        if auth:
            timestamp = str(int(time.time() * 1000))
            signed = timestamp + self.api_key + RECV_WINDOW + (query if method == "GET" else body_str)
            headers.update({
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": RECV_WINDOW,
                "X-BAPI-SIGN": self._sign(signed),
            })
        url = BASE_URL + path + (f"?{query}" if query else "")
        resp = self.session.request(method, url, data=body_str or None, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        code = payload.get("retCode")
        if code not in (0, "0"):
            raise BybitError(code, payload.get("retMsg"), path)
        return payload.get("result") or {}

    def _get(self, path: str, auth: bool = True, **params):
        return self._request("GET", path, params=params, auth=auth)

    def _post(self, path: str, body: dict):
        return self._request("POST", path, body=body)

    # ---------- diagnostics ----------

    def clock_drift_seconds(self) -> float | None:
        """Local clock minus Bybit's. Signed requests fail if this exceeds the recv window (10s)."""
        try:
            resp = self.session.get(BASE_URL + "/v5/market/time", timeout=10)
            result = resp.json().get("result") or {}
            server = (_f(result.get("timeNano")) / 1e9 or _f(result.get("timeSecond"))
                      or email.utils.parsedate_to_datetime(resp.headers["Date"]).timestamp())
            return time.time() - server
        except Exception as e:
            log.warning("Could not measure clock drift against Bybit: %s", e)
            return None

    def get_position_mode(self) -> str:
        return "HEDGE" if self._is_hedge_mode("BTCUSDT") else "ONE_WAY"

    def check_account(self) -> list[str]:
        """Account problems to report at startup: the bot needs a unified account."""
        info = self._get("/v5/account/info")
        status = int(_f(info.get("unifiedMarginStatus")))
        log.info("Bybit account: unifiedMarginStatus=%s marginMode=%s", status, info.get("marginMode"))
        if status == 1:
            return ["This Bybit account is a classic account. The bot needs a Unified Trading Account — "
                    "upgrade it in the Bybit app (Assets → Upgrade to Unified Trading Account)."]
        if info.get("marginMode") == "PORTFOLIO_MARGIN":
            return ["Bybit account is in Portfolio Margin mode. The bot sizes and labels trades as CROSS."]
        return []

    # ---------- account / instrument info ----------

    def _wallet(self) -> tuple[dict, dict]:
        data = self._get("/v5/account/wallet-balance", accountType="UNIFIED", coin=SETTLE_COIN)
        accounts = data.get("list") or []
        if not accounts:
            raise RuntimeError("Empty wallet response from Bybit")
        coin = next((c for c in accounts[0].get("coin") or [] if c.get("coin") == SETTLE_COIN), {})
        return accounts[0], coin

    def get_equity_usdt(self) -> float:
        """USDT wallet balance (excluding unrealized PnL). Other collateral coins aren't counted."""
        _, coin = self._wallet()
        return _f(coin.get("walletBalance"))

    def get_wallet_info(self) -> dict:
        account, coin = self._wallet()
        balance = _f(coin.get("walletBalance"))
        # totalAvailableBalance is empty in isolated margin mode: work it out from the margin in use.
        available = _f(account.get("totalAvailableBalance")) or max(
            0.0, balance - _f(coin.get("totalPositionIM")) - _f(coin.get("totalOrderIM")))
        return {"equity": balance, "available": available, "unrealized_pnl": _f(coin.get("unrealisedPnl"))}

    def account_margin_mode(self) -> str | None:
        """'CROSS' or 'ISOLATED': a unified account's margin mode covers every symbol."""
        mode, ts = self._margin_mode_cache
        if mode and time.time() - ts < 60:
            return mode
        try:
            raw = str(self._get("/v5/account/info").get("marginMode") or "").upper()
        except Exception as e:
            if mode:
                log.warning("Could not refresh Bybit margin mode (%s) — using last known %s", e, mode)
                return mode
            raise
        mode = "ISOLATED" if raw == "ISOLATED_MARGIN" else "CROSS"
        self._margin_mode_cache = (mode, time.time())
        return mode

    def get_instrument_info(self, symbol: str) -> dict:
        symbol = self._norm(symbol)
        now = time.time()
        if symbol in self._instrument_cache:
            info, ts = self._instrument_cache[symbol]
            if now - ts < self._cache_ttl:
                return info
        # Low-priced coins trade in lots of 1000 on Bybit: PEPE is 1000PEPEUSDT.
        not_listed = (f"{symbol} is not listed on Bybit USDT perpetuals. Some coins trade there in lots of "
                      f"1000 — try 1000{symbol.removesuffix('USDT')}.")
        try:
            data = self._get("/v5/market/instruments-info", auth=False, category=CATEGORY, symbol=symbol)
        except BybitError as e:
            if e.code == PARAMS_ERROR:
                raise ValueError(not_listed) from e
            raise
        info = next((d for d in data.get("list") or [] if d.get("symbol") == symbol), None)
        if not info:
            raise ValueError(not_listed)
        self._instrument_cache[symbol] = (info, now)
        return info

    def check_tradable(self, symbol: str):
        """Raise ValueError (shown to the user) if Bybit won't accept orders on this symbol."""
        info = self.get_instrument_info(symbol)
        status = str(info.get("status") or "Trading")
        if status != "Trading":
            raise ValueError(f"{self._norm(symbol)} is not tradable on Bybit right now (status {status}).")

    def get_max_leverage(self, symbol: str) -> int:
        """Bybit's maximum leverage for the symbol, or 0 if it doesn't say."""
        return int(_f((self.get_instrument_info(symbol).get("leverageFilter") or {}).get("maxLeverage")))

    def _lot(self, symbol: str) -> dict:
        return self.get_instrument_info(symbol).get("lotSizeFilter") or {}

    def _qty_step(self, symbol: str) -> Decimal:
        return Decimal(str(self._lot(symbol).get("qtyStep") or "1"))

    def _tick(self, symbol: str) -> Decimal:
        return Decimal(str((self.get_instrument_info(symbol).get("priceFilter") or {}).get("tickSize") or "1"))

    def round_qty(self, symbol: str, qty: float) -> float:
        """
        Round DOWN to a multiple of Bybit's qtyStep, so rounding never adds
        risk. Below the symbol's minimum it returns the minimum — callers must
        check the risk of the qty they actually get.
        """
        symbol = self._norm(symbol)
        step = self._qty_step(symbol)
        floored = float((Decimal(repr(qty)) / step).to_integral_value(rounding=ROUND_DOWN) * step)
        return max(floored, _f(self._lot(symbol).get("minOrderQty")))

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        return f"{qty:.{_decimals(self._qty_step(symbol))}f}"

    def round_price(self, symbol: str, price: float) -> float:
        """Nearest multiple of the symbol's tickSize (which isn't always a power of ten)."""
        symbol = self._norm(symbol)
        tick = self._tick(symbol)
        result = float((Decimal(repr(price)) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick)
        if result == 0 and price > 0:
            log.warning("round_price(%s, %s, tick=%s) rounded to 0 — keeping raw price", symbol, price, tick)
            result = price
        return result

    def _fmt_price(self, symbol: str, price: float) -> str:
        return f"{price:.{_decimals(self._tick(symbol))}f}"

    def format_price(self, symbol: str, price: float) -> str:
        """Price (or price distance) at the symbol's own precision, for messages."""
        return self._fmt_price(self._norm(symbol), price)

    def get_tickers(self, symbols: list[str]) -> dict:
        symbols = [self._norm(s) for s in symbols]
        if len(symbols) > 5:  # one call for all linear tickers beats many single ones
            data = self._get("/v5/market/tickers", auth=False, category=CATEGORY)
            wanted = set(symbols)
            return {t["symbol"]: t for t in data.get("list") or [] if t.get("symbol") in wanted}
        tickers = {}
        for s in symbols:
            for t in self._get("/v5/market/tickers", auth=False, category=CATEGORY, symbol=s).get("list") or []:
                tickers[t.get("symbol")] = t
        return tickers

    def get_mark_prices(self, symbols: list[str]) -> dict:
        return {s: _f(t.get("markPrice")) for s, t in self.get_tickers(symbols).items()
                if _f(t.get("markPrice")) > 0}

    def get_mark_price(self, symbol: str) -> float:
        symbol = self._norm(symbol)
        mark = self.get_mark_prices([symbol]).get(symbol)
        if not mark:
            raise RuntimeError(f"No mark price for {symbol} from Bybit")
        return mark

    def get_last_price(self, symbol: str) -> float:
        symbol = self._norm(symbol)
        last = _f((self.get_tickers([symbol]).get(symbol) or {}).get("lastPrice"))
        if not last:
            raise RuntimeError(f"No last price for {symbol} from Bybit")
        return last

    # ---------- position / leverage setup ----------

    def set_leverage(self, symbol: str, leverage: int):
        try:
            self._post("/v5/position/set-leverage",
                       {"category": CATEGORY, "symbol": self._norm(symbol),
                        "buyLeverage": str(int(leverage)), "sellLeverage": str(int(leverage))})
        except BybitError as e:
            if e.code not in NOT_MODIFIED:
                raise

    def set_margin_mode(self, symbol: str, mode: str):
        """Margin mode is account-wide on Bybit: only check it, never flip the whole account."""
        target = "ISOLATED" if mode.upper().startswith("ISOLAT") else "CROSS"
        current = self.account_margin_mode()
        if current != target:
            raise ValueError(f"Your Bybit account is in {current} margin, not {target}. Bybit sets this for "
                             f"the whole account — change it in the Bybit app if you want {target}.")

    def _symbol_positions(self, symbol: str) -> list:
        """Every position slot of the symbol, empty ones included (one per side in hedge mode)."""
        data = self._get("/v5/position/list", category=CATEGORY, symbol=self._norm(symbol))
        return data.get("list") or []

    def _is_hedge_mode(self, symbol: str) -> bool:
        symbol = self._norm(symbol)
        hedge, ts = self._hedge_cache.get(symbol, (None, 0.0))
        if hedge is None or time.time() - ts > self._cache_ttl:
            hedge = any(int(_f(p.get("positionIdx"))) in (1, 2) for p in self._symbol_positions(symbol))
            self._hedge_cache[symbol] = (hedge, time.time())
        return hedge

    def _position_idx(self, symbol: str, side: str) -> int:
        """positionIdx for an opening order: 0 in one-way mode, 1 (BUY) / 2 (SELL) in hedge mode."""
        if not self._is_hedge_mode(symbol):
            return 0
        return 1 if side.upper() in ("BUY", "LONG") else 2

    def _raw_positions(self, symbol: str | None = None) -> list:
        if symbol:
            positions = self._symbol_positions(symbol)
        else:
            positions = self._get("/v5/position/list", category=CATEGORY,
                                  settleCoin=SETTLE_COIN, limit=200).get("list") or []
        return [p for p in positions if _f(p.get("size")) > 0]

    def _raw_position(self, symbol: str) -> dict | None:
        symbol = self._norm(symbol)
        return next((p for p in self._raw_positions(symbol) if p.get("symbol") == symbol), None)

    def get_tpsl_orders(self, symbol: str, position_id: str | None = None) -> list:
        """Bybit keeps the SL/TP on the position, not in separate TP/SL orders: there are none to list."""
        return []

    @staticmethod
    def _normalize_position(p: dict) -> dict:
        side = {"Buy": "LONG", "Sell": "SHORT"}.get(p.get("side"), str(p.get("side") or "").upper())
        sl, tp = _f(p.get("stopLoss")), _f(p.get("takeProfit"))
        return {
            "symbol": p.get("symbol"),
            "position_id": str(int(_f(p.get("positionIdx")))),
            "side": side,
            "size": _f(p.get("size")),
            "avg_price": _f(p.get("avgPrice")),
            "unrealized_pnl": _f(p.get("unrealisedPnl")),
            "leverage": int(_f(p.get("leverage")) or 1),
            # Full-mode SL/TP always cover the whole position.
            "stop_loss": sl,
            "sl_prices": [sl] if sl > 0 else [],
            "take_profit": tp,
            "tp_prices": [tp] if tp > 0 else [],
        }

    def get_all_open_positions(self) -> list:
        return [self._normalize_position(p) for p in self._raw_positions()]

    def get_open_position(self, symbol: str) -> dict | None:
        raw = self._raw_position(symbol)
        return self._normalize_position(raw) if raw else None

    def has_open_orders_or_position(self, symbol: str) -> bool:
        if self.get_open_orders(symbol):
            return True
        return bool(self._raw_positions(symbol))

    # ---------- orders ----------

    @staticmethod
    def _normalize_order(o: dict) -> dict:
        return {
            "order_id": str(o.get("orderId") or ""),
            "client_id": o.get("orderLinkId"),
            "symbol": o.get("symbol"),
            "side": str(o.get("side") or "").upper(),
            "order_type": str(o.get("orderType") or "").upper(),
            "status": _STATUS.get(o.get("orderStatus"), str(o.get("orderStatus") or "").upper()),
            "qty": _f(o.get("qty")),
            "price": _f(o.get("price")),
            "reduce_only": bool(o.get("reduceOnly")),
            "stop_loss": _f(o.get("stopLoss")),
            "take_profit": _f(o.get("takeProfit")),
        }

    def get_open_orders(self, symbol: str) -> list:
        """Regular open orders (entry/DCA/limit). The position's own SL/TP orders are left out, as on Bitunix."""
        data = self._get("/v5/order/realtime", category=CATEGORY, symbol=self._norm(symbol), limit=50)
        return [self._normalize_order(o) for o in data.get("list") or []
                if str(o.get("stopOrderType") or "") in _REGULAR_ORDER]

    def find_order(self, symbol: str, client_id: str) -> dict | None:
        """Look an order up by orderLinkId, open or historical."""
        for path in ("/v5/order/realtime", "/v5/order/history"):
            try:
                found = self._get(path, category=CATEGORY, symbol=self._norm(symbol),
                                  orderLinkId=client_id).get("list") or []
                if found:
                    return self._normalize_order(found[0])
            except Exception as e:
                log.warning("Order lookup (%s) failed for %s: %s", path, client_id, e)
        return None

    def _add_tpsl(self, body: dict, symbol: str, stop_loss: float | None, take_profit: float | None):
        if stop_loss is not None:
            body.update(stopLoss=self._fmt_price(symbol, stop_loss), slTriggerBy=TRIGGER_BY)
        if take_profit is not None:
            body.update(takeProfit=self._fmt_price(symbol, take_profit), tpTriggerBy=TRIGGER_BY)

    def _place_order_once(self, body: dict, max_attempts: int = 3) -> str:
        """
        Place an order with a client-side orderLinkId so a retry can never
        create a second order: if a timed-out request actually reached Bybit,
        the retry is rejected as a duplicate (or found by lookup) and the
        original order id is returned instead. Returns the Bybit orderId.
        """
        client_id = uuid.uuid4().hex
        body["orderLinkId"] = client_id
        symbol = body["symbol"]
        for attempt in range(max_attempts):
            try:
                return str(self._post("/v5/order/create", body)["orderId"])
            except Exception as e:
                if isinstance(e, BybitError) and e.code == DUPLICATE_LINK_ID:
                    existing = self.find_order(symbol, client_id)
                    if existing:
                        log.warning("Order %s already accepted by Bybit — not placing again", client_id)
                        return existing["order_id"]
                    raise
                if not _is_transient(e) or attempt == max_attempts - 1:
                    raise
                existing = self.find_order(symbol, client_id)
                if existing:
                    log.warning("Order %s reached Bybit despite error (%s) — not placing again", client_id, e)
                    return existing["order_id"]
                log.warning("Retrying order/create %s after: %s (attempt %d/%d)",
                            client_id, e, attempt + 1, max_attempts)
                time.sleep(attempt + 1)

    def _opening_body(self, symbol: str, side: str, qty: float, order_type: str,
                      stop_loss: float | None, take_profit: float | None) -> dict:
        body = {"category": CATEGORY, "symbol": symbol, "side": "Buy" if side.upper() == "BUY" else "Sell",
                "orderType": order_type, "qty": self._fmt_qty(symbol, qty),
                "positionIdx": self._position_idx(symbol, side)}
        self._add_tpsl(body, symbol, stop_loss, take_profit)
        if stop_loss is not None or take_profit is not None:
            body["tpslMode"] = "Full"  # on fill they become the position's own SL/TP
        return body

    def place_market_order(self, symbol: str, side: str, qty: float,
                           stop_loss: float | None = None,
                           take_profit: float | None = None) -> str:
        """Opening market order. side is BUY or SELL. Returns the order id."""
        symbol = self._norm(symbol)
        return self._place_order_once(self._opening_body(symbol, side, qty, "Market", stop_loss, take_profit))

    def place_limit_order(self, symbol: str, side: str, qty: float, price: float,
                          stop_loss: float | None = None,
                          take_profit: float | None = None) -> str:
        """Opening GTC limit order. side is BUY or SELL. Returns the order id."""
        symbol = self._norm(symbol)
        body = self._opening_body(symbol, side, qty, "Limit", stop_loss, take_profit)
        body.update(price=self._fmt_price(symbol, price), timeInForce="GTC")
        return self._place_order_once(body)

    def amend_order_tpsl(self, symbol: str, order: dict, sl_price: float, tp_price: float | None):
        """Change the SL/TP attached to a resting (unfilled) order. tp_price None keeps its current TP."""
        symbol = self._norm(symbol)
        body = {"category": CATEGORY, "symbol": symbol, "orderId": order["order_id"]}
        self._add_tpsl(body, symbol, sl_price, tp_price)
        self._post("/v5/order/amend", body)

    def _trading_stop(self, symbol: str, raw: dict, **fields) -> bool:
        """Set the position's own SL/TP. Fields left out keep their value. False if nothing changed."""
        body = {"category": CATEGORY, "symbol": symbol, "tpslMode": "Full",
                "positionIdx": int(_f(raw.get("positionIdx"))), **fields}
        try:
            self._post("/v5/position/trading-stop", body)
        except BybitError as e:
            if e.code in NOT_MODIFIED:
                return False
            raise
        return True

    @_retry()
    def set_position_sl(self, symbol: str, sl_price: float):
        """Put sl_price on the open position (it covers the whole position). Raises on failure."""
        symbol = self._norm(symbol)
        if sl_price is None or sl_price <= 0:
            raise ValueError(f"Refusing to set invalid SL {sl_price} on {symbol}")
        raw = self._raw_position(symbol)
        if not raw:
            raise RuntimeError(f"No open {symbol} position to attach an SL to")
        sl_str = self._fmt_price(symbol, sl_price)
        current = _f(raw.get("stopLoss"))
        if current > 0 and abs(current - float(sl_str)) <= float(sl_str) * 1e-9:
            return
        self._trading_stop(symbol, raw, stopLoss=sl_str, slTriggerBy=TRIGGER_BY)
        log.info("%s: SL %s -> %s", symbol, raw.get("stopLoss") or "none", sl_str)

    @_retry()
    def set_position_tp(self, symbol: str, tp_price: float, exclude_ids=()) -> bool:
        """Put tp_price on the open position. Returns True if it changed; raises on failure."""
        symbol = self._norm(symbol)
        if tp_price is None or tp_price <= 0:
            raise ValueError(f"Refusing to set invalid TP {tp_price} on {symbol}")
        raw = self._raw_position(symbol)
        if not raw:
            raise RuntimeError(f"No open {symbol} position to attach a TP to")
        tp_str = self._fmt_price(symbol, tp_price)
        current = _f(raw.get("takeProfit"))
        if current > 0 and abs(current - float(tp_str)) <= float(tp_str) * 1e-9:
            return False
        changed = self._trading_stop(symbol, raw, takeProfit=tp_str, tpTriggerBy=TRIGGER_BY)
        if changed:
            log.info("%s: TP %s -> %s", symbol, raw.get("takeProfit") or "none", tp_str)
        return changed

    def cancel_tpsl(self, symbol: str, order_id: str):
        self._post("/v5/order/cancel", {"category": CATEGORY, "symbol": self._norm(symbol), "orderId": str(order_id)})

    def cancel_order(self, symbol: str, order_id: str):
        try:
            self._post("/v5/order/cancel",
                       {"category": CATEGORY, "symbol": self._norm(symbol), "orderId": str(order_id)})
        except Exception as e:
            log.warning("Cancel failed for %s (%s): %s", order_id, symbol, e)

    @_retry()
    def cancel_all(self, symbol: str):
        """Cancel all regular (entry/DCA/limit) orders on the symbol. The position's SL/TP is untouched."""
        symbol = self._norm(symbol)
        for o in self.get_open_orders(symbol):
            try:
                self._post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderId": o["order_id"]})
            except BybitError as e:
                if e.code not in ORDER_NOT_FOUND:  # filled or cancelled meanwhile: nothing to do
                    raise

    def close_position_market(self, symbol: str) -> float:
        """Market-close the whole open position, if any. Returns the qty closed (0 if none)."""
        symbol = self._norm(symbol)
        raw = self._raw_position(symbol)
        if not raw:
            return 0.0
        size = Decimal(str(raw.get("size")))
        # A market order can't exceed maxMktOrderQty: close a large position in chunks.
        max_chunk = Decimal(str(self._lot(symbol).get("maxMktOrderQty") or raw.get("size")))
        remaining = size
        while remaining > 0:
            chunk = min(remaining, max_chunk)
            self._place_order_once({
                "category": CATEGORY, "symbol": symbol, "side": "Sell" if raw.get("side") == "Buy" else "Buy",
                "orderType": "Market", "qty": self._fmt_qty(symbol, float(chunk)), "reduceOnly": True,
                "positionIdx": int(_f(raw.get("positionIdx"))),
            })
            remaining -= chunk
        return float(size)

    # ---------- private websocket (orders / positions) ----------

    @staticmethod
    def _normalize_order_event(d: dict) -> dict:
        return {
            "symbol": d.get("symbol"),
            "order_id": str(d.get("orderId") or ""),
            "client_id": d.get("orderLinkId"),
            "event": str(d.get("createType") or "").upper(),
            "status": _STATUS.get(d.get("orderStatus"), str(d.get("orderStatus") or "").upper()),
            "side": str(d.get("side") or "").upper(),
            "order_type": str(d.get("orderType") or "").upper(),
            "qty": _f(d.get("qty")),
            "avg_price": _f(d.get("avgPrice")) or _f(d.get("price")),
            "reduce_only": bool(d.get("reduceOnly")),
        }

    def _normalize_position_event(self, d: dict) -> dict | None:
        """
        Bybit pushes the position after every change (fills, but also SL/TP
        and leverage edits) with no event type. OPEN = it grew, CLOSE = it went
        to 0, UPDATE = anything else. None for an empty slot that stayed empty
        (the other side in hedge mode, a setting change).
        """
        symbol = d.get("symbol")
        key = (symbol, d.get("positionIdx"))
        size = _f(d.get("size"))
        previous = self._ws_sizes.get(key)
        self._ws_sizes[key] = size
        if size <= 0:
            if previous == 0:
                return None
            event = "CLOSE"
        else:
            event = "OPEN" if previous is None or size > previous else "UPDATE"
        return {
            "symbol": symbol,
            "position_id": str(d.get("positionIdx") or 0),
            "event": event,
            "side": {"Buy": "LONG", "Sell": "SHORT"}.get(d.get("side"), str(d.get("side") or "").upper()),
            "size": size,
        }

    def start_private_ws(self, on_order, on_position):
        """Start the private socket in a background thread; waits up to 30s for the first login."""
        self._ws_callbacks = (on_order, on_position)
        self._ws_stop = threading.Event()
        self._ws_logged_in.clear()
        self._ws_last_msg = time.time()
        self._ws_thread = threading.Thread(target=self._ws_run, args=(self._ws_stop,),
                                           daemon=True, name="bybit-ws")
        self._ws_thread.start()
        if not self._ws_logged_in.wait(30):
            log.warning("Bybit private WebSocket not logged in after 30s — still retrying in the background")

    def _ws_run(self, stop: threading.Event):
        backoff = 1
        while not stop.is_set():
            connected_at = time.time()
            app = websocket.WebSocketApp(
                WS_URL,
                on_open=self._ws_on_open,
                on_message=self._ws_on_message,
                on_error=lambda _ws, e: log.warning("Bybit WebSocket error: %s", e),
                on_close=lambda _ws, code, msg: log.warning("Bybit WebSocket closed (%s %s)", code, msg),
            )
            self._ws_app = app
            try:
                # Protocol pings off: Bybit wants JSON pings, sent by _ws_pinger.
                app.run_forever(ping_interval=0, sslopt={"ca_certs": config.SSL_CA_PATH})
            except Exception as e:
                log.warning("Bybit WebSocket crashed: %s", e)
            self._ws_logged_in.clear()
            if stop.is_set():
                break
            if time.time() - connected_at > 60:
                backoff = 1
            log.warning("Bybit WebSocket reconnecting in %ss", backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, self.WS_MAX_BACKOFF)

    def _ws_on_open(self, app):
        expires = int((time.time() + 10) * 1000)
        app.send(json.dumps({"op": "auth", "args": [self.api_key, expires, self._sign(f"GET/realtime{expires}")]}))
        threading.Thread(target=self._ws_pinger, args=(app,), daemon=True, name="bybit-ws-ping").start()

    def _ws_pinger(self, app):
        while self._ws_app is app and not self._ws_stop.wait(self.WS_PING_SECONDS):
            try:
                app.send(json.dumps({"op": "ping"}))
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
        if op == "auth":
            if not msg.get("success"):
                log.error("Bybit WebSocket login rejected: %s", msg)
                self.ws_login_error = str(msg.get("ret_msg") or msg)[:200]
                return
            log.info("Bybit private WebSocket logged in")
            self.ws_login_error = None
            self._ws_logged_in.set()
            app.send(json.dumps({"op": "subscribe", "args": ["order", "position"]}))
            return
        if op == "subscribe":
            if not msg.get("success"):
                log.error("Bybit WebSocket subscribe failed: %s", msg)
            return
        if op in ("ping", "pong"):
            return

        topic = msg.get("topic")
        on_order, on_position = self._ws_callbacks
        for item in msg.get("data") or []:
            if item.get("category") not in (None, CATEGORY):
                continue  # spot / options on the same account
            try:
                if topic == "order":
                    self._record_trigger(item)
                    on_order(self._normalize_order_event(item))
                elif topic == "position":
                    event = self._normalize_position_event(item)
                    if event:
                        on_position(event)
            except Exception:
                log.exception("WebSocket %s handler failed for %s", topic, item)

    def _record_trigger(self, item: dict):
        # The order a fired SL/TP turns into says which one it was; remember
        # it so the position close that follows can be labelled.
        if item.get("orderStatus") != "Filled":
            return
        source = f"{item.get('stopOrderType') or ''} {item.get('createType') or ''}"
        if "StopLoss" in source or "TrailingStop" in source:
            kind = "SL"
        elif "TakeProfit" in source:
            kind = "TP"
        else:
            return
        self._tpsl_triggers[item.get("symbol")] = (kind, time.time())

    def recent_trigger(self, symbol: str, within: float = 30) -> str | None:
        """'SL' or 'TP' if the position's SL/TP on symbol fired in the last `within` seconds."""
        kind, ts = self._tpsl_triggers.get(self._norm(symbol), (None, 0))
        return kind if time.time() - ts <= within else None

    def ws_is_healthy(self) -> bool:
        """False when the socket thread died, nothing (not even a pong) arrived recently, or it isn't logged in."""
        thread = self._ws_thread
        if thread is None or not thread.is_alive():
            return False
        if not self._ws_logged_in.is_set():
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
