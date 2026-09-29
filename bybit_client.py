"""
Thin wrapper around pybit's unified trading HTTP + private WebSocket.
All Bybit-specific calls live here so trade_manager.py stays exchange-agnostic-ish.
"""
import functools
import time
import logging
import uuid
from decimal import Decimal
from pybit.unified_trading import HTTP, WebSocket
import config

log = logging.getLogger("bybit_client")


def _retry(max_attempts=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    msg = str(e).lower()
                    if "timeout" in msg or "rate limit" in msg or "too many requests" in msg:
                        log.warning("Retrying %s after: %s (attempt %d/%d)",
                                    func.__name__, e, attempt + 1, max_attempts)
                        time.sleep(delay * (attempt + 1))
                    else:
                        raise
            raise last_exc
        return wrapper
    return decorator


class BybitClient:
    def __init__(self):
        self.http = HTTP(
            api_key=config.BYBIT_API_KEY,
            api_secret=config.BYBIT_API_SECRET,
            testnet=False,
            timeout=30,
        )
        self.category = config.BYBIT_CATEGORY
        self._instrument_cache = {}  # symbol -> (info, timestamp)
        self._cache_ttl = 300
        self._ws = None

    def _norm(self, symbol: str) -> str:
        return symbol.replace("/", "").replace(" ", "")

    @staticmethod
    def _decimal_places(value: float) -> int:
        return abs(Decimal(str(value)).as_tuple().exponent)

    # ---------- account / instrument info ----------

    def get_equity_usdt(self) -> float:
        resp = self.http.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        try:
            return float(resp["result"]["list"][0]["coin"][0]["walletBalance"])
        except (KeyError, IndexError, TypeError):
            log.debug("Raw wallet response: %s", resp)
            raise RuntimeError("Could not parse equity from Bybit wallet response")

    def get_wallet_info(self) -> dict:
        """Returns equity and available balance as a dict."""
        resp = self.http.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        try:
            coin = resp["result"]["list"][0]["coin"][0]
            return {
                "equity": float(coin["walletBalance"]),
                "available": float(coin.get("availableToWithdraw") or 0),
            }
        except (KeyError, IndexError, TypeError):
            log.debug("Raw wallet response: %s", resp)
            raise RuntimeError("Could not parse wallet info from Bybit response")

    def get_instrument_info(self, symbol: str) -> dict:
        symbol = self._norm(symbol)
        now = time.time()
        if symbol in self._instrument_cache:
            info, ts = self._instrument_cache[symbol]
            if now - ts < self._cache_ttl:
                return info
        resp = self.http.get_instruments_info(category=self.category, symbol=symbol)
        info = resp["result"]["list"][0]
        self._instrument_cache[symbol] = (info, now)
        return info

    def get_max_leverage(self, symbol: str) -> int:
        info = self.get_instrument_info(symbol)
        return int(float(info["leverageFilter"]["maxLeverage"]))

    def round_qty(self, symbol: str, qty: float) -> float:
        symbol = self._norm(symbol)
        info = self.get_instrument_info(symbol)
        step = float(info["lotSizeFilter"]["qtyStep"])
        min_qty = float(info["lotSizeFilter"]["minOrderQty"])
        rounded = round(qty / step) * step
        return max(rounded, min_qty)

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        info = self.get_instrument_info(symbol)
        step = float(info["lotSizeFilter"]["qtyStep"])
        decimals = self._decimal_places(step)
        return f"{qty:.{decimals}f}"

    def round_price(self, symbol: str, price: float) -> float:
        symbol = self._norm(symbol)
        info = self.get_instrument_info(symbol)
        tick = float(info["priceFilter"]["tickSize"])
        decimals = self._decimal_places(tick)
        rounded = round(price / tick) * tick
        result = round(rounded, decimals)
        if result == 0 and price > 0:
            price_decimals = self._decimal_places(price)
            result = round(price, max(decimals, price_decimals))
            log.warning("round_price(%s, %s, tick=%s, decimals=%s) -> %s",
                        symbol, price, tick, max(decimals, price_decimals), result)
            if result == 0:
                result = price
        return result

    def _fmt_price(self, symbol: str, price: float) -> str:
        info = self.get_instrument_info(symbol)
        tick = float(info["priceFilter"]["tickSize"])
        decimals = self._decimal_places(tick)
        return f"{price:.{decimals}f}"

    # ---------- position / leverage setup ----------

    def set_leverage(self, symbol: str, leverage: int):
        symbol = self._norm(symbol)
        try:
            self.http.set_leverage(
                category=self.category,
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage),
            )
        except Exception as e:
            # Bybit throws if leverage is already set to this value — safe to ignore
            if "leverage not modified" not in str(e).lower():
                raise

    def set_margin_mode(self, symbol: str, mode: str, leverage: int = 0):
        symbol = self._norm(symbol)
        trade_mode = 1 if mode.upper() == "ISOLATED" else 0
        lev = min(leverage, self.get_max_leverage(symbol)) if leverage else config.DEFAULT_LEVERAGE
        try:
            self.http.switch_margin_mode(
                category=self.category,
                symbol=symbol,
                tradeMode=trade_mode,
                buyLeverage=str(lev),
                sellLeverage=str(lev),
            )
        except Exception as e:
            msg = str(e).lower()
            if "unified account is forbidden" in msg:
                log.warning("UTA account detected — margin mode is set at account level, skipping switch.")
                return
            if "not modified" in msg:
                return
            raise

    def get_all_open_positions(self) -> list:
        """Return all open USDT perpetual positions (no symbol filter)."""
        resp = self.http.get_positions(category=self.category, settleCoin="USDT")
        return [
            pos for pos in resp["result"]["list"]
            if float(pos.get("size", 0)) > 0
        ]

    def get_open_position(self, symbol: str) -> dict | None:
        symbol = self._norm(symbol)
        resp = self.http.get_positions(category=self.category, symbol=symbol)
        for pos in resp["result"]["list"]:
            if float(pos.get("size", 0)) > 0:
                return pos
        return None

    def has_open_orders_or_position(self, symbol: str) -> bool:
        symbol = self._norm(symbol)
        resp = self.http.get_open_orders(category=self.category, symbol=symbol)
        if resp["result"]["list"]:
            return True
        return self.get_open_position(symbol) is not None

    # ---------- orders ----------

    def _add_tpsl(self, body: dict, symbol: str, stop_loss: float | None, take_profit: float | None):
        has_tpsl = False
        if stop_loss is not None:
            body["stopLoss"] = self._fmt_price(symbol, stop_loss)
            body["slTriggerBy"] = "MarkPrice"
            body["slOrderType"] = "Market"
            has_tpsl = True
        if take_profit is not None:
            body["takeProfit"] = self._fmt_price(symbol, take_profit)
            body["tpTriggerBy"] = "MarkPrice"
            body["tpOrderType"] = "Market"
            has_tpsl = True
        if has_tpsl:
            body["tpslMode"] = "Full"

    def find_order(self, symbol: str, order_link_id: str) -> dict | None:
        """Look an order up by orderLinkId in open orders, then in order history."""
        symbol = self._norm(symbol)
        for fetch in (self.http.get_open_orders, self.http.get_order_history):
            try:
                resp = fetch(category=self.category, symbol=symbol, orderLinkId=order_link_id)
                orders = resp["result"]["list"]
                if orders:
                    return orders[0]
            except Exception as e:
                log.warning("Order lookup (%s) failed for %s: %s", fetch.__name__, order_link_id, e)
        return None

    def _place_order_once(self, body: dict, max_attempts: int = 3):
        """
        Place an order with a client-side orderLinkId so a retry can never
        create a second order: if a timed-out request actually reached Bybit,
        the retry is rejected as a duplicate (or found by lookup) and the
        original order is returned instead of placing another one.
        """
        link_id = uuid.uuid4().hex
        body["orderLinkId"] = link_id
        symbol = body["symbol"]
        for attempt in range(max_attempts):
            try:
                return self.http.place_order(**body)
            except Exception as e:
                msg = str(e).lower()
                code = getattr(e, "status_code", None)
                if code == 110072 or "duplicate" in msg:
                    existing = self.find_order(symbol, link_id)
                    if existing:
                        log.warning("Order %s already accepted by Bybit — not placing again", link_id)
                        return {"result": {"orderId": existing["orderId"], "orderLinkId": link_id}}
                    raise
                transient = any(s in msg for s in ("timeout", "timed out", "rate limit",
                                                   "too many requests", "connection", "ssl"))
                if not transient or attempt == max_attempts - 1:
                    raise
                existing = self.find_order(symbol, link_id)
                if existing:
                    log.warning("Order %s reached Bybit despite error (%s) — not placing again", link_id, e)
                    return {"result": {"orderId": existing["orderId"], "orderLinkId": link_id}}
                log.warning("Retrying place_order %s after: %s (attempt %d/%d)",
                            link_id, e, attempt + 1, max_attempts)
                time.sleep(attempt + 1)

    def place_market_order(self, symbol: str, side: str, qty: float, reduce_only=False,
                           stop_loss: float | None = None,
                           take_profit: float | None = None):
        symbol = self._norm(symbol)
        body = dict(
            category=self.category, symbol=symbol, side=side,
            orderType="Market", qty=self._fmt_qty(symbol, qty), reduceOnly=reduce_only,
        )
        self._add_tpsl(body, symbol, stop_loss, take_profit)
        return self._place_order_once(body)

    def place_limit_order(self, symbol: str, side: str, qty: float, price: float, reduce_only=False,
                          stop_loss: float | None = None,
                          take_profit: float | None = None):
        symbol = self._norm(symbol)
        body = dict(
            category=self.category, symbol=symbol, side=side,
            orderType="Limit", qty=self._fmt_qty(symbol, qty), price=self._fmt_price(symbol, price),
            timeInForce="GTC", reduceOnly=reduce_only,
        )
        self._add_tpsl(body, symbol, stop_loss, take_profit)
        return self._place_order_once(body)

    def get_open_orders(self, symbol: str) -> list:
        symbol = self._norm(symbol)
        resp = self.http.get_open_orders(category=self.category, symbol=symbol)
        return resp["result"]["list"]

    def amend_order_sl(self, symbol: str, order_id: str, sl_price: float):
        """Change the SL attached to a resting (unfilled) order."""
        symbol = self._norm(symbol)
        try:
            self.http.amend_order(
                category=self.category, symbol=symbol, orderId=order_id,
                stopLoss=self._fmt_price(symbol, sl_price), slTriggerBy="MarkPrice",
            )
        except Exception as e:
            if "not modified" in str(e).lower():
                return
            raise

    @_retry()
    def set_position_sl(self, symbol: str, sl_price: float, trigger_by: str = "MarkPrice",
                        position_idx: int = 0):
        """Set the position-level SL. Raises on failure so callers can alert."""
        symbol = self._norm(symbol)
        if sl_price is None or sl_price <= 0:
            # Bybit treats stopLoss=0 as "remove SL" — never send it by accident.
            raise ValueError(f"Refusing to set invalid SL {sl_price} on {symbol}")
        try:
            self.http.set_trading_stop(
                category=self.category, symbol=symbol,
                stopLoss=self._fmt_price(symbol, sl_price),
                slTriggerBy=trigger_by,
                slOrderType="Market",
                tpslMode="Full",
                positionIdx=position_idx,
            )
        except Exception as e:
            msg = str(e).lower()
            if "not modified" in msg:
                return
            log.warning("set_trading_stop failed for %s (idx=%s, sl=%s): %s", symbol, position_idx, sl_price, e)
            raise

    def set_trailing_stop(self, symbol: str, distance: float, activation: float | None = None):
        symbol = self._norm(symbol)
        kwargs = dict(
            category=self.category, symbol=symbol,
            trailingStop=str(distance),
            tpslMode="Full",
            positionIdx=0,
        )
        if activation is not None:
            kwargs["activePrice"] = self._fmt_price(symbol, activation)
        try:
            self.http.set_trading_stop(**kwargs)
        except Exception as e:
            msg = str(e).lower()
            if "not modified" in msg:
                return
            log.warning("set_trailing_stop failed for %s: %s", symbol, e)
            raise

    def cancel_trailing_stop(self, symbol: str):
        symbol = self._norm(symbol)
        try:
            self.http.set_trading_stop(
                category=self.category, symbol=symbol,
                trailingStop="0",
                tpslMode="Full",
                positionIdx=0,
            )
        except Exception as e:
            log.warning("cancel_trailing_stop failed for %s: %s", symbol, e)

    def cancel_order(self, symbol: str, order_id: str):
        symbol = self._norm(symbol)
        try:
            self.http.cancel_order(category=self.category, symbol=symbol, orderId=order_id)
        except Exception as e:
            log.warning("Cancel failed for %s (%s): %s", order_id, symbol, e)

    @_retry()
    def cancel_all(self, symbol: str):
        symbol = self._norm(symbol)
        self.http.cancel_all_orders(category=self.category, symbol=symbol)

    def close_position_market(self, symbol: str, side: str, qty: float):
        symbol = self._norm(symbol)
        return self.place_market_order(symbol, side, qty, reduce_only=True)

    # ---------- websocket (fills / position updates) ----------

    # pybit's default is 10 reconnect attempts, after which it stops for good
    # while Telegram keeps working — the bot looks alive but never sees fills.
    WS_RETRIES = 0  # 0 = reconnect forever

    def start_private_ws(self, on_order, on_position):
        self._ws_callbacks = (on_order, on_position)
        self._ws = WebSocket(testnet=False, channel_type="private",
                              api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET,
                              retries=self.WS_RETRIES, restart_on_error=True)
        self._ws.order_stream(callback=on_order)
        self._ws.position_stream(callback=on_position)

    def ws_is_healthy(self) -> bool:
        """
        False when the socket is down and pybit is not reconnecting it.
        pybit only auto-reconnects on three specific error types; any other
        error (SSL, DNS, network unreachable, ...) makes it exit permanently.
        """
        ws = self._ws
        if ws is None:
            return False
        try:
            if ws.is_connected():
                return True
            return bool(getattr(ws, "attempting_connection", False)) and not getattr(ws, "exited", False)
        except Exception:
            return False

    def restart_private_ws(self):
        """Tear down a dead private WebSocket and open a fresh one (blocks until connected)."""
        on_order, on_position = self._ws_callbacks
        self.stop_ws()
        self.start_private_ws(on_order, on_position)

    def stop_ws(self):
        if self._ws:
            try:
                self._ws.exit()  # pybit's WebSocket has exit(), not close()
            except Exception as e:
                log.warning("WebSocket close error: %s", e)
