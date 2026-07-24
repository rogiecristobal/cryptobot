"""
Thin wrapper around pybit's unified trading HTTP + private WebSocket.
All Bybit-specific calls live here so trade_manager.py stays exchange-agnostic-ish.
"""
import functools
import time
import logging
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

    @_retry()
    def place_market_order(self, symbol: str, side: str, qty: float, reduce_only=False,
                           stop_loss: float | None = None,
                           take_profit: float | None = None):
        symbol = self._norm(symbol)
        body = dict(
            category=self.category, symbol=symbol, side=side,
            orderType="Market", qty=self._fmt_qty(symbol, qty), reduceOnly=reduce_only,
        )
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
        return self.http.place_order(**body)

    @_retry()
    def place_limit_order(self, symbol: str, side: str, qty: float, price: float, reduce_only=False,
                          stop_loss: float | None = None,
                          take_profit: float | None = None):
        symbol = self._norm(symbol)
        body = dict(
            category=self.category, symbol=symbol, side=side,
            orderType="Limit", qty=self._fmt_qty(symbol, qty), price=self._fmt_price(symbol, price),
            timeInForce="GTC", reduceOnly=reduce_only,
        )
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
        return self.http.place_order(**body)

    @_retry()
    def set_position_sl(self, symbol: str, sl_price: float, trigger_by: str = "MarkPrice",
                        position_idx: int = 0):
        symbol = self._norm(symbol)
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

    def start_private_ws(self, on_order, on_position):
        self._ws = WebSocket(testnet=False, channel_type="private",
                              api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        self._ws.order_stream(callback=on_order)
        self._ws.position_stream(callback=on_position)

    def stop_ws(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                log.warning("WebSocket close error: %s", e)
