"""
Thin wrapper around pybit's unified trading HTTP + private WebSocket.
All Bybit-specific calls live here so trade_manager.py stays exchange-agnostic-ish.
"""
import logging
from pybit.unified_trading import HTTP, WebSocket
import config

log = logging.getLogger("bybit_client")


class BybitClient:
    def __init__(self):
        self.http = HTTP(
            api_key=config.BYBIT_API_KEY,
            api_secret=config.BYBIT_API_SECRET,
            testnet=False,
        )
        self.category = config.BYBIT_CATEGORY
        self._instrument_cache = {}

    def _norm(self, symbol: str) -> str:
        return symbol.replace("/", "").replace(" ", "")

    # ---------- account / instrument info ----------

    def get_equity_usdt(self) -> float:
        resp = self.http.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        try:
            return float(resp["result"]["list"][0]["coin"][0]["walletBalance"])
        except (KeyError, IndexError, TypeError):
            raise RuntimeError(f"Could not parse equity from wallet response: {resp}")

    def get_instrument_info(self, symbol: str) -> dict:
        symbol = self._norm(symbol)
        if symbol in self._instrument_cache: 
            return self._instrument_cache[symbol]
        resp = self.http.get_instruments_info(category=self.category, symbol=symbol)
        info = resp["result"]["list"][0]
        self._instrument_cache[symbol] = info
        return info

    def round_qty(self, symbol: str, qty: float) -> float:
        symbol = self._norm(symbol)
        info = self.get_instrument_info(symbol)
        step = float(info["lotSizeFilter"]["qtyStep"])
        min_qty = float(info["lotSizeFilter"]["minOrderQty"])
        rounded = round(qty / step) * step
        return max(rounded, min_qty)

    def round_price(self, symbol: str, price: float) -> float:
        symbol = self._norm(symbol)
        info = self.get_instrument_info(symbol)
        tick = float(info["priceFilter"]["tickSize"])
        decimals = max(0, str(tick)[::-1].find("."))
        rounded = round(price / tick) * tick
        return round(rounded, decimals)

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

    def set_margin_mode(self, symbol: str, mode: str):
        symbol = self._norm(symbol)
        trade_mode = 1 if mode.upper() == "ISOLATED" else 0
        try:
            self.http.switch_margin_mode(
                category=self.category,
                symbol=symbol,
                tradeMode=trade_mode,
                buyLeverage=str(config.DEFAULT_LEVERAGE),
                sellLeverage=str(config.DEFAULT_LEVERAGE),
            )
        except Exception as e:
            msg = str(e).lower()
            if "unified account is forbidden" in msg:
                log.warning("UTA account detected — margin mode is set at account level, skipping switch.")
                return
            if "not modified" in msg:
                return
            raise

    def get_open_position(self, symbol: str) -> dict | None:
        symbol = self._norm(symbol)
        resp = self.http.get_positions(category=self.category, symbol=symbol)
        for pos in resp["result"]["list"]:
            if float(pos.get("size", 0)) > 0:
                return pos
        return None

    def has_open_orders_or_position(self, symbol: str) -> bool:
        symbol = self._norm(symbol)
        if self.get_open_position(symbol):
            return True
        resp = self.http.get_open_orders(category=self.category, symbol=symbol)
        return len(resp["result"]["list"]) > 0

    # ---------- orders ----------

    def place_market_order(self, symbol: str, side: str, qty: float, reduce_only=False):
        symbol = self._norm(symbol)
        return self.http.place_order(
            category=self.category, symbol=symbol, side=side,
            orderType="Market", qty=str(qty), reduceOnly=reduce_only,
        )

    def place_limit_order(self, symbol: str, side: str, qty: float, price: float, reduce_only=False):
        symbol = self._norm(symbol)
        return self.http.place_order(
            category=self.category, symbol=symbol, side=side,
            orderType="Limit", qty=str(qty), price=str(price),
            timeInForce="GTC", reduceOnly=reduce_only,
        )

    def place_stop_loss(self, symbol: str, side: str, qty: float, trigger_price: float):
        symbol = self._norm(symbol)
        # side here is the CLOSING side (opposite of position side)
        # triggerBy=MarkPrice avoids the SL firing on thin-orderbook last-price wicks
        return self.http.place_order(
            category=self.category, symbol=symbol, side=side,
            orderType="Market", qty=str(qty),
            triggerPrice=str(trigger_price), triggerDirection=1 if side == "Sell" else 2,
            triggerBy="MarkPrice",
            reduceOnly=True, closeOnTrigger=True,
        )

    def cancel_order(self, symbol: str, order_id: str):
        symbol = self._norm(symbol)
        try:
            self.http.cancel_order(category=self.category, symbol=symbol, orderId=order_id)
        except Exception as e:
            log.warning(f"Cancel failed for {order_id} ({symbol}): {e}")

    def cancel_all(self, symbol: str):
        symbol = self._norm(symbol)
        self.http.cancel_all_orders(category=self.category, symbol=symbol)

    def close_position_market(self, symbol: str, side: str, qty: float):
        symbol = self._norm(symbol)
        return self.place_market_order(symbol, side, qty, reduce_only=True)

    # ---------- websocket (fills / position updates) ----------

    def start_private_ws(self, on_order, on_position):
        ws = WebSocket(testnet=False, channel_type="private",
                        api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        ws.order_stream(callback=on_order)
        ws.position_stream(callback=on_position)
        return ws
