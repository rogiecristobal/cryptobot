"""
The worker's private Bitunix calls (live mode only).

It uses its OWN BitunixClient instance (own HTTP session, own server-time
offset), so nothing it does changes the existing bot's client. Signing,
endpoints and field handling are the existing client's. Everything the worker
creates carries a client order ID starting with se_, and it only ever
closes/cancels positions and TP/SL orders on symbols where it found no
foreign position or order before entering.

Endpoints used (all checked against openapidoc.bitunix.com):
  GET  /api/v1/futures/account                         available balance (existing client)
  GET  /api/v1/futures/position/get_pending_positions  (existing client)
  GET  /api/v1/futures/trade/get_pending_orders        (existing client)
  GET  /api/v1/futures/trade/get_order_detail          lookup by clientId (existing client)
  GET  /api/v1/futures/tpsl/get_pending_orders         (existing client)
  POST /api/v1/futures/trade/place_order               market BUY with slPrice/tpPrice attached
  POST /api/v1/futures/tpsl/position/place_order       Position TP/SL (existing client)
  POST /api/v1/futures/tpsl/cancel_order               (existing client)
  POST /api/v1/futures/trade/flash_close_position      (existing client)
  POST /api/v1/futures/account/change_margin_mode|change_leverage (existing client, same as /place)
  GET  /api/v1/futures/trade/get_history_trades        fill price/qty/fee of the entry order
  GET  /api/v1/futures/position/get_history_positions  closed position: close price, fee, funding, PnL
"""
import logging
import threading
import time
from decimal import Decimal
from typing import Optional

from bitunix_client import (BitunixClient, BitunixError, DUPLICATE_CLIENT_ID, _as_list, _as_obj,
                            _is_transient)

log = logging.getLogger("se.exchange")

MIN_REQUEST_GAP = 0.15  # seconds between this worker's requests (well under Bitunix's 10/s)


def _d(v) -> Decimal:
    try:
        return Decimal(str(v)) if v not in (None, "") else Decimal(0)
    except Exception:
        return Decimal(0)


class SEExchange:
    def __init__(self, api_key: str, api_secret: str):
        if not api_key or not api_secret:
            raise RuntimeError("BITUNIX_API_KEY / BITUNIX_API_SECRET are missing in .env")
        self.client = BitunixClient(api_key, api_secret)
        self._throttle_lock = threading.Lock()
        self._last_request = 0.0

    def _throttle(self):
        """Simple client-side rate limit; the worker only needs a few requests a minute."""
        with self._throttle_lock:
            wait = self._last_request + MIN_REQUEST_GAP - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    # ---------- clock ----------

    def sync_time(self) -> Optional[float]:
        """Measure local-vs-server time and apply the offset to signed requests. Returns drift (s)."""
        self._throttle()
        drift = self.client.clock_drift_seconds()  # local - server
        if drift is not None:
            self.client.time_offset_ms = int(-drift * 1000)
        return drift

    # ---------- account / positions / orders ----------

    def available_usdt(self) -> Decimal:
        self._throttle()
        return _d(self.client.get_wallet_info()["available"])

    def raw_positions(self) -> list:
        """Every open position on the account (one request)."""
        self._throttle()
        return self.client._raw_positions()

    def position(self, symbol: str) -> Optional[dict]:
        """Normalized position (see bitunix_client) incl. stop_loss / take_profit coverage."""
        self._throttle()
        return self.client.get_open_position(symbol)

    def open_orders(self, symbol: str) -> list:
        self._throttle()
        return self.client.get_open_orders(symbol)

    def find_order(self, symbol: str, client_id: str) -> Optional[dict]:
        self._throttle()
        return self.client.find_order(symbol, client_id)

    def max_leverage(self, symbol: str) -> int:
        self._throttle()
        return self.client.get_max_leverage(symbol)

    def foreign_activity(self, symbol: str) -> list:
        """
        Anything on the symbol the worker didn't create: an open position (the
        worker only calls this when it has none of its own there) or an open
        order whose client ID doesn't start with se_. Returns descriptions.
        """
        found = []
        self._throttle()
        for p in self.client._raw_positions(symbol):
            if p.get("symbol") == symbol:
                found.append(f"open {p.get('side')} position of {p.get('qty')}")
        for o in self.open_orders(symbol):
            if not str(o.get("client_id") or "").startswith("se_"):
                found.append(f"open {o['side']} {o['order_type']} order {o['order_id']}")
        return found

    # ---------- entry ----------

    def prepare_symbol(self, symbol: str, leverage: int, margin_mode: str):
        """Margin mode and leverage, exactly as /place sets them before its entry."""
        self._throttle()
        self.client.set_margin_mode(symbol, margin_mode)
        self._throttle()
        self.client.set_leverage(symbol, leverage)

    def place_entry(self, symbol: str, qty: Decimal, sl: Decimal, tp: Decimal, client_id: str,
                    attempts: int = 3) -> str:
        """
        Market BUY with the SL and TP attached in the same request (like /place,
        MARK_PRICE triggers), under a fixed client order ID. If a request times
        out, the order is looked up by that ID before retrying, so a retry can
        never open a second position. Returns the Bitunix order ID.
        """
        body = dict(symbol=symbol, side="BUY", tradeSide="OPEN", orderType="MARKET",
                    qty=format(qty, "f"), clientId=client_id)
        self.client._add_tpsl(body, symbol, float(sl), float(tp))
        delay = 1.0
        for attempt in range(attempts):
            try:
                self._throttle()
                data = _as_obj(self.client._post("/api/v1/futures/trade/place_order", body))
                return str(data["orderId"])
            except Exception as e:
                if isinstance(e, BitunixError) and e.code == DUPLICATE_CLIENT_ID:
                    existing = self.find_order(symbol, client_id)
                    if existing:
                        log.warning("Order %s already on Bitunix — not placing again", client_id)
                        return existing["order_id"]
                    raise
                if not _is_transient(e) or attempt == attempts - 1:
                    raise
                existing = self.find_order(symbol, client_id)
                if existing:
                    log.warning("Order %s reached Bitunix despite %s — not placing again", client_id, e)
                    return existing["order_id"]
                log.warning("place_order %s failed (%s), retry %d/%d in %.0fs",
                            client_id, e, attempt + 1, attempts, delay)
                time.sleep(delay)
                delay *= 2

    def fill_details(self, symbol: str, order_id: str) -> Optional[dict]:
        """{'price', 'qty', 'fee'} of an order's fills (fee as a positive cost), or None if none yet."""
        self._throttle()
        data = self.client._get("/api/v1/futures/trade/get_history_trades",
                                symbol=symbol, orderId=order_id, limit=100)
        fills = [t for t in _as_list(data, "tradeList") if str(t.get("orderId")) == str(order_id)]
        qty = sum((_d(t.get("qty")) for t in fills), Decimal(0))
        if qty <= 0:
            return None
        value = sum((_d(t.get("price")) * _d(t.get("qty")) for t in fills), Decimal(0))
        fee = sum((abs(_d(t.get("fee"))) for t in fills), Decimal(0))
        return {"price": value / qty, "qty": qty, "fee": fee}

    # ---------- protection / close ----------

    def ensure_tpsl(self, symbol: str, sl: Decimal, tp: Decimal):
        """Put the SL and TP on the whole position (Position TP/SL if none covers it). Raises on failure."""
        self._throttle()
        self.client.set_position_sl(symbol, float(sl))
        self._throttle()
        self.client.set_position_tp(symbol, float(tp))

    def close_position(self, symbol: str) -> Decimal:
        """Market-close the symbol's position (flash close). Returns the qty closed (0 if none)."""
        self._throttle()
        return _d(self.client.close_position_market(symbol))

    def cancel_position_tpsl(self, symbol: str, position_id: str) -> int:
        """Cancel TP/SL orders still pending for this (worker-owned) position. Returns how many."""
        if not position_id:
            return 0
        self._throttle()
        orders = self.client.get_tpsl_orders(symbol, position_id)
        for o in orders:
            self._throttle()
            self.client.cancel_tpsl(symbol, o.get("id") or o.get("orderId"))
        return len(orders)

    # ---------- history ----------

    def history_position(self, symbol: str, position_id: str) -> Optional[dict]:
        """
        A closed position from Get history positions:
        {'close_price', 'entry_price', 'fee', 'funding', 'realized_pnl', 'qty', 'close_time'}.
        Per the docs, realizedPNL excludes fees and funding. None if not listed yet.
        """
        if not position_id:
            return None
        self._throttle()
        data = self.client._get("/api/v1/futures/position/get_history_positions",
                                symbol=symbol, positionId=position_id, limit=10)
        rows = [p for p in _as_list(data, "positionList") if str(p.get("positionId")) == str(position_id)]
        if not rows:
            return None
        p = rows[0]
        return {
            "close_price": _d(p.get("closePrice")),
            "entry_price": _d(p.get("entryPrice")),
            "fee": _d(p.get("fee")),
            "funding": _d(p.get("funding")),
            "realized_pnl": _d(p.get("realizedPNL")),
            "qty": _d(p.get("maxQty")),
            "close_time": int(_d(p.get("mtime"))) or int(time.time() * 1000),
            "raw": {k: p.get(k) for k in ("closePrice", "fee", "funding", "realizedPNL", "maxQty", "mtime")},
        }
