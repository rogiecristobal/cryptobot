"""
Core trade lifecycle:
  parse -> validate -> await confirmation -> execute entry/DCA
  -> on fill: sync SL + split TPs against actual position size
  -> on TP1 fill: move SL to breakeven
  -> on SL fill: cancel everything else for that symbol, close out state
"""
import logging
import time
from signal_parser import ParsedSignal
from bybit_client import BybitClient
from state_db import StateDB
import config
 
log = logging.getLogger("trade_manager")
 
 
class TradeManager:
    def __init__(self, bybit: BybitClient, db: StateDB, notify):
        self.bybit = bybit
        self.db = db
        self.notify = notify  # async-callable(str) -> sends a Telegram message
        self.pending = {}     # symbol -> (ParsedSignal, expiry_timestamp)
 
    # ---------- stage 1: validate + queue for confirmation ----------
 
    def stage_signal(self, signal: ParsedSignal) -> str:
        """Returns the confirmation prompt text, or raises ValueError with the reason it was rejected."""
        if signal.errors:
            raise ValueError("Signal rejected:\n- " + "\n- ".join(signal.errors))
 
        symbol = signal.asset
        if self.bybit.has_open_orders_or_position(symbol):
            raise ValueError(f"{symbol} already has an open position or pending order — new signal rejected.")
 
        expiry = time.time() + config.CONFIRM_TIMEOUT_SECONDS
        self.pending[symbol] = (signal, expiry)
 
        qty_entry, qty_dca = self._calc_qty(signal)
        lines = [
            f"⚠️ Confirm trade — reply ✅ within {config.CONFIRM_TIMEOUT_SECONDS}s",
            f"{symbol} ({signal.position})",
            f"Entry: {'MARKET' if signal.entry_is_market else signal.entry}  (qty ~{qty_entry})",
        ]
        if signal.dca:
            lines.append(f"DCA: {signal.dca}  (qty ~{qty_dca})")
        lines.append(f"SL: {signal.sl}")
        lines.append(f"TPs: {', '.join(str(t) for t in signal.tps)}")
        lines.append(f"Leverage: {signal.leverage}x ({signal.leverage_mode or config.DEFAULT_MARGIN_MODE})")
        return "\n".join(lines)
 
    def _calc_qty(self, signal: ParsedSignal):
        equity = self.bybit.get_equity_usdt()
        risk_amount = equity * (config.RISK_PERCENT / 100)
 
        entry_price = signal.entry
        if signal.entry_is_market:
            ticker = self.bybit.http.get_tickers(category=config.BYBIT_CATEGORY, symbol=signal.asset)
            entry_price = float(ticker["result"]["list"][0]["lastPrice"])
 
        if signal.dca:
            w_e = config.DCA_SPLIT_RATIO
            w_d = 1 - w_e
            avg_entry = entry_price * w_e + signal.dca * w_d
            total_qty = risk_amount / abs(avg_entry - signal.sl)
            qty_entry = self.bybit.round_qty(signal.asset, total_qty * w_e)
            qty_dca = self.bybit.round_qty(signal.asset, total_qty * w_d)
        else:
            total_qty = risk_amount / abs(entry_price - signal.sl)
            qty_entry = self.bybit.round_qty(signal.asset, total_qty)
            qty_dca = 0.0
 
        return qty_entry, qty_dca
 
    # ---------- stage 2: confirmed -> place entry/DCA ----------
 
    def confirm(self, symbol: str) -> str:
        entry = self.pending.pop(symbol, None)
        if not entry:
            return f"No pending confirmation for {symbol} (expired or never staged)."
        signal, expiry = entry
        if time.time() > expiry:
            return f"Confirmation window for {symbol} expired — resend the signal."
 
        qty_entry, qty_dca = self._calc_qty(signal)
        side = "Buy" if signal.position == "LONG" else "Sell"
 
        self.bybit.set_margin_mode(symbol, signal.leverage_mode or config.DEFAULT_MARGIN_MODE)
        self.bybit.set_leverage(symbol, signal.leverage)
 
        if signal.entry_is_market:
            entry_order = self.bybit.place_market_order(symbol, side, qty_entry)
        else:
            entry_price = self.bybit.round_price(symbol, signal.entry)
            entry_order = self.bybit.place_limit_order(symbol, side, qty_entry, entry_price)
        entry_order_id = entry_order["result"]["orderId"]
 
        dca_order_id = None
        if signal.dca and qty_dca > 0:
            dca_price = self.bybit.round_price(symbol, signal.dca)
            dca_order = self.bybit.place_limit_order(symbol, side, qty_dca, dca_price)
            dca_order_id = dca_order["result"]["orderId"]
 
        self.db.upsert(
            symbol,
            position=signal.position,
            status="active",
            entry_order_id=entry_order_id,
            dca_order_id=dca_order_id,
            sl_order_id=None,
            tp_order_ids=self.db.dumps([]),
            entry_price=signal.entry if not signal.entry_is_market else 0,
            sl_price=signal.sl,
            original_sl_price=signal.sl,
            tp_prices=self.db.dumps(signal.tps),
            breakeven_moved=0,
            raw_signal=signal.asset,
        )
        return f"Placed entry{' + DCA' if dca_order_id else ''} for {symbol}. Waiting for fill to arm SL/TPs."
 
    # ---------- stage 3: fill-driven protective order management ----------
 
    def sync_protective_orders(self, symbol: str):
        """Cancel + re-place SL and TP orders sized to the CURRENT actual position. Call this
        after any fill event (entry, DCA) that could change position size."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        position = self.bybit.get_open_position(symbol)
        if not position:
            return  # nothing filled yet
 
        total_qty = float(position["size"])
        pos_side = position["side"]  # "Buy" or "Sell"
        close_side = "Sell" if pos_side == "Buy" else "Buy"
 
        if state["sl_order_id"]:
            self.bybit.cancel_order(symbol, state["sl_order_id"])
        for oid in self.db.loads(state["tp_order_ids"]):
            self.bybit.cancel_order(symbol, oid)
 
        sl_price = self.bybit.round_price(symbol, state["sl_price"])
        sl_order = self.bybit.place_stop_loss(symbol, close_side, total_qty, sl_price)
        sl_order_id = sl_order["result"]["orderId"]
 
        tp_prices = self.db.loads(state["tp_prices"])
        n = len(tp_prices)
        tp_order_ids = []
        if n:
            base_qty = self.bybit.round_qty(symbol, total_qty / n)
            allocated = 0.0
            for i, tp_price in enumerate(tp_prices):
                qty = base_qty if i < n - 1 else self.bybit.round_qty(symbol, total_qty - allocated)
                allocated += qty
                order = self.bybit.place_limit_order(symbol, close_side, qty,
                                                       self.bybit.round_price(symbol, tp_price),
                                                       reduce_only=True)
                tp_order_ids.append(order["result"]["orderId"])
 
        self.db.upsert(symbol, sl_order_id=sl_order_id, tp_order_ids=self.db.dumps(tp_order_ids))
 
    def handle_tp_fill(self, symbol: str, filled_order_id: str):
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        tp_ids = self.db.loads(state["tp_order_ids"])
        if filled_order_id not in tp_ids:
            return
 
        # Remove the filled TP from the remaining set so sync_protective_orders
        # doesn't re-place an order at a price that's already been hit.
        idx = tp_ids.index(filled_order_id)
        tp_prices = self.db.loads(state["tp_prices"])
        if idx < len(tp_prices):
            tp_prices.pop(idx)
        self.db.upsert(symbol, tp_prices=self.db.dumps(tp_prices))
 
        first_tp = not state["breakeven_moved"]
        if first_tp:
            new_sl = state["entry_price"] or state["original_sl_price"]
            self.db.upsert(symbol, sl_price=new_sl, breakeven_moved=1)
 
        # Always resync — every TP fill shrinks the position, so the SL order's
        # qty must be re-derived from the current actual position every time,
        # not just on the first hit. closeOnTrigger is a backstop, not a substitute.
        self.sync_protective_orders(symbol)
 
        if first_tp:
            self.notify(f"🎯 TP hit on {symbol} — SL moved to breakeven ({new_sl}).")
        else:
            self.notify(f"🎯 Another TP hit on {symbol} — SL resynced to remaining size.")
 
    def handle_sl_fill(self, symbol: str):
        state = self.db.get(symbol)
        if not state:
            return
        self.bybit.cancel_all(symbol)
 
        # Safety net: don't just trust closeOnTrigger closed everything —
        # verify, and force-close any residual with a market order if not.
        position = self.bybit.get_open_position(symbol)
        if position and float(position.get("size", 0)) > 0:
            side = position["side"]
            close_side = "Sell" if side == "Buy" else "Buy"
            qty = float(position["size"])
            self.bybit.close_position_market(symbol, close_side, qty)
            self.notify(f"🛑 SL hit on {symbol} — residual position detected, force-closed {qty}.")
 
        self.db.delete(symbol)
        self.notify(f"🛑 SL hit on {symbol} — all related orders cancelled, position closed.")
 
    def handle_entry_or_dca_fill(self, symbol: str):
        self.sync_protective_orders(symbol)