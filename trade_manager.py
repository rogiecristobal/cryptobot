"""
Core trade lifecycle:
  parse -> validate -> await confirmation -> execute entry/DCA
  -> on fill: sync SL + split TPs against actual position size
  -> on TP1 fill: move SL to breakeven
  -> on SL fill: cancel everything else for that symbol, close out state
"""
import logging
import time
from typing import Optional, List
from signal_parser import ParsedSignal
from bybit_client import BybitClient
from state_db import StateDB
import config

log = logging.getLogger("trade_manager")

SPECIAL_ASSETS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"}
 
 
class TradeManager:
    def __init__(self, bybit: BybitClient, db: StateDB, notify):
        self.bybit = bybit
        self.db = db
        self.notify = notify  # async-callable(str) -> sends a Telegram message
        self.pending = {}     # symbol -> dict(signal, expiry, chat_id, message_id)
        self.pending_mods = {}  # symbol -> dict(type, params, chat_id, message_id)
 
    # ---------- stage 1: validate + queue for confirmation ----------
 
    def stage_signal(self, signal: ParsedSignal) -> str:
        """Returns the confirmation prompt text, or raises ValueError with the reason it was rejected."""
        if signal.errors:
            raise ValueError("Signal rejected:\n- " + "\n- ".join(signal.errors))
 
        symbol = signal.asset
        if self.bybit.has_open_orders_or_position(symbol):
            raise ValueError(f"{symbol} already has an open position or pending order — new signal rejected.")

        # Validate SL/TP against current MarkPrice
        try:
            ticker = self.bybit.http.get_tickers(category=config.BYBIT_CATEGORY, symbol=symbol)
            mark = float(ticker["result"]["list"][0]["markPrice"])
        except Exception:
            mark = None
        if mark is not None:
            if signal.position == "LONG":
                if signal.sl >= mark:
                    raise ValueError(f"SL {signal.sl} must be below current MarkPrice {mark} for LONG.")
                for tp in signal.tps:
                    if tp <= mark:
                        raise ValueError(f"TP {tp} must be above current MarkPrice {mark} for LONG.")
            else:
                if signal.sl <= mark:
                    raise ValueError(f"SL {signal.sl} must be above current MarkPrice {mark} for SHORT.")
                for tp in signal.tps:
                    if tp >= mark:
                        raise ValueError(f"TP {tp} must be below current MarkPrice {mark} for SHORT.")
 
        expiry = time.time() + config.CONFIRM_TIMEOUT_SECONDS
        self.pending[symbol] = {"signal": signal, "expiry": expiry, "chat_id": None, "message_id": None}
 
        qty_entry, qty_dca, risk_amount, equity, risk_pct = self._calc_qty(signal)
        lines = [
            f"⚠️ Confirm trade — tap below within {config.CONFIRM_TIMEOUT_SECONDS}s",
            f"{symbol} ({signal.position})",
            f"Entry: {'MARKET' if signal.entry_is_market else signal.entry}  (qty ~{qty_entry})",
        ]
        if signal.dca:
            lines.append(f"DCA: {signal.dca}  (qty ~{qty_dca})")
        lines.append(f"SL: {signal.sl}")
        lines.append(f"Risk: ${risk_amount:.2f} ({risk_pct}% of ${equity:,.2f})")
        if signal.tps:
            lines.append(f"TPs: {', '.join(str(t) for t in signal.tps)}")
        lines.append(f"Leverage: {signal.leverage}x ({signal.leverage_mode or config.DEFAULT_MARGIN_MODE})")
        return "\n".join(lines)
 
    def _calc_qty(self, signal: ParsedSignal):
        equity = self.bybit.get_equity_usdt()

        is_special = signal.asset in SPECIAL_ASSETS
        if is_special:
            risk_pct = 5.0
        else:
            risk_pct = 3.0

        risk_amount = equity * (risk_pct / 100)

        entry_price = signal.entry
        if signal.entry_is_market:
            ticker = self.bybit.http.get_tickers(category=config.BYBIT_CATEGORY, symbol=signal.asset)
            entry_price = float(ticker["result"]["list"][0]["lastPrice"])

        if signal.dca:
            if is_special:
                # Each position sized independently — 1.5% risk each
                qty_entry = risk_amount / abs(entry_price - signal.sl)
                qty_dca = risk_amount / abs(signal.dca - signal.sl)
                qty_entry = self.bybit.round_qty(signal.asset, qty_entry)
                qty_dca = self.bybit.round_qty(signal.asset, qty_dca)
            else:
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

        return qty_entry, qty_dca, risk_amount, equity, risk_pct
 
    # ---------- stage 2: confirmed -> place entry/DCA + SL/TP ----------
  
    def confirm(self, symbol: str) -> str:
        entry = self.pending.pop(symbol, None)
        if not entry:
            return f"No pending confirmation for {symbol} (expired or never staged)."
        signal = entry["signal"]
        if time.time() > entry["expiry"]:
            return f"Confirmation window for {symbol} expired — resend the signal."

        qty_entry, qty_dca, *_ = self._calc_qty(signal)
        side = "Buy" if signal.position == "LONG" else "Sell"
        close_side = "Sell" if side == "Buy" else "Buy"

        max_lev = self.bybit.get_max_leverage(symbol)
        leverage = min(signal.leverage, max_lev)
        self.bybit.set_margin_mode(symbol, signal.leverage_mode or config.DEFAULT_MARGIN_MODE)
        self.bybit.set_leverage(symbol, leverage)

        tp = signal.tps[0] if signal.tps else None
        if signal.entry_is_market:
            entry_order = self.bybit.place_market_order(symbol, side, qty_entry,
                                                        stop_loss=signal.sl, take_profit=tp)
        else:
            entry_price = self.bybit.round_price(symbol, signal.entry)
            entry_order = self.bybit.place_limit_order(symbol, side, qty_entry, entry_price,
                                                       stop_loss=signal.sl, take_profit=tp)
        entry_order_id = entry_order["result"]["orderId"]

        dca_order_id = None
        dca_price = None
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
            entry_price=signal.entry if not signal.entry_is_market else 0,
            sl_price=signal.sl,
            original_sl_price=signal.sl,
            tp_prices=self.db.dumps(signal.tps),
            breakeven_moved=0,
            raw_signal=signal.asset,
            dca_price=dca_price,
        )

        entry_desc = "Market" if signal.entry_is_market else "Limit"
        if tp is not None:
            return f"{entry_desc} entry placed for {symbol} with native SL & TP."
        return f"{entry_desc} entry placed for {symbol} with native SL."

    def cancel(self, symbol: str) -> str:
        self.pending.pop(symbol, None)
        return f"Trade for {symbol} cancelled."

    # ---------- stage 3: fill-driven protective order management ----------
 
    def sync_protective_orders(self, symbol: str):
        """Update the native SL price on the position to match current state.
        Call this after any fill event or breakeven move."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        position = self.bybit.get_open_position(symbol)
        if not position:
            return  # nothing filled yet

        sl_price = self.bybit.round_price(symbol, state["sl_price"])
        self.bybit.set_position_sl(symbol, sl_price)
 
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
        filled_price = None
        if idx < len(tp_prices):
            filled_price = tp_prices.pop(idx)

        # Track achieved TPs so the user can see them in /status
        filled_tp_prices = self.db.loads(state.get("filled_tp_prices", "[]"))
        if filled_price is not None:
            filled_tp_prices.append(filled_price)
        self.db.upsert(symbol,
                       tp_prices=self.db.dumps(tp_prices),
                       filled_tp_prices=self.db.dumps(filled_tp_prices))
 
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
 
    # ---------- manual TP detection (user places TPs on Bybit UI) ----------

    def handle_manual_tp_fill(self, symbol: str) -> int:
        """Called when a reduce-only fill is detected that doesn't match our tracked orders.
        Returns the new manual_tp_count (0 if no active state)."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return 0

        count = (state.get("manual_tp_count") or 0) + 1
        self.db.upsert(symbol, manual_tp_count=count)

        if count == 1:
            self.notify(f"🎯 TP1 hit on {symbol}!")
        elif count == 2:
            self.notify(f"🎯 TP2 hit on {symbol}!")
        elif count >= 3:
            self.notify(f"🎯 TP3 hit on {symbol}!")

        return count

    def apply_breakeven(self, symbol: str):
        """Move SL to entry / original SL price and resync the SL order on Bybit."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        if state.get("breakeven_moved"):
            return

        new_sl = state["entry_price"] or state["original_sl_price"]
        self.db.upsert(symbol, sl_price=new_sl, breakeven_moved=1, breakeven_prompt_msg_id=None)
        self.sync_protective_orders(symbol)
        self.notify(f"✅ SL moved to entry ({new_sl}) for {symbol}.")

    def clear_breakeven_prompt(self, symbol: str):
        """User declined — clear the pending prompt flag so the timeout is a no-op."""
        self.db.upsert(symbol, breakeven_prompt_msg_id=None)

    def handle_sl_fill(self, symbol: str, source: str = "SL"):
        """Called when a native SL or native TP triggers. source is 'SL', 'TP', or 'Position'."""
        state = self.db.get(symbol)
        if not state:
            return
        self.bybit.cancel_all(symbol)

        # Verify position is closed, force-close any residual
        position = self.bybit.get_open_position(symbol)
        if position and float(position.get("size", 0)) > 0:
            side = position["side"]
            close_side = "Sell" if side == "Buy" else "Buy"
            qty = float(position["size"])
            self.bybit.close_position_market(symbol, close_side, qty)
            self.notify(f"🛑 {source} triggered on {symbol} — residual detected, force-closed {qty}.")

        self.db.delete(symbol)
        self.notify(f"🛑 {source} triggered on {symbol} — position closed.")
 
    def handle_entry_or_dca_fill(self, symbol: str):
        self.sync_protective_orders(symbol)

    # ---------- modification commands (sl, tp, dca, entry) ----------

    def _dca_qty_from_state(self, state: dict) -> float:
        """Recalculate DCA qty for an active position using same risk as entry."""
        equity = self.bybit.get_equity_usdt()
        is_special = state["symbol"] in SPECIAL_ASSETS
        risk_pct = 5.0 if is_special else 3.0
        risk_amount = equity * (risk_pct / 100)
        dca_price = state.get("dca_price") or 0
        if dca_price <= 0:
            return 0.0
        entry_price = state["entry_price"]
        if entry_price == 0:
            entry_price = state["sl_price"]
        qty = risk_amount / max(abs(entry_price - state["sl_price"]), 1)
        return self.bybit.round_qty(state["symbol"], qty)

    def stage_modify_sl(self, symbol: str, new_sl: float) -> str:
        """Stage an SL modification. Returns a preview prompt."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            # Check if it's a staged trade
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                return f"Modify SL for {symbol}?\n  Current: {signal.sl}\n  New: {new_sl}"
            raise ValueError(f"No active position or pending trade for {symbol}.")

        old_sl = state["sl_price"]
        return f"Modify SL for {symbol}?\n  Current: {old_sl}\n  New: {new_sl}"

    def stage_modify_tp(self, symbol: str, new_prices: List[float]) -> str:
        """Stage a TP modification. Returns a preview prompt."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                old = ", ".join(str(t) for t in signal.tps)
                new = ", ".join(str(t) for t in new_prices)
                return f"Modify TPs for {symbol}?\n  Current: {old}\n  New: {new}"
            raise ValueError(f"No active position or pending trade for {symbol}.")

        old = ", ".join(str(t) for t in self.db.loads(state["tp_prices"]))
        new = ", ".join(str(t) for t in new_prices)
        return f"Modify TPs for {symbol}?\n  Current: {old}\n  New: {new}"

    def stage_modify_dca(self, symbol: str, dca_price: Optional[float]) -> str:
        """Stage a DCA modification. None = remove DCA."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                if dca_price is None:
                    return f"Remove DCA for {symbol}?"
                return f"Add DCA for {symbol}?\n  Price: {dca_price}"
            raise ValueError(f"No active position or pending trade for {symbol}.")

        if dca_price is None:
            return f"Remove DCA for {symbol}?"
        return f"Modify DCA for {symbol}?\n  Price: {dca_price}"

    def stage_modify_entry(self, symbol: str, new_price: Optional[float], is_market: bool) -> str:
        """Stage an entry modification (staged trades only)."""
        if symbol in self.pending:
            entry_desc = "MARKET" if is_market else new_price
            return f"Modify Entry for {symbol}?\n  New: {entry_desc}"
        raise ValueError(f"No pending trade for {symbol} — entry can only be modified before confirmation.")

    def apply_modification(self, symbol: str) -> str:
        """Apply a previously staged modification (active positions only). Returns a result message."""
        mod = self.pending_mods.pop(symbol, None)
        if not mod:
            return f"No pending modification for {symbol}."

        mod_type = mod["type"]
        params = mod["params"]
        state = self.db.get(symbol)
        if not state:
            return f"No active position for {symbol}."

        if mod_type == "sl":
            self.db.upsert(symbol, sl_price=params["new_price"])
            self.sync_protective_orders(symbol)
            return f"✅ SL updated for {symbol} to {params['new_price']}."

        elif mod_type == "tp":
            self.db.upsert(symbol, tp_prices=self.db.dumps(params["new_prices"]))
            self.sync_protective_orders(symbol)
            return f"✅ TPs updated for {symbol}: {', '.join(str(t) for t in params['new_prices'])}."

        elif mod_type == "dca":
            # Cancel existing DCA if any
            if state.get("dca_order_id"):
                self.bybit.cancel_order(symbol, state["dca_order_id"])
            self.db.upsert(symbol, dca_order_id=None, dca_price=None)
            if params["new_price"] is not None:
                pos_side = state["position"]
                side = "Buy" if pos_side == "LONG" else "Sell"
                dca_qty = self._dca_qty_from_state({**state, "dca_price": params["new_price"]})
                if dca_qty > 0:
                    dca_price = self.bybit.round_price(symbol, params["new_price"])
                    dca_order = self.bybit.place_limit_order(symbol, side, dca_qty, dca_price)
                    self.db.upsert(symbol, dca_order_id=dca_order["result"]["orderId"], dca_price=dca_price)
                    return f"✅ DCA placed for {symbol} at {dca_price} (qty ~{dca_qty})."
            return f"✅ DCA removed for {symbol}."

        return f"Unknown modification type: {mod_type}"

    def cancel_modification(self, symbol: str) -> str:
        self.pending_mods.pop(symbol, None)
        return f"Modification for {symbol} cancelled."