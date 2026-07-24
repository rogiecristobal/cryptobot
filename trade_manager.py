"""
Core trade lifecycle:
  parse -> validate -> await confirmation -> execute entry/DCA
  -> on fill: sync SL + split TPs against actual position size
  -> on TP1 fill: move SL to breakeven
  -> on SL fill: cancel everything else for that symbol, close out state
"""
import logging
import threading
import time
from typing import Optional, List
from signal_parser import ParsedSignal
from bybit_client import BybitClient
from state_db import StateDB
import config

log = logging.getLogger("trade_manager")


class TradeManager:
    def __init__(self, bybit: BybitClient, db: StateDB, notify):
        self.bybit = bybit
        self.db = db
        self.notify = notify
        self._lock = threading.Lock()
        self.pending = {}
        self.pending_mods = {}

    # ---------- thread-safe pending access ----------

    def get_pending(self, symbol: str):
        with self._lock:
            return self.pending.get(symbol)

    def set_pending_metadata(self, symbol: str, chat_id: int, message_id: int):
        with self._lock:
            entry = self.pending.get(symbol)
            if entry:
                entry["chat_id"] = chat_id
                entry["message_id"] = message_id

    def set_pending_mod_metadata(self, symbol: str, chat_id: int, message_id: int):
        with self._lock:
            entry = self.pending_mods.get(symbol)
            if entry:
                entry["chat_id"] = chat_id
                entry["message_id"] = message_id

    # ---------- startup reconciliation ----------

    def reconcile(self) -> list[str]:
        messages = []

        for symbol in self.db.all_active():
            pos = self.bybit.get_open_position(symbol)
            if pos and float(pos.get("size", 0)) > 0:
                actual_size = float(pos["size"])
                self.db.upsert(symbol, breakeven_prompt_msg_id=None)
                state = self.db.get(symbol)
                if state and state.get("sl_price"):
                    try:
                        self.sync_protective_orders(symbol)
                    except Exception as e:
                        log.warning("sync_protective_orders failed during reconcile for %s: %s", symbol, e)
                messages.append(f"✅ {symbol}: reconciled (size {actual_size}), SL resynced.")
            else:
                try:
                    self.bybit.cancel_all(symbol)
                except Exception as e:
                    log.warning("cancel_all failed for %s during reconcile: %s", symbol, e)
                self.db.delete(symbol)
                messages.append(f"🛑 {symbol}: position closed while offline — cleaned up.")

        bybit_positions = self.bybit.get_all_open_positions()
        db_active = self.db.all_active()
        db_by_norm = {}
        for s in db_active:
            norm = s.replace("/", "").replace(" ", "")
            db_by_norm[norm] = s

        for pos in bybit_positions:
            b_sym = pos["symbol"]
            if b_sym not in db_by_norm:
                side = "LONG" if pos.get("side") == "Buy" else "SHORT"
                size = float(pos.get("size", 0))
                entry = float(pos.get("avgPrice", 0))
                self.db.upsert(
                    b_sym,
                    position=side,
                    status="active",
                    entry_price=entry,
                    sl_price=0,
                    original_sl_price=0,
                    tp_prices=self.db.dumps([]),
                    breakeven_moved=0,
                    manual_tp_count=0,
                    breakeven_prompt_msg_id=None,
                    entry_order_id="",
                    dca_order_id=None,
                    dca_price=None,
                )
                messages.append(f"⚠️ {b_sym}: orphan position on Bybit — recovered ({side}, {size})")

        return messages

    # ---------- stage 1: validate + queue for confirmation ----------

    def stage_signal(self, signal: ParsedSignal) -> str:
        if signal.errors:
            raise ValueError("Signal rejected:\n- " + "\n- ".join(signal.errors))

        symbol = signal.asset
        if self.bybit.has_open_orders_or_position(symbol):
            raise ValueError(f"{symbol} already has an open position or pending order — new signal rejected.")

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
        qty_entry, qty_dca, risk_amount, equity, risk_pct = self._calc_qty(signal)

        with self._lock:
            self.pending[symbol] = {
                "signal": signal, "expiry": expiry,
                "chat_id": None, "message_id": None,
                "cached_qty": (qty_entry, qty_dca, risk_amount, equity, risk_pct),
            }

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

        risk_pct = signal.margin_percent if signal.margin_percent is not None else config.RISK_PERCENT
        risk_amount = equity * (risk_pct / 100)

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

        return qty_entry, qty_dca, risk_amount, equity, risk_pct

    # ---------- stage 2: confirmed -> place entry/DCA + SL/TP ----------

    def confirm(self, symbol: str) -> str:
        with self._lock:
            entry = self.pending.pop(symbol, None)
        if not entry:
            return f"No pending confirmation for {symbol} (expired or never staged)."
        signal = entry["signal"]
        if time.time() > entry["expiry"]:
            return f"Confirmation window for {symbol} expired — resend the signal."

        # Re-validate SL against current mark price
        try:
            ticker = self.bybit.http.get_tickers(category=config.BYBIT_CATEGORY, symbol=symbol)
            mark = float(ticker["result"]["list"][0]["markPrice"])
        except Exception:
            mark = None
        if mark is not None:
            if signal.position == "LONG" and signal.sl >= mark:
                return f"❌ Trade aborted: SL {signal.sl} is now above mark price {mark}."
            if signal.position == "SHORT" and signal.sl <= mark:
                return f"❌ Trade aborted: SL {signal.sl} is now below mark price {mark}."

        qty_entry, qty_dca, *_ = entry.get("cached_qty") or self._calc_qty(signal)
        side = "Buy" if signal.position == "LONG" else "Sell"

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
                                                       stop_loss=None, take_profit=None)
        entry_order_id = entry_order["result"]["orderId"]

        dca_order_id = None
        dca_price = None
        if signal.dca and qty_dca > 0:
            dca_price = self.bybit.round_price(symbol, signal.dca)
            dca_order = self.bybit.place_limit_order(symbol, side, qty_dca, dca_price,
                                                     stop_loss=None, take_profit=None)
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
            raw_signal=signal.raw_text or signal.asset,
            dca_price=dca_price,
        )

        entry_desc = "Market" if signal.entry_is_market else "Limit"
        sl_desc = " with native SL" if signal.entry_is_market else " (SL applied after fill)"
        if tp is not None:
            return f"{entry_desc} entry placed for {symbol}{sl_desc} & TP."
        return f"{entry_desc} entry placed for {symbol}{sl_desc}."

    def cancel(self, symbol: str) -> str:
        with self._lock:
            self.pending.pop(symbol, None)
        return f"Trade for {symbol} cancelled."

    # ---------- stage 3: fill-driven protective order management ----------

    def sync_protective_orders(self, symbol: str):
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        position = self.bybit.get_open_position(symbol)
        if not position:
            return

        sl_price = self.bybit.round_price(symbol, state["sl_price"])
        try:
            self.bybit.set_position_sl(symbol, sl_price, position_idx=0)
        except Exception as e:
            log.warning("set_position_sl failed for %s: %s", symbol, e)

    def handle_tp_fill(self, symbol: str, filled_order_id: str):
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        tp_ids = self.db.loads(state["tp_order_ids"])
        if filled_order_id not in tp_ids:
            return

        idx = tp_ids.index(filled_order_id)
        tp_prices = self.db.loads(state["tp_prices"])
        filled_price = None
        if idx < len(tp_prices):
            filled_price = tp_prices.pop(idx)

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

        self.sync_protective_orders(symbol)

        if first_tp:
            self.notify(f"🎯 TP hit on {symbol} — SL moved to breakeven ({new_sl}).")
        else:
            self.notify(f"🎯 Another TP hit on {symbol} — SL resynced to remaining size.")

    # ---------- manual TP detection (user places TPs on Bybit UI) ----------

    def handle_manual_tp_fill(self, symbol: str) -> int:
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
        self.db.upsert(symbol, breakeven_prompt_msg_id=None)

    def handle_sl_fill(self, symbol: str, source: str = "SL"):
        with self._lock:
            state = self.db.get(symbol)
            if not state:
                return
            self.db.delete(symbol)

        try:
            self.bybit.cancel_all(symbol)
            position = self.bybit.get_open_position(symbol)
            if position and float(position.get("size", 0)) > 0:
                side = position["side"]
                close_side = "Sell" if side == "Buy" else "Buy"
                qty = float(position["size"])
                self.bybit.close_position_market(symbol, close_side, qty)
                self.notify(f"🛑 {source} triggered on {symbol} — residual detected, force-closed {qty}.")
        except Exception as e:
            log.error("Force-close failed for %s: %s", symbol, e)
            self.notify(f"⚠️ {source} on {symbol} — force-close failed: {e}")
        finally:
            self.notify(f"🛑 {source} triggered on {symbol} — position closed.")

    def handle_entry_or_dca_fill(self, symbol: str):
        position = self.bybit.get_open_position(symbol)
        if position:
            avg_price = float(position.get("avgPrice", 0))
            state = self.db.get(symbol)
            if state and state["entry_price"] == 0 and avg_price > 0:
                self.db.upsert(symbol, entry_price=avg_price)
                # Only check SL breach on the initial entry fill (not DCA fills)
                sl_price = state.get("sl_price", 0)
                pos_side = state.get("position", "")
                try:
                    ticker = self.bybit.http.get_tickers(category=config.BYBIT_CATEGORY, symbol=symbol)
                    mark = float(ticker["result"]["list"][0]["markPrice"])
                except Exception:
                    mark = None
                if mark is not None and sl_price > 0:
                    sl_breached = (pos_side == "LONG" and mark <= sl_price) or (pos_side == "SHORT" and mark >= sl_price)
                    if sl_breached:
                        log.warning("%s entry filled but mark %.2f already past SL %.2f — closing immediately", symbol, mark, sl_price)
                        self.handle_sl_fill(symbol, "SL")
                        return
        self.sync_protective_orders(symbol)

    # ---------- modification commands (sl, tp, dca, entry) ----------

    def _dca_qty_from_state(self, state: dict) -> float:
        equity = self.bybit.get_equity_usdt()
        risk_pct = config.RISK_PERCENT
        risk_amount = equity * (risk_pct / 100)
        dca_price = state.get("dca_price") or 0
        if dca_price <= 0:
            return 0.0
        entry_price = state["entry_price"]
        if entry_price == 0:
            entry_price = state["sl_price"]
        qty = risk_amount / max(abs(entry_price - state["sl_price"]), 1e-8)
        return self.bybit.round_qty(state["symbol"], qty)

    def stage_modify_sl(self, symbol: str, new_sl: float) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                prompt = f"Modify SL for {symbol}?\n  Current: {signal.sl}\n  New: {new_sl}"
                with self._lock:
                    self.pending_mods[symbol] = {"type": "sl", "params": {"new_price": new_sl}, "chat_id": None, "message_id": None}
                return prompt
            raise ValueError(f"No active position or pending trade for {symbol}.")

        old_sl = state["sl_price"]
        prompt = f"Modify SL for {symbol}?\n  Current: {old_sl}\n  New: {new_sl}"
        with self._lock:
            self.pending_mods[symbol] = {"type": "sl", "params": {"new_price": new_sl}, "chat_id": None, "message_id": None}
        return prompt

    def stage_modify_tp(self, symbol: str, new_prices: List[float]) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                old = ", ".join(str(t) for t in signal.tps)
                prompt = f"Modify TPs for {symbol}?\n  Current: {old}\n  New: {', '.join(str(t) for t in new_prices)}"
                with self._lock:
                    self.pending_mods[symbol] = {"type": "tp", "params": {"new_prices": new_prices}, "chat_id": None, "message_id": None}
                return prompt
            raise ValueError(f"No active position or pending trade for {symbol}.")

        old = ", ".join(str(t) for t in self.db.loads(state["tp_prices"]))
        prompt = f"Modify TPs for {symbol}?\n  Current: {old}\n  New: {', '.join(str(t) for t in new_prices)}"
        with self._lock:
            self.pending_mods[symbol] = {"type": "tp", "params": {"new_prices": new_prices}, "chat_id": None, "message_id": None}
        return prompt

    def stage_modify_dca(self, symbol: str, dca_price: Optional[float]) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                if dca_price is None:
                    prompt = f"Remove DCA for {symbol}?"
                else:
                    prompt = f"Add DCA for {symbol}?\n  Price: {dca_price}"
                with self._lock:
                    self.pending_mods[symbol] = {"type": "dca", "params": {"new_price": dca_price}, "chat_id": None, "message_id": None}
                return prompt
            raise ValueError(f"No active position or pending trade for {symbol}.")

        if dca_price is None:
            prompt = f"Remove DCA for {symbol}?"
        else:
            prompt = f"Modify DCA for {symbol}?\n  Price: {dca_price}"
        with self._lock:
            self.pending_mods[symbol] = {"type": "dca", "params": {"new_price": dca_price}, "chat_id": None, "message_id": None}
        return prompt

    def stage_modify_entry(self, symbol: str, new_price: Optional[float], is_market: bool) -> str:
        if symbol in self.pending:
            entry_desc = "MARKET" if is_market else new_price
            return f"Modify Entry for {symbol}?\n  New: {entry_desc}"
        raise ValueError(f"No pending trade for {symbol} — entry can only be modified before confirmation.")

    def apply_modification(self, symbol: str) -> str:
        with self._lock:
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
        with self._lock:
            self.pending_mods.pop(symbol, None)
        return f"Modification for {symbol} cancelled."

    # ---------- status & close ----------

    def get_status(self, symbol: Optional[str] = None) -> str:
        wallet = self.bybit.get_wallet_info()
        lines = [f"📊 Equity: ${wallet['equity']:,.2f} | Available: ${wallet['available']:,.2f}"]

        symbols = [symbol] if symbol else self.db.all_active()
        if not symbols:
            lines.append("\nNo active positions.")
            return "\n".join(lines)

        for sym in symbols:
            state = self.db.get(sym)
            pos = self.bybit.get_open_position(sym)
            if not state or not pos:
                lines.append(f"\n{sym}: no active position")
                continue
            side = state["position"]
            entry = state["entry_price"] or float(pos.get("avgPrice", 0))
            mark = float(pos.get("markPrice", 0))
            qty = float(pos.get("size", 0))
            leverage = int(float(pos.get("leverage", 1)))
            pnl = float(pos.get("unrealisedPnl", 0))
            pnl_pct = (pnl / max(entry * qty / leverage, 1e-8)) * 100 if entry > 0 else 0
            sl = state["sl_price"]
            tp_raw = self.db.loads(state.get("tp_prices", "[]"))
            tps = ", ".join(str(t) for t in tp_raw) if tp_raw else "none"
            dca_info = f"\n  DCA: {state['dca_price']}" if state.get("dca_price") else ""
            be = " ✓" if state.get("breakeven_moved") else ""

            lines.append(
                f"\n{sym} {side}{be}"
                f"\n  Entry: {entry:,.1f} | Mark: {mark:,.1f}"
                f"\n  PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)"
                f"\n  SL: {sl} | TP: {tps}"
                f"{dca_info}"
            )

        return "\n".join(lines)

    def close_position(self, symbol: str) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return f"No active position for {symbol}."
        self.handle_sl_fill(symbol, source="Manual close")
        return f"✅ {symbol} position closed."
