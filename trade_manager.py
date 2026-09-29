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

ALERT_COOLDOWN_SECONDS = 600
# A freshly confirmed trade gets this long before the watchdog may decide its
# entry order vanished (orders are placed right after the state is written).
NEW_TRADE_GRACE_SECONDS = 120


class TradeManager:
    def __init__(self, bybit: BybitClient, db: StateDB, notify):
        self.bybit = bybit
        self.db = db
        self.notify = notify
        self._lock = threading.Lock()
        self.pending = {}
        self.pending_mods = {}
        self._last_alert = {}

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

    def _entry_order_resting(self, symbol: str, state: dict) -> bool:
        entry_id = state.get("entry_order_id")
        if not entry_id:
            return False
        return any(o.get("orderId") == entry_id for o in self.bybit.get_open_orders(symbol))

    def _reconcile_symbol(self, symbol: str) -> str:
        state = self.db.get(symbol)
        pos = self.bybit.get_open_position(symbol)
        if pos and float(pos.get("size", 0)) > 0:
            actual_size = float(pos["size"])
            updates = {"breakeven_prompt_msg_id": None, "position_opened": 1}
            if not state.get("entry_price") and float(pos.get("avgPrice", 0)) > 0:
                updates["entry_price"] = float(pos["avgPrice"])
            self.db.upsert(symbol, **updates)
            log.info("Reconcile %s: size=%s avg=%s exchange_sl=%r trailing=%r db_sl=%s be_moved=%s",
                     symbol, actual_size, pos.get("avgPrice"), pos.get("stopLoss"),
                     pos.get("trailingStop"), state.get("sl_price"), state.get("breakeven_moved"))
            synced = False
            if state.get("sl_price"):
                synced = self.sync_protective_orders(symbol)
            if state.get("trailing_distance") and state.get("breakeven_moved"):
                try:
                    self._activate_trailing_if_needed(symbol)
                except Exception as e:
                    log.warning("Could not re-activate trailing for %s: %s", symbol, e)
            if synced:
                return f"✅ {symbol}: reconciled (size {actual_size}), SL resynced."
            else:
                return f"⚠️ {symbol}: reconciled (size {actual_size}) but SL could NOT be set — check Bybit."
        elif not state.get("position_opened") and self._entry_order_resting(symbol, state):
            # Limit entry still waiting to fill — this is NOT a closed position.
            log.info("Reconcile %s: entry order %s still resting, db_sl=%s",
                     symbol, state.get("entry_order_id"), state.get("sl_price"))
            self.sync_protective_orders(symbol)
            return f"⏳ {symbol}: limit entry still waiting to fill — kept."
        else:
            try:
                self.bybit.cancel_all(symbol)
            except Exception as e:
                log.warning("cancel_all failed for %s during reconcile: %s", symbol, e)
            self.db.delete(symbol)
            return f"🛑 {symbol}: position closed while offline — cleaned up."

    def reconcile(self) -> list[str]:
        messages = []

        for symbol in self.db.all_active():
            try:
                messages.append(self._reconcile_symbol(symbol))
            except Exception as e:
                log.error("Reconcile failed for %s: %s", symbol, e)
                messages.append(f"⚠️ {symbol}: could not reconcile ({e}) — watchdog will retry.")

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
                exchange_sl = float(pos.get("stopLoss") or 0)
                self.db.upsert(
                    b_sym,
                    position=side,
                    status="active",
                    position_opened=1,
                    entry_price=entry,
                    sl_price=exchange_sl,
                    original_sl_price=exchange_sl,
                    tp_prices=self.db.dumps([]),
                    breakeven_moved=0,
                    manual_tp_count=0,
                    breakeven_prompt_msg_id=None,
                    entry_order_id="",
                    dca_order_id=None,
                    dca_price=None,
                )
                if exchange_sl > 0:
                    messages.append(f"⚠️ {b_sym}: orphan position on Bybit — recovered ({side}, {size}), SL {exchange_sl}")
                else:
                    messages.append(f"⚠️ {b_sym}: orphan position on Bybit — recovered ({side}, {size}). "
                                    f"It has NO stop loss — set one with /sl {b_sym} <price>")

        return messages

    # ---------- stage 1: validate + queue for confirmation ----------

    def stage_signal(self, signal: ParsedSignal) -> str:
        if signal.errors:
            raise ValueError("Signal rejected:\n- " + "\n- ".join(signal.errors))

        symbol = signal.asset
        if self.bybit.has_open_orders_or_position(symbol):
            raise ValueError(f"{symbol} already has an open position or pending order — new signal rejected.")

        # The SL is attached to the DCA order too, so it must sit beyond the DCA.
        if signal.dca is not None:
            if signal.position == "LONG" and signal.dca <= signal.sl:
                raise ValueError(f"For LONG, DCA {signal.dca} must be above SL {signal.sl}.")
            if signal.position == "SHORT" and signal.dca >= signal.sl:
                raise ValueError(f"For SHORT, DCA {signal.dca} must be below SL {signal.sl}.")

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

        trailing_desc = ""
        if signal.trailing_r_mult is not None:
            trailing_desc = f"\nTrailing: {signal.trailing_r_mult}R after breakeven"

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
        lines.append(f"Leverage: {signal.leverage}x ({signal.leverage_mode or config.DEFAULT_MARGIN_MODE}){trailing_desc}")
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

        qty_entry, qty_dca, risk_amount, *_ = entry.get("cached_qty") or self._calc_qty(signal)
        side = "Buy" if signal.position == "LONG" else "Sell"

        max_lev = self.bybit.get_max_leverage(symbol)
        leverage = min(signal.leverage, max_lev)
        self.bybit.set_margin_mode(symbol, signal.leverage_mode or config.DEFAULT_MARGIN_MODE)
        self.bybit.set_leverage(symbol, leverage)

        trailing_distance = None
        if signal.trailing_r_mult is not None and not signal.entry_is_market and signal.entry and signal.sl:
            # Market entries get their distance after the fill (handle_entry_or_dca_fill)
            td = round(abs(signal.entry - signal.sl) * signal.trailing_r_mult, 2)
            trailing_distance = td if td > 0 else None

        # Persist the trade BEFORE placing any order. A limit priced at/through
        # the market fills instantly, and its WebSocket fill event can arrive
        # before place_order() even returns — it must find this state.
        self.db.delete(symbol)
        self.db.upsert(
            symbol,
            position=signal.position,
            status="active",
            position_opened=0,
            entry_order_id=None,
            dca_order_id=None,
            entry_price=signal.entry if not signal.entry_is_market else 0,
            sl_price=signal.sl,
            original_sl_price=signal.sl,
            tp_prices=self.db.dumps(signal.tps),
            breakeven_moved=0,
            raw_signal=signal.raw_text or signal.asset,
            dca_price=None,
            risk_amount=risk_amount,
            entry_qty=qty_entry,
            trailing_distance=trailing_distance,
            trailing_r_mult=signal.trailing_r_mult,
        )

        # The SL is attached to every opening order (market, limit entry, DCA),
        # so Bybit arms it on the fill itself — protection no longer depends on
        # the bot being online and catching the fill event.
        tp = signal.tps[0] if signal.tps else None
        try:
            if signal.entry_is_market:
                entry_order = self.bybit.place_market_order(symbol, side, qty_entry,
                                                            stop_loss=signal.sl, take_profit=tp)
            else:
                entry_price = self.bybit.round_price(symbol, signal.entry)
                entry_order = self.bybit.place_limit_order(symbol, side, qty_entry, entry_price,
                                                           stop_loss=signal.sl, take_profit=None)
        except Exception:
            self._cleanup_failed_entry(symbol)
            raise
        self.db.upsert(symbol, entry_order_id=entry_order["result"]["orderId"])

        dca_warning = ""
        if signal.dca and qty_dca > 0:
            dca_price = self.bybit.round_price(symbol, signal.dca)
            try:
                dca_order = self.bybit.place_limit_order(symbol, side, qty_dca, dca_price,
                                                         stop_loss=signal.sl, take_profit=None)
                self.db.upsert(symbol, dca_order_id=dca_order["result"]["orderId"], dca_price=dca_price)
            except Exception as e:
                log.error("DCA order failed for %s: %s", symbol, e)
                dca_warning = (f"\n⚠️ DCA order at {dca_price} FAILED: {e}\n"
                               f"Entry is live with its SL. Retry with /dca {symbol} {dca_price}")

        entry_desc = "Market" if signal.entry_is_market else "Limit"
        tp_desc = " & TP" if tp is not None and signal.entry_is_market else ""
        return f"{entry_desc} entry placed for {symbol} with native SL{tp_desc}.{dca_warning}"

    def _cleanup_failed_entry(self, symbol: str):
        """Entry placement raised. Keep tracking if anything reached Bybit anyway."""
        try:
            exists = self.bybit.has_open_orders_or_position(symbol)
        except Exception as e:
            log.error("Could not verify %s after failed entry: %s", symbol, e)
            exists = True  # can't tell — keep state so the watchdog keeps checking
        if exists:
            log.error("%s entry call failed but orders/position exist on Bybit — keeping state", symbol)
            self.notify(f"⚠️ {symbol}: entry request errored but something is live on Bybit. "
                        f"Bot is still tracking it — check Bybit.")
        else:
            self.db.delete(symbol)

    def cancel(self, symbol: str) -> str:
        with self._lock:
            self.pending.pop(symbol, None)
        return f"Trade for {symbol} cancelled."

    # ---------- stage 3: fill-driven protective order management ----------

    def _alert(self, key: str, text: str, every: float = ALERT_COOLDOWN_SECONDS):
        """Telegram notify, rate-limited per key so the watchdog can't spam."""
        now = time.time()
        with self._lock:
            if now - self._last_alert.get(key, 0) < every:
                return
            self._last_alert[key] = now
        self.notify(text)

    def _sync_resting_order_sl(self, symbol: str, state: dict, sl_price: float):
        """Keep the SL attached to unfilled entry/DCA orders equal to the current SL,
        so a later fill doesn't re-arm a stale SL on the position."""
        ours = {state.get("entry_order_id"), state.get("dca_order_id")} - {None, ""}
        if not ours:
            return
        tol = sl_price * 1e-9
        for order in self.bybit.get_open_orders(symbol):
            if order.get("orderId") not in ours:
                continue
            current = float(order.get("stopLoss") or 0)
            if abs(current - sl_price) > tol:
                try:
                    self.bybit.amend_order_sl(symbol, order["orderId"], sl_price)
                    log.info("Amended resting order %s SL %s -> %s", order["orderId"], current, sl_price)
                except Exception as e:
                    log.warning("Could not amend SL on resting order %s (%s): %s", order["orderId"], symbol, e)

    def sync_protective_orders(self, symbol: str) -> bool:
        """
        Make Bybit's SL match state. Returns True when the SL is in place (or
        there's no position yet), False when it could not be set — in which
        case the user has been alerted.

        The fixed SL is kept even while a trailing stop is configured: Bybit
        runs both side by side, and before the trailing stop activates the
        fixed SL is the only protection.
        """
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return True
        raw_sl = state.get("sl_price") or 0
        if raw_sl <= 0:
            log.warning("%s has no SL on record — nothing to sync", symbol)
            return False
        sl_price = self.bybit.round_price(symbol, raw_sl)

        try:
            self._sync_resting_order_sl(symbol, state, sl_price)
        except Exception as e:
            log.warning("Resting-order SL sync failed for %s: %s", symbol, e)

        try:
            position = self.bybit.get_open_position(symbol)
            if not position:
                return True
            self.bybit.set_position_sl(symbol, sl_price, position_idx=0)
            return True
        except Exception as e:
            log.error("Failed to set SL %s on %s: %s", sl_price, symbol, e)
            self._alert(f"sl_fail:{symbol}",
                        f"⚠️ Could not set SL {sl_price} on {symbol}: {e}\n"
                        f"The position may be UNPROTECTED — check Bybit. Bot will keep retrying.")
            return False

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
            self._activate_trailing_if_needed(symbol)

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

    def apply_breakeven(self, symbol: str) -> bool:
        """Move SL to entry (then arm trailing if configured). Returns True on success."""
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return False
        if state.get("breakeven_moved"):
            return True

        new_sl = state["entry_price"] or state["original_sl_price"]
        self.db.upsert(symbol, sl_price=new_sl, breakeven_moved=1, breakeven_prompt_msg_id=None)

        # Always move the fixed SL first: a trailing stop with an activation
        # price does nothing until price reaches it, so on its own it would
        # leave the original SL in place.
        ok = self.sync_protective_orders(symbol)
        self._activate_trailing_if_needed(symbol)

        if ok:
            self.notify(f"✅ SL moved to entry ({new_sl}) for {symbol}.")
        else:
            self.notify(f"⚠️ Failed to move SL to entry ({new_sl}) for {symbol} — check Bybit.")
        return ok

    def clear_breakeven_prompt(self, symbol: str):
        self.db.upsert(symbol, breakeven_prompt_msg_id=None)

    def _activate_trailing_if_needed(self, symbol: str) -> bool:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return False
        dist = state.get("trailing_distance")
        if not dist or float(dist) <= 0:
            return False
        entry = state.get("entry_price", 0)
        if entry <= 0:
            return False
        pos = state.get("position", "LONG")
        distance = float(dist)
        activation = entry + distance if pos == "LONG" else entry - distance
        try:
            self.bybit.set_trailing_stop(symbol, distance, activation=activation)
            r_mult = state.get("trailing_r_mult", "?")
            log.info("Trailing stop activated for %s: %.2f USDT (%sR), activation=%s",
                     symbol, distance, r_mult, activation)
            self.notify(f"↗️ Trailing active on {symbol}: −${distance:,.0f} ({r_mult}R)")
            return True
        except Exception as e:
            log.warning("Failed to activate trailing for %s: %s", symbol, e)
            return False

    def handle_sl_fill(self, symbol: str, source: str = "SL"):
        with self._lock:
            state = self.db.get(symbol)
            if not state:
                return
            self.db.delete(symbol)

        try:
            self.bybit.cancel_trailing_stop(symbol)
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
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        # The REST position can lag the WebSocket fill event by a moment.
        position = None
        for attempt in range(5):
            position = self.bybit.get_open_position(symbol)
            if position:
                break
            time.sleep(0.5)
        if not position:
            log.warning("%s: fill reported but no position visible yet — watchdog will re-check", symbol)
            return
        if not state.get("position_opened"):
            self.db.upsert(symbol, position_opened=1)
        avg_price = float(position.get("avgPrice", 0))
        state = self.db.get(symbol)
        if state and state["entry_price"] == 0 and avg_price > 0:
            self.db.upsert(symbol, entry_price=avg_price)
            # Trailing: calculate distance from fill price if r_mult is set
            if state.get("trailing_r_mult") and not state.get("trailing_distance"):
                r_mult = float(state["trailing_r_mult"])
                sl_price = state.get("sl_price", 0)
                if sl_price > 0 and r_mult > 0:
                    risk = abs(avg_price - sl_price)
                    td = round(risk * r_mult, 2)
                    if td > 0:
                        self.db.upsert(symbol, trailing_distance=td)
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

    # ---------- watchdog (safety net for missed WebSocket events) ----------

    def ensure_protection(self):
        """
        Periodic check of every active trade against Bybit. Catches anything
        the event-driven path missed: dropped fill events, a dead WebSocket,
        a failed SL call, or a close that happened while disconnected.
        """
        for symbol in self.db.all_active():
            try:
                self._check_symbol(symbol)
            except Exception as e:
                log.warning("Watchdog check failed for %s: %s", symbol, e)

    def _check_symbol(self, symbol: str):
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return
        pos = self.bybit.get_open_position(symbol)

        if pos:
            if not state.get("position_opened"):
                log.warning("Watchdog: %s has a position but its fill was never processed — handling now", symbol)
                self.handle_entry_or_dca_fill(symbol)
                return
            if float(pos.get("stopLoss") or 0) > 0:
                return
            sl = state.get("sl_price") or 0
            if sl > 0:
                log.warning("Watchdog: %s position has NO stop loss on Bybit — re-applying %s", symbol, sl)
                if self.sync_protective_orders(symbol):
                    self._alert(f"sl_fixed:{symbol}", f"🛡️ {symbol} had no SL on Bybit — re-applied SL at {sl}.")
            else:
                self._alert(f"no_sl:{symbol}",
                            f"⚠️ {symbol} has NO stop loss and none is on record. "
                            f"Set one with /sl {symbol} <price>")
            return

        if state.get("position_opened"):
            log.warning("Watchdog: %s position is gone but the close event was missed — cleaning up", symbol)
            self.handle_sl_fill(symbol, "Position closed")
            return

        # Not filled yet: is the entry still resting on Bybit?
        if time.time() - (state.get("created_at") or 0) < NEW_TRADE_GRACE_SECONDS:
            return
        open_orders = self.bybit.get_open_orders(symbol)
        entry_id = state.get("entry_order_id")
        if entry_id:
            resting = any(o.get("orderId") == entry_id for o in open_orders)
        else:
            resting = any(not o.get("reduceOnly") for o in open_orders)
        if resting:
            return
        if self.bybit.get_open_position(symbol):
            return  # filled between the two calls — next pass handles it
        log.warning("Watchdog: %s entry order is no longer on Bybit and no position exists — cleaning up", symbol)
        try:
            self.bybit.cancel_all(symbol)
        except Exception as e:
            log.warning("cancel_all failed for %s: %s", symbol, e)
        self.db.delete(symbol)
        self.notify(f"🗑️ {symbol}: entry order is no longer on Bybit (cancelled or rejected) — trade removed.")

    # ---------- trailing stop ----------

    def cancel_trailing(self, symbol: str) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return f"No active position for {symbol}."
        if not state.get("trailing_distance"):
            return f"No trailing stop active on {symbol}."

        self.bybit.cancel_trailing_stop(symbol)
        original_sl = state.get("original_sl_price")
        self.db.upsert(symbol, trailing_distance=None, trailing_r_mult=None)
        if original_sl and float(original_sl) > 0:
            self.db.upsert(symbol, sl_price=float(original_sl))
            self.sync_protective_orders(symbol)
            return f"Trailing cancelled for {symbol}. SL restored to {original_sl}."
        return f"Trailing cancelled for {symbol}."

    def stage_modify_trail(self, symbol: str, mult: float) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            raise ValueError(f"No active position for {symbol}.")
        if not state.get("breakeven_moved"):
            raise ValueError(
                f"Trailing can only be set after breakeven for {symbol}. "
                f"Close at least one TP first."
            )
        entry = state["entry_price"]
        original_sl = state.get("original_sl_price", 0)
        if entry <= 0 or float(original_sl) <= 0:
            raise ValueError(f"Cannot determine R (entry={entry}, original_sl={original_sl})")
        risk = abs(entry - float(original_sl))
        td = round(risk * mult, 2)
        prompt = (
            f"Set trailing stop for {symbol}?\n"
            f"  R (entry − original SL): ${risk:,.0f}\n"
            f"  Multiplier: {mult}R\n"
            f"  Distance: ${td:,.0f} retracement from peak"
        )
        with self._lock:
            self.pending_mods[symbol] = {
                "type": "trail",
                "params": {"distance": td, "r_mult": mult},
                "chat_id": None, "message_id": None,
            }
        return prompt

    # ---------- modification commands (sl, tp, dca, entry) ----------

    def _dca_qty_from_state(self, state: dict, dca_price: float) -> tuple[float, float, float]:
        """
        Size a DCA from the risk budget the entry hasn't used yet, so that
        entry + DCA both hitting SL loses about the trade's original risk.
        Returns (qty, remaining_risk, risk_amount); raises ValueError with a
        user-facing reason when a DCA can't be sized safely.
        """
        symbol = state["symbol"]
        sl = state.get("sl_price") or 0
        if sl <= 0:
            raise ValueError(f"{symbol} has no SL on record — set one with /sl before adding a DCA.")
        if state["position"] == "LONG" and dca_price <= sl:
            raise ValueError(f"For LONG, DCA {dca_price} must be above SL {sl}.")
        if state["position"] == "SHORT" and dca_price >= sl:
            raise ValueError(f"For SHORT, DCA {dca_price} must be below SL {sl}.")

        risk_amount = state.get("risk_amount")
        if not risk_amount:
            risk_amount = self.bybit.get_equity_usdt() * (config.RISK_PERCENT / 100)

        position = self.bybit.get_open_position(symbol)
        if position:
            size = float(position["size"])
            avg = float(position.get("avgPrice", 0))
        else:
            size = state.get("entry_qty") or 0
            avg = state.get("entry_price") or 0
        if size <= 0 or avg <= 0:
            raise ValueError(f"Entry for {symbol} hasn't filled yet — add the DCA after it fills.")

        used = size * abs(avg - sl)
        remaining = risk_amount - used
        if remaining <= 0:
            raise ValueError(
                f"Entry already risks ${used:,.2f} of the ${risk_amount:,.2f} budget — "
                f"a DCA would exceed your risk. Not placed."
            )
        qty = self.bybit.round_qty(symbol, remaining / abs(dca_price - sl))
        return qty, remaining, risk_amount

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

        if new_sl <= 0:
            raise ValueError(f"Invalid SL {new_sl}.")
        try:
            ticker = self.bybit.http.get_tickers(category=config.BYBIT_CATEGORY, symbol=symbol)
            mark = float(ticker["result"]["list"][0]["markPrice"])
        except Exception:
            mark = None
        if mark is not None:
            if state["position"] == "LONG" and new_sl >= mark:
                raise ValueError(f"For LONG, SL {new_sl} must be below current mark price {mark}.")
            if state["position"] == "SHORT" and new_sl <= mark:
                raise ValueError(f"For SHORT, SL {new_sl} must be above current mark price {mark}.")

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
            qty, remaining, risk_amount = self._dca_qty_from_state(state, dca_price)
            prompt = (f"Modify DCA for {symbol}?\n  Price: {dca_price}  (qty ~{qty})\n"
                      f"  Uses remaining risk ${remaining:,.2f} of ${risk_amount:,.2f}")
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
            if state.get("trailing_distance"):
                self.bybit.cancel_trailing_stop(symbol)
                self.db.upsert(symbol, trailing_distance=None, trailing_r_mult=None)
            self.db.upsert(symbol, sl_price=params["new_price"])
            if self.sync_protective_orders(symbol):
                return f"✅ SL updated for {symbol} to {params['new_price']}."
            return (f"⚠️ SL for {symbol} saved as {params['new_price']} but Bybit did NOT accept it — "
                    f"the old SL may still be active. Check Bybit.")

        elif mod_type == "tp":
            self.db.upsert(symbol, tp_prices=self.db.dumps(params["new_prices"]))
            self.sync_protective_orders(symbol)
            return f"✅ TPs updated for {symbol}: {', '.join(str(t) for t in params['new_prices'])}."

        elif mod_type == "dca":
            new_price = params["new_price"]
            dca_qty = 0.0
            if new_price is not None:
                # Size before cancelling anything, so a rejected DCA leaves the old one intact
                try:
                    dca_qty, *_ = self._dca_qty_from_state(state, new_price)
                except ValueError as e:
                    return f"❌ DCA not changed: {e}"
            if state.get("dca_order_id"):
                self.bybit.cancel_order(symbol, state["dca_order_id"])
            self.db.upsert(symbol, dca_order_id=None, dca_price=None)
            if new_price is not None and dca_qty > 0:
                side = "Buy" if state["position"] == "LONG" else "Sell"
                dca_price = self.bybit.round_price(symbol, new_price)
                dca_order = self.bybit.place_limit_order(symbol, side, dca_qty, dca_price,
                                                         stop_loss=state["sl_price"])
                self.db.upsert(symbol, dca_order_id=dca_order["result"]["orderId"], dca_price=dca_price)
                return f"✅ DCA placed for {symbol} at {dca_price} (qty ~{dca_qty}) with SL {state['sl_price']}."
            return f"✅ DCA removed for {symbol}."

        elif mod_type == "trail":
            distance = params["distance"]
            r_mult = params["r_mult"]
            self.db.upsert(symbol, trailing_distance=distance, trailing_r_mult=r_mult)
            if state.get("breakeven_moved"):
                self._activate_trailing_if_needed(symbol)
            return f"✅ Trailing stop set for {symbol}: −${distance:,.0f} ({r_mult}R)."

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
            trail_info = ""
            if state.get("trailing_distance") and state.get("breakeven_moved"):
                dist = float(state["trailing_distance"])
                r_mult = state.get("trailing_r_mult")
                trail_info = f"\n  Trailing: −${dist:,.0f} from peak" + (f" ({r_mult}R)" if r_mult else "")

            lines.append(
                f"\n{sym} {side}{be}"
                f"\n  Entry: {entry:,.1f} | Mark: {mark:,.1f}"
                f"\n  PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)"
                f"\n  SL: {sl} | TP: {tps}"
                f"{dca_info}{trail_info}"
            )

        return "\n".join(lines)

    def close_position(self, symbol: str) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return f"No active position for {symbol}."
        self.handle_sl_fill(symbol, source="Manual close")
        return f"✅ {symbol} position closed."
