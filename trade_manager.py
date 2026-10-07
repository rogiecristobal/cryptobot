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
from bitunix_client import BitunixClient
from state_db import StateDB
import config

log = logging.getLogger("trade_manager")

ALERT_COOLDOWN_SECONDS = 600
# A freshly confirmed trade gets this long before the watchdog may decide its
# entry order vanished (orders are placed right after the state is written).
NEW_TRADE_GRACE_SECONDS = 120


class TradeManager:
    def __init__(self, exchange: BitunixClient, db: StateDB, notify):
        self.exchange = exchange
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
        return any(o["order_id"] == entry_id for o in self.exchange.get_open_orders(symbol))

    def _reconcile_symbol(self, symbol: str) -> str:
        state = self.db.get(symbol)
        pos = self.exchange.get_open_position(symbol)
        if pos and pos["size"] > 0:
            actual_size = pos["size"]
            updates = {"breakeven_prompt_msg_id": None, "position_opened": 1}
            if not state.get("entry_price") and pos["avg_price"] > 0:
                updates["entry_price"] = pos["avg_price"]
            self.db.upsert(symbol, **updates)
            log.info("Reconcile %s: size=%s avg=%s exchange_sl=%s db_sl=%s exchange_tp=%s db_tp=%s "
                     "trailing=%s be_moved=%s",
                     symbol, actual_size, pos["avg_price"], pos["sl_prices"], state.get("sl_price"),
                     pos["tp_prices"], state.get("tp_prices"),
                     state.get("trailing_distance"), state.get("breakeven_moved"))
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
                return f"⚠️ {symbol}: reconciled (size {actual_size}) but SL could NOT be set — check Bitunix."
        elif not state.get("position_opened") and self._entry_order_resting(symbol, state):
            # Limit entry still waiting to fill — this is NOT a closed position.
            log.info("Reconcile %s: entry order %s still resting, db_sl=%s",
                     symbol, state.get("entry_order_id"), state.get("sl_price"))
            self.sync_protective_orders(symbol)
            return f"⏳ {symbol}: limit entry still waiting to fill — kept."
        else:
            try:
                self.exchange.cancel_all(symbol)
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

        exchange_positions = self.exchange.get_all_open_positions()
        db_active = self.db.all_active()
        db_by_norm = {}
        for s in db_active:
            norm = s.replace("/", "").replace(" ", "")
            db_by_norm[norm] = s

        for pos in exchange_positions:
            b_sym = pos["symbol"]
            if b_sym not in db_by_norm:
                side = pos["side"]
                size = pos["size"]
                entry = pos["avg_price"]
                exchange_sl = pos["stop_loss"]
                exchange_tp = pos["take_profit"]
                self.db.upsert(
                    b_sym,
                    position=side,
                    status="active",
                    position_opened=1,
                    entry_price=entry,
                    sl_price=exchange_sl,
                    original_sl_price=exchange_sl,
                    tp_prices=self.db.dumps([exchange_tp] if exchange_tp > 0 else []),
                    breakeven_moved=0,
                    manual_tp_count=0,
                    breakeven_prompt_msg_id=None,
                    entry_order_id="",
                    dca_order_id=None,
                    dca_price=None,
                )
                if exchange_sl > 0:
                    messages.append(f"⚠️ {b_sym}: orphan position on Bitunix — recovered ({side}, {size}), SL {exchange_sl}")
                else:
                    messages.append(f"⚠️ {b_sym}: orphan position on Bitunix — recovered ({side}, {size}). "
                                    f"It has NO stop loss — set one with /sl {b_sym} <price>")

        return messages

    # ---------- stage 1: validate + queue for confirmation ----------

    def stage_signal(self, signal: ParsedSignal) -> str:
        if signal.errors:
            raise ValueError("Signal rejected:\n- " + "\n- ".join(signal.errors))

        symbol = signal.asset
        self.exchange.check_tradable(symbol)
        if self.exchange.has_open_orders_or_position(symbol):
            raise ValueError(f"{symbol} already has an open position or pending order — new signal rejected.")

        # The SL is attached to the DCA order too, so it must sit beyond the DCA.
        if signal.dca is not None:
            if signal.position == "LONG" and signal.dca <= signal.sl:
                raise ValueError(f"For LONG, DCA {signal.dca} must be above SL {signal.sl}.")
            if signal.position == "SHORT" and signal.dca >= signal.sl:
                raise ValueError(f"For SHORT, DCA {signal.dca} must be below SL {signal.sl}.")

        try:
            mark = self.exchange.get_mark_price(symbol)
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
        equity = self.exchange.get_equity_usdt()

        risk_pct = signal.margin_percent if signal.margin_percent is not None else config.RISK_PERCENT
        risk_amount = equity * (risk_pct / 100)

        entry_price = signal.entry
        if signal.entry_is_market:
            entry_price = self.exchange.get_last_price(signal.asset)

        if signal.dca:
            w_e = config.DCA_SPLIT_RATIO
            w_d = 1 - w_e
            avg_entry = entry_price * w_e + signal.dca * w_d
            total_qty = risk_amount / abs(avg_entry - signal.sl)
            qty_entry = self.exchange.round_qty(signal.asset, total_qty * w_e)
            qty_dca = self.exchange.round_qty(signal.asset, total_qty * w_d)
        else:
            total_qty = risk_amount / abs(entry_price - signal.sl)
            qty_entry = self.exchange.round_qty(signal.asset, total_qty)
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
            mark = self.exchange.get_mark_price(symbol)
        except Exception:
            mark = None
        if mark is not None:
            if signal.position == "LONG" and signal.sl >= mark:
                return f"❌ Trade aborted: SL {signal.sl} is now above mark price {mark}."
            if signal.position == "SHORT" and signal.sl <= mark:
                return f"❌ Trade aborted: SL {signal.sl} is now below mark price {mark}."

        qty_entry, qty_dca, risk_amount, *_ = entry.get("cached_qty") or self._calc_qty(signal)
        side = "BUY" if signal.position == "LONG" else "SELL"

        max_lev = self.exchange.get_max_leverage(symbol)
        leverage = min(signal.leverage, max_lev)
        self.exchange.set_margin_mode(symbol, signal.leverage_mode or config.DEFAULT_MARGIN_MODE)
        self.exchange.set_leverage(symbol, leverage)

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

        # The SL and TP1 are attached to every opening order (market, limit entry, DCA),
        # so Bitunix creates its TP/SL order on the fill itself — protection no longer depends on
        # the bot being online and catching the fill event.
        tp = signal.tps[0] if signal.tps else None
        try:
            if signal.entry_is_market:
                entry_order = self.exchange.place_market_order(symbol, side, qty_entry,
                                                            stop_loss=signal.sl, take_profit=tp)
            else:
                entry_price = self.exchange.round_price(symbol, signal.entry)
                entry_order = self.exchange.place_limit_order(symbol, side, qty_entry, entry_price,
                                                           stop_loss=signal.sl, take_profit=tp)
        except Exception:
            self._cleanup_failed_entry(symbol)
            raise
        self.db.upsert(symbol, entry_order_id=entry_order)

        dca_warning = ""
        if signal.dca and qty_dca > 0:
            dca_price = self.exchange.round_price(symbol, signal.dca)
            try:
                dca_order = self.exchange.place_limit_order(symbol, side, qty_dca, dca_price,
                                                         stop_loss=signal.sl, take_profit=tp)
                self.db.upsert(symbol, dca_order_id=dca_order, dca_price=dca_price)
            except Exception as e:
                log.error("DCA order failed for %s: %s", symbol, e)
                dca_warning = (f"\n⚠️ DCA order at {dca_price} FAILED: {e}\n"
                               f"Entry is live with its SL. Retry with /dca {symbol} {dca_price}")

        entry_desc = "Market" if signal.entry_is_market else "Limit"
        tp_desc = f" & TP {tp}" if tp is not None else ""
        return f"{entry_desc} entry placed for {symbol} with native SL{tp_desc}.{dca_warning}"

    def _cleanup_failed_entry(self, symbol: str):
        """Entry placement raised. Keep tracking if anything reached Bitunix anyway."""
        try:
            exists = self.exchange.has_open_orders_or_position(symbol)
        except Exception as e:
            log.error("Could not verify %s after failed entry: %s", symbol, e)
            exists = True  # can't tell — keep state so the watchdog keeps checking
        if exists:
            log.error("%s entry call failed but orders/position exist on Bitunix — keeping state", symbol)
            self.notify(f"⚠️ {symbol}: entry request errored but something is live on Bitunix. "
                        f"Bot is still tracking it — check Bitunix.")
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

    def _target_tp(self, symbol: str, state: dict) -> Optional[float]:
        """TP1 from state, rounded for Bitunix. Only TP1 is placed on the exchange (it closes the whole position)."""
        tps = self.db.loads(state.get("tp_prices"))
        return self.exchange.round_price(symbol, tps[0]) if tps else None

    @staticmethod
    def _same_price(a: float, b: float) -> bool:
        return abs(a - b) <= abs(b) * 1e-9

    def _sync_resting_orders(self, symbol: str, state: dict, sl_price: float, tp_price: Optional[float]):
        """Keep the SL/TP attached to unfilled entry/DCA orders equal to state,
        so a later fill doesn't re-arm a stale SL or TP on the position."""
        ours = {state.get("entry_order_id"), state.get("dca_order_id")} - {None, ""}
        if not ours:
            return
        for order in self.exchange.get_open_orders(symbol):
            if order["order_id"] not in ours:
                continue
            sl_ok = self._same_price(order["stop_loss"], sl_price)
            tp_ok = tp_price is None or self._same_price(order["take_profit"], tp_price)
            if sl_ok and tp_ok:
                continue
            try:
                self.exchange.amend_order_tpsl(symbol, order, sl_price, tp_price)
                log.info("Amended resting order %s SL %s -> %s, TP %s -> %s", order["order_id"],
                         order["stop_loss"], sl_price, order["take_profit"], tp_price)
            except Exception as e:
                log.warning("Could not amend SL/TP on resting order %s (%s): %s", order["order_id"], symbol, e)

    def _sync_position_tp(self, symbol: str, state: dict) -> bool:
        """Put TP1 from state on the open position. Returns False (and alerts) on failure."""
        tp_price = self._target_tp(symbol, state)
        if tp_price is None:
            return True
        try:
            self.exchange.set_position_tp(symbol, tp_price)
            return True
        except Exception as e:
            log.error("Failed to set TP %s on %s: %s", tp_price, symbol, e)
            self._alert(f"tp_fail:{symbol}",
                        f"⚠️ Could not set TP {tp_price} on {symbol}: {e}\n"
                        f"SL is unaffected. Bot will keep retrying — or change it with /tp {symbol} <price>.")
            return False

    def sync_protective_orders(self, symbol: str) -> bool:
        """
        Make Bitunix's SL and TP1 match state. Returns True when the SL is in
        place (or there's no position yet), False when it could not be set —
        in which case the user has been alerted. A TP failure alerts on its
        own and never blocks the SL.

        Bitunix has no native trailing stop: update_trailing_stops() ratchets
        sl_price in the DB and calls this, so the SL on Bitunix is always the
        one in state.
        """
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return True
        raw_sl = state.get("sl_price") or 0
        if raw_sl <= 0:
            log.warning("%s has no SL on record — nothing to sync", symbol)
            return False
        sl_price = self.exchange.round_price(symbol, raw_sl)

        try:
            self._sync_resting_orders(symbol, state, sl_price, self._target_tp(symbol, state))
        except Exception as e:
            log.warning("Resting-order SL/TP sync failed for %s: %s", symbol, e)

        try:
            position = self.exchange.get_open_position(symbol)
            if not position:
                return True
            self.exchange.set_position_sl(symbol, sl_price)
        except Exception as e:
            log.error("Failed to set SL %s on %s: %s", sl_price, symbol, e)
            self._alert(f"sl_fail:{symbol}",
                        f"⚠️ Could not set SL {sl_price} on {symbol}: {e}\n"
                        f"The position may be UNPROTECTED — check Bitunix. Bot will keep retrying.")
            return False
        # After the SL so the TP/SL orders set_position_sl created get the TP too.
        self._sync_position_tp(symbol, state)
        return True

    def tp_mismatch(self, symbol: str) -> Optional[str]:
        """Read Bitunix back and describe how its TP differs from state, or None if it matches."""
        state = self.db.get(symbol)
        if not state:
            return None
        want = self._target_tp(symbol, state)
        if want is None:
            return None
        pos = self.exchange.get_open_position(symbol)
        if pos:
            if pos["take_profit"] > 0 and all(self._same_price(p, want) for p in pos["tp_prices"]):
                return None
            if not pos["tp_prices"]:
                return "the position has no TP on Bitunix"
            return f"position TP on Bitunix is {pos['tp_prices']}" + \
                   ("" if pos["take_profit"] > 0 else " (not covering the full size)")
        ours = {state.get("entry_order_id"), state.get("dca_order_id")} - {None, ""}
        bad = [o for o in self.exchange.get_open_orders(symbol)
               if o["order_id"] in ours and not self._same_price(o["take_profit"], want)]
        if bad:
            return "resting order TP on Bitunix is " + ", ".join(str(o["take_profit"] or "none") for o in bad)
        return None

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

    # ---------- manual TP detection (user places TPs on Bitunix UI) ----------

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
            self.notify(f"⚠️ Failed to move SL to entry ({new_sl}) for {symbol} — check Bitunix.")
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
        # Bitunix has no trailing-stop API: update_trailing_stops() does the
        # trailing. This only announces it; the peak is tracked from activation.
        r_mult = state.get("trailing_r_mult", "?")
        log.info("Trailing stop armed for %s: %.2f USDT (%sR), activation=%s",
                 symbol, distance, r_mult, activation)
        self.notify(f"↗️ Trailing armed on {symbol}: −${distance:,.0f} ({r_mult}R), "
                    f"starts once price reaches {activation:g}")
        return True

    def update_trailing_stops(self):
        """
        Bot-side trailing stop. Once mark price reaches entry ± distance the
        peak is tracked and the SL ratcheted to peak ∓ distance — never
        loosened. Only works while the bot is running; the last SL it set
        stays on Bitunix if it goes offline.
        """
        states = [s for s in (self.db.get(sym) for sym in self.db.all_active())
                  if s and s.get("position_opened") and s.get("breakeven_moved")
                  and s.get("trailing_distance") and (s.get("entry_price") or 0) > 0]
        if not states:
            return
        marks = self.exchange.get_mark_prices([s["symbol"] for s in states])
        for state in states:
            mark = marks.get(state["symbol"])
            if not mark:
                continue
            try:
                self._trail_symbol(state, mark)
            except Exception as e:
                log.warning("Trailing update failed for %s: %s", state["symbol"], e)

    def _trail_symbol(self, state: dict, mark: float):
        symbol = state["symbol"]
        is_long = state["position"] == "LONG"
        distance = float(state["trailing_distance"])
        entry = state["entry_price"]
        peak = state.get("trailing_peak")
        if peak is None:
            activation = entry + distance if is_long else entry - distance
            if (is_long and mark < activation) or (not is_long and mark > activation):
                return
            peak = mark
            self.notify(f"↗️ Trailing engaged on {symbol} at {mark:g}")
        else:
            peak = max(peak, mark) if is_long else min(peak, mark)
        if peak != state.get("trailing_peak"):
            self.db.upsert(symbol, trailing_peak=peak)

        new_sl = self.exchange.round_price(symbol, peak - distance if is_long else peak + distance)
        current = state.get("sl_price") or 0
        tighter = new_sl > current if is_long else (current <= 0 or new_sl < current)
        if not tighter:
            return
        self.db.upsert(symbol, sl_price=new_sl)
        if self.sync_protective_orders(symbol):
            log.info("Trailing SL %s: %s -> %s (peak %s)", symbol, current, new_sl, peak)

    def classify_exit(self, symbol: str, fill_price: float) -> str:
        """Label a closing market fill as TP or SL by which level it landed nearest."""
        state = self.db.get(symbol)
        if not state or not fill_price:
            return "SL"
        trailing = bool(state.get("trailing_distance") and state.get("breakeven_moved"))
        sl = state.get("sl_price") or 0
        tps = self.db.loads(state.get("tp_prices"))
        if tps and sl > 0 and min(abs(fill_price - t) for t in tps) < abs(fill_price - sl):
            return "TP"
        return "Trailing SL" if trailing else "SL"

    def handle_sl_fill(self, symbol: str, source: str = "SL"):
        with self._lock:
            state = self.db.get(symbol)
            if not state:
                return
            self.db.delete(symbol)

        try:
            self.exchange.cancel_all(symbol)
            qty = self.exchange.close_position_market(symbol)
            if qty > 0:
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
            position = self.exchange.get_open_position(symbol)
            if position:
                break
            time.sleep(0.5)
        if not position:
            log.warning("%s: fill reported but no position visible yet — watchdog will re-check", symbol)
            return
        if not state.get("position_opened"):
            self.db.upsert(symbol, position_opened=1)
        avg_price = position["avg_price"]
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
                mark = self.exchange.get_mark_price(symbol)
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
        Periodic check of every active trade against Bitunix. Catches anything
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
        pos = self.exchange.get_open_position(symbol)

        if pos:
            if not state.get("position_opened"):
                log.warning("Watchdog: %s has a position but its fill was never processed — handling now", symbol)
                self.handle_entry_or_dca_fill(symbol)
                return
            if not self._check_sl(symbol, state, pos):
                self._check_tp(symbol, state, pos)
            return

        if state.get("position_opened"):
            log.warning("Watchdog: %s position is gone but the close event was missed — cleaning up", symbol)
            self.handle_sl_fill(symbol, "Position closed")
            return

        # Not filled yet: is the entry still resting on Bitunix?
        if time.time() - (state.get("created_at") or 0) < NEW_TRADE_GRACE_SECONDS:
            return
        open_orders = self.exchange.get_open_orders(symbol)
        entry_id = state.get("entry_order_id")
        if entry_id:
            resting = any(o["order_id"] == entry_id for o in open_orders)
        else:
            resting = any(not o["reduce_only"] for o in open_orders)
        if resting:
            return
        if self.exchange.get_open_position(symbol):
            return  # filled between the two calls — next pass handles it
        log.warning("Watchdog: %s entry order is no longer on Bitunix and no position exists — cleaning up", symbol)
        try:
            self.exchange.cancel_all(symbol)
        except Exception as e:
            log.warning("cancel_all failed for %s: %s", symbol, e)
        self.db.delete(symbol)
        self.notify(f"🗑️ {symbol}: entry order is no longer on Bitunix (cancelled or rejected) — trade removed.")

    def _check_sl(self, symbol: str, state: dict, pos: dict) -> bool:
        """Watchdog SL check. Returns True if it resynced (which also resyncs the TP)."""
        sl = state.get("sl_price") or 0
        if pos["stop_loss"] > 0:
            if sl <= 0:
                return False
            want = self.exchange.round_price(symbol, sl)
            if all(self._same_price(p, want) for p in pos["sl_prices"]):
                return False
            log.warning("Watchdog: %s SL on Bitunix %s != state %s — resyncing", symbol, pos["sl_prices"], want)
            if self.sync_protective_orders(symbol):
                self._alert(f"sl_drift:{symbol}", f"🛡️ {symbol} SL on Bitunix differed from the bot's — reset to {want}.")
            return True
        if sl > 0:
            log.warning("Watchdog: %s position has NO stop loss on Bitunix — re-applying %s", symbol, sl)
            if self.sync_protective_orders(symbol):
                self._alert(f"sl_fixed:{symbol}", f"🛡️ {symbol} had no SL on Bitunix — re-applied SL at {sl}.")
            return True
        self._alert(f"no_sl:{symbol}",
                    f"⚠️ {symbol} has NO stop loss and none is on record. "
                    f"Set one with /sl {symbol} <price>")
        return False

    def _check_tp(self, symbol: str, state: dict, pos: dict):
        """Watchdog TP check: put TP1 from state back if Bitunix lost or changed it."""
        want = self._target_tp(symbol, state)
        if want is None:
            return
        if pos["take_profit"] > 0 and all(self._same_price(p, want) for p in pos["tp_prices"]):
            return
        log.warning("Watchdog: %s TP on Bitunix %s (covers all=%s) != state %s — resyncing",
                    symbol, pos["tp_prices"], pos["take_profit"] > 0, want)
        if self._sync_position_tp(symbol, state):
            self._alert(f"tp_fixed:{symbol}", f"🎯 {symbol} TP on Bitunix was missing or different — set to {want}.")

    # ---------- trailing stop ----------

    def cancel_trailing(self, symbol: str) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            return f"No active position for {symbol}."
        if not state.get("trailing_distance"):
            return f"No trailing stop active on {symbol}."

        original_sl = state.get("original_sl_price")
        self.db.upsert(symbol, trailing_distance=None, trailing_r_mult=None, trailing_peak=None)
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
            risk_amount = self.exchange.get_equity_usdt() * (config.RISK_PERCENT / 100)

        position = self.exchange.get_open_position(symbol)
        if position:
            size = position["size"]
            avg = position["avg_price"]
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
        qty = self.exchange.round_qty(symbol, remaining / abs(dca_price - sl))
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
            mark = self.exchange.get_mark_price(symbol)
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

    def _validate_tps(self, symbol: str, position: str, prices: List[float]):
        """Bitunix rejects a TP on the wrong side of mark — catch it before the user confirms."""
        if any(p <= 0 for p in prices):
            raise ValueError(f"Invalid TP in {prices}.")
        try:
            mark = self.exchange.get_mark_price(symbol)
        except Exception:
            return
        for p in prices:
            if position == "LONG" and p <= mark:
                raise ValueError(f"For LONG, TP {p} must be above current mark price {mark}.")
            if position == "SHORT" and p >= mark:
                raise ValueError(f"For SHORT, TP {p} must be below current mark price {mark}.")

    def stage_modify_tp(self, symbol: str, new_prices: List[float]) -> str:
        state = self.db.get(symbol)
        if not state or state["status"] != "active":
            if symbol in self.pending:
                signal = self.pending[symbol]["signal"]
                self._validate_tps(symbol, signal.position, new_prices)
                old = ", ".join(str(t) for t in signal.tps)
                prompt = f"Modify TPs for {symbol}?\n  Current: {old}\n  New: {', '.join(str(t) for t in new_prices)}"
                with self._lock:
                    self.pending_mods[symbol] = {"type": "tp", "params": {"new_prices": new_prices}, "chat_id": None, "message_id": None}
                return prompt
            raise ValueError(f"No active position or pending trade for {symbol}.")

        self._validate_tps(symbol, state["position"], new_prices)
        old = ", ".join(str(t) for t in self.db.loads(state["tp_prices"])) or "none"
        prompt = f"Modify TPs for {symbol}?\n  Current: {old}\n  New: {', '.join(str(t) for t in new_prices)}"
        if len(new_prices) > 1:
            prompt += f"\n  (Bitunix gets TP1 {new_prices[0]} for the full position)"
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
            with self._lock:
                pend = self.pending.get(symbol)
                if pend and mod_type == "tp":
                    pend["signal"].tps = list(params["new_prices"])
            if pend and mod_type == "tp":
                return (f"✅ TPs for pending {symbol} set to {', '.join(str(t) for t in params['new_prices'])} "
                        f"— tap Confirm on the trade card to place it.")
            return f"No active position for {symbol}."

        if mod_type == "sl":
            if state.get("trailing_distance"):
                self.db.upsert(symbol, trailing_distance=None, trailing_r_mult=None, trailing_peak=None)
            self.db.upsert(symbol, sl_price=params["new_price"])
            if self.sync_protective_orders(symbol):
                return f"✅ SL updated for {symbol} to {params['new_price']}."
            return (f"⚠️ SL for {symbol} saved as {params['new_price']} but Bitunix did NOT accept it — "
                    f"the old SL may still be active. Check Bitunix.")

        elif mod_type == "tp":
            prices = ", ".join(str(t) for t in params["new_prices"])
            self.db.upsert(symbol, tp_prices=self.db.dumps(params["new_prices"]))
            self.sync_protective_orders(symbol)
            try:
                mismatch = self.tp_mismatch(symbol)
            except Exception as e:
                mismatch = f"could not read it back ({e})"
            if mismatch:
                return (f"⚠️ TPs for {symbol} saved as {prices} but Bitunix did NOT take it — {mismatch}. "
                        f"Bot will keep retrying; check Bitunix.")
            return f"✅ TP {params['new_prices'][0]} set on Bitunix for {symbol} (saved: {prices})."

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
                self.exchange.cancel_order(symbol, state["dca_order_id"])
            self.db.upsert(symbol, dca_order_id=None, dca_price=None)
            if new_price is not None and dca_qty > 0:
                side = "BUY" if state["position"] == "LONG" else "SELL"
                dca_price = self.exchange.round_price(symbol, new_price)
                dca_order = self.exchange.place_limit_order(symbol, side, dca_qty, dca_price,
                                                         stop_loss=state["sl_price"],
                                                         take_profit=self._target_tp(symbol, state))
                self.db.upsert(symbol, dca_order_id=dca_order, dca_price=dca_price)
                return f"✅ DCA placed for {symbol} at {dca_price} (qty ~{dca_qty}) with SL {state['sl_price']}."
            return f"✅ DCA removed for {symbol}."

        elif mod_type == "trail":
            distance = params["distance"]
            r_mult = params["r_mult"]
            self.db.upsert(symbol, trailing_distance=distance, trailing_r_mult=r_mult, trailing_peak=None)
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
        wallet = self.exchange.get_wallet_info()
        lines = [f"📊 Equity: ${wallet['equity']:,.2f} | Available: ${wallet['available']:,.2f}"]

        symbols = [symbol] if symbol else self.db.all_active()
        if not symbols:
            lines.append("\nNo active positions.")
            return "\n".join(lines)

        for sym in symbols:
            state = self.db.get(sym)
            pos = self.exchange.get_open_position(sym)
            if not state or not pos:
                lines.append(f"\n{sym}: no active position")
                continue
            side = state["position"]
            entry = state["entry_price"] or pos["avg_price"]
            try:
                mark = self.exchange.get_mark_price(sym)
            except Exception:
                mark = 0.0
            qty = pos["size"]
            leverage = pos["leverage"]
            pnl = pos["unrealized_pnl"]
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
