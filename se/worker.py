"""
The Sweep & Engulf background worker.

It runs in a daemon thread inside the existing bot's process (see start()).
Every TICK_SECONDS it:
  1. tops up each symbol's closed 4h candles (UTC-aligned) from Bitunix,
  2. checks open trades for exits (paper: candles touch SL/TP; live: the
     position is gone from Bitunix -> record the close from position history),
  3. looks for a signal on each symbol's newest closed candle and enters,
  4. every 5 minutes (live) reconciles saved state with Bitunix,
  5. sends the daily summary after 00:05 UTC.

Scheduling is by the UTC wall clock on every tick, so after the phone wakes
from sleep the worker simply sees where the clock is: a candle that closed
while it slept is still processed, but its signal is skipped if the entry
would be more than late_entry_minutes late.

Nothing here may raise out of the thread: each step catches its own errors,
and start() wraps the whole loop in a restart loop with backoff.
"""
import logging
import logging.handlers
import os
import threading
import time
from decimal import Decimal
from typing import Callable, List, Optional

from se import config as se_config
from se import market
from se import strategy as s
from se.store import Store

log = logging.getLogger("se")

TICK_SECONDS = 20
LIVE_CHECK_SECONDS = 60          # how often live positions are checked for a close
RECONCILE_SECONDS = 300
PROTECT_WAIT_SECONDS = 10        # SL/TP must be visible on Bitunix this soon after the fill
TIME_SYNC_SECONDS = 1800
INSTRUMENT_TTL_SECONDS = 3600
ERROR_COOLDOWN_SECONDS = 900     # at most one Telegram error per type per 15 minutes
HISTORY_CANDLES = 1000
KEEP_CANDLES = 1500
SUMMARY_AFTER_MS = 5 * 60 * 1000  # 00:05 UTC
DAY_MS = 24 * 3600 * 1000


def setup_logging():
    """se.log: rotates at ~5 MB, keeps 3 old files. Not mixed into bot.log."""
    if any(isinstance(h, logging.handlers.RotatingFileHandler) for h in log.handlers):
        return
    os.makedirs(se_config.LOG_DIR, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(se_config.LOG_PATH, maxBytes=5 * 1024 * 1024,
                                                   backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


def _fmt_time(ms: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ms / 1000))


def _money(x: Decimal) -> str:
    return f"{'-' if x < 0 else '+'}${abs(x):,.2f}"


class SEWorker:
    def __init__(self, cfg: se_config.SEConfig, store: Store, notify: Callable[[str], None],
                 clock: Callable[[], float] = time.time, exchange_factory=None, session=None):
        self.cfg = cfg
        self.store = store
        self._send = notify
        self.clock = clock
        self.session = session or market.new_session()
        self._exchange_factory = exchange_factory
        self._exchange = None
        self.candles = {}          # symbol -> [Candle] (closed only, oldest first)
        self.instruments = {}      # symbol -> (info dict, fetched_at)
        self.lock = threading.RLock()
        self._last_error = {}
        self._last_live_check = 0.0
        self._last_reconcile = 0.0
        self._last_time_sync = 0.0
        self._started = False

    # ---------- basics ----------

    def now_ms(self) -> int:
        return int(self.clock() * 1000)

    def notify(self, text: str):
        log.info("TG: %s", text.replace("\n", " | "))
        try:
            self._send(f"[S&E] {text}")
        except Exception as e:
            log.warning("Telegram send failed: %s", e)

    def error(self, kind: str, text: str):
        """Log every error; tell Telegram at most once per kind per 15 minutes."""
        log.error("%s: %s", kind, text)
        now = time.time()
        if now - self._last_error.get(kind, 0) >= ERROR_COOLDOWN_SECONDS:
            self._last_error[kind] = now
            self.notify(f"⚠️ {text}")

    @property
    def mode(self) -> str:
        return self.store.get("mode", self.cfg.mode)

    @property
    def paused(self) -> bool:
        return bool(self.store.get("paused", False))

    def _ex(self):
        """The private Bitunix client, created on first use (live mode only)."""
        if self._exchange is None:
            if self._exchange_factory:
                self._exchange = self._exchange_factory()
            else:
                from se.exchange import SEExchange
                self._exchange = SEExchange(os.getenv("BITUNIX_API_KEY", ""), os.getenv("BITUNIX_API_SECRET", ""))
        return self._exchange

    def _instrument(self, symbol: str) -> dict:
        info, ts = self.instruments.get(symbol, (None, 0))
        if info is None or time.time() - ts > INSTRUMENT_TTL_SECONDS:
            try:
                info = market.fetch_instrument(self.session, symbol)
                self.instruments[symbol] = (info, time.time())
            except Exception:
                if info is None:
                    raise
                log.warning("Using cached %s precision (refresh failed)", symbol)
        return info

    def _px(self, symbol: str, x) -> str:
        """A price at the symbol's precision, for messages."""
        if x is None:
            return "-"
        try:
            step = self._instrument(symbol)["price_step"]
            return str(s.round_to_step(Decimal(x), step).quantize(step))
        except Exception:
            return str(x)

    def _last_price(self, symbol: str) -> Decimal:
        return market.fetch_last_price(self.session, symbol)

    def _loss_state(self) -> s.LossState:
        d = self.store.get("loss_state") or {}
        return s.LossState(count=d.get("count", 0), day=d.get("day"), blocked_day=d.get("blocked_day"))

    def _save_loss_state(self, st: s.LossState):
        self.store.set("loss_state", {"count": st.count, "day": st.day, "blocked_day": st.blocked_day})

    # ---------- equity ----------

    def equity(self, mode: Optional[str] = None, prices: Optional[dict] = None):
        """
        (equity, open entry value, unrealized P&L) for one mode's trades.
        Equity = budget + realized net P&L of closed trades + unrealized P&L of
        open ones. Paper and live keep separate books, so paper results never
        size a live trade.
        """
        mode = mode or self.mode
        prices = dict(prices or {})
        realized = sum((t["pnl"] for t in self.store.closed_trades()
                        if t["mode"] == mode and t["pnl"] is not None), Decimal(0))
        open_value = Decimal(0)
        upnl = Decimal(0)
        for t in self.store.active_trades():
            if t["mode"] != mode or not t["qty"]:
                continue
            entry = t["entry_price"] or t["signal_close"]
            open_value += entry * t["qty"]
            if t["entry_price"]:
                if t["symbol"] not in prices:
                    prices[t["symbol"]] = self._last_price(t["symbol"])
                upnl += (prices[t["symbol"]] - entry) * t["qty"]
        return self.cfg.budget_usdt + realized + upnl, open_value, upnl

    # ---------- main loop ----------

    def startup(self):
        setup_logging()
        cfg = self.cfg
        if self.store.get("mode") is None:
            self.store.set("mode", cfg.mode)
        if self.mode == "live" and not cfg.live_allowed:
            self.store.set("mode", "paper")
            self.notify("⚠️ Saved mode was live, but SE_LIVE_CONFIRM in .env is not I_ACCEPT_THE_RISK — "
                        "running in PAPER mode.")
        if self.store.get("last_summary_day") is None:
            self.store.set("last_summary_day", s.utc_day(self.now_ms()))
        log.info("Starting: mode=%s %s", self.mode, cfg.summary())

        for sym in cfg.symbols:
            try:
                self._instrument(sym)
                self.candles[sym] = market.fetch_history(self.session, sym, count=HISTORY_CANDLES,
                                                         now_ms=self.now_ms())
                n = len(self.candles[sym])
                log.info("%s: loaded %d closed 4h candles (last %s)", sym, n,
                         _fmt_time(self.candles[sym][-1].t) if n else "-")
                if n < cfg.ema_length + 50:
                    self.notify(f"⚠️ {sym}: only {n} closed 4h candles from Bitunix — EMA{cfg.ema_length} "
                                f"may not have settled.")
            except Exception as e:
                self.error(f"load:{sym}", f"{sym}: could not load candle history: {e}")

        if self._has_live_work():
            self._sync_time(force=True)
            self.reconcile(startup=True)

        equity, open_value, _ = self.equity()
        active = self.store.active_trades()
        self.notify(
            f"🟢 Worker started — mode {self.mode.upper()}"
            + (" (PAUSED)" if self.paused else "")
            + (" (STOP file present: no new entries)" if os.path.exists(se_config.STOP_FILE) else "")
            + f"\nBudget ${cfg.budget_usdt} | equity ${equity:,.2f} | open trades {len(active)}"
            + f"\n{cfg.summary()}")
        self._started = True

    def run(self, stop: threading.Event):
        if not self._started:
            self.startup()
        while not stop.is_set():
            try:
                with self.lock:
                    self.tick()
            except Exception as e:
                log.exception("tick failed")
                self.error("tick", f"Worker loop error: {e}")
            stop.wait(TICK_SECONDS)

    def tick(self):
        now = self.now_ms()
        for sym in self.cfg.symbols:
            try:
                self._update_candles(sym, now)
            except Exception as e:
                self.error(f"candles:{sym}", f"{sym}: candle update failed: {e}")
        self._manage_open_trades(now)
        self._maybe_resume_message(now)
        for sym in self.cfg.symbols:
            try:
                self._process_symbol(sym, now)
            except Exception as e:
                log.exception("signal processing failed for %s", sym)
                self.error(f"signal:{sym}", f"{sym}: signal processing failed: {e}")
        if self._has_live_work() and time.time() - self._last_reconcile >= RECONCILE_SECONDS:
            self._sync_time()
            self.reconcile()
        try:
            self._maybe_daily_summary(now)
        except Exception as e:
            self.error("summary", f"Daily summary failed: {e}")

    def _has_live_work(self) -> bool:
        return self.mode == "live" or any(t["mode"] == "live" for t in self.store.active_trades())

    def _sync_time(self, force: bool = False):
        if not force and time.time() - self._last_time_sync < TIME_SYNC_SECONDS:
            return
        try:
            drift = self._ex().sync_time()
            self._last_time_sync = time.time()
            if drift is not None:
                log.info("Clock drift vs Bitunix: %+.1fs (offset applied to signed requests)", drift)
                if abs(drift) > 30:
                    self.error("clock", f"Device clock is {drift:+.0f}s off Bitunix time. Requests are "
                                        f"corrected, but candle timing may be off — sync the phone clock.")
        except Exception as e:
            self.error("timesync", f"Could not measure Bitunix server time: {e}")

    # ---------- candles ----------

    def _update_candles(self, sym: str, now: int):
        expected = s.last_closed_open_time(now)
        cs = self.candles.get(sym)
        if not cs:
            self.candles[sym] = market.fetch_history(self.session, sym, count=HISTORY_CANDLES, now_ms=now)
            return
        if cs[-1].t >= expected:
            return
        missing = (expected - cs[-1].t) // s.CANDLE_MS
        if missing > 150:  # slept a long time: reload the whole window
            self.candles[sym] = market.fetch_history(self.session, sym, count=HISTORY_CANDLES, now_ms=now)
            return
        page = market.fetch_klines(self.session, sym, limit=int(missing) + 3)
        new = [c for c in page if c.t > cs[-1].t and s.is_closed(c.t, now)]
        if new and new[0].t != cs[-1].t + s.CANDLE_MS:
            log.warning("%s: gap in candles (%s -> %s) — reloading history", sym,
                        _fmt_time(cs[-1].t), _fmt_time(new[0].t))
            self.candles[sym] = market.fetch_history(self.session, sym, count=HISTORY_CANDLES, now_ms=now)
            return
        if new:
            cs.extend(new)
            del cs[:-KEEP_CANDLES]
            log.info("%s: new closed candle %s O%s H%s L%s C%s", sym, _fmt_time(new[-1].t),
                     new[-1].o, new[-1].h, new[-1].l, new[-1].c)

    # ---------- signals and entries ----------

    def _process_symbol(self, sym: str, now: int):
        cs = self.candles.get(sym)
        if not cs:
            return
        last = cs[-1]
        key = f"last_candle:{sym}"
        processed = self.store.get(key)
        if processed is not None and last.t <= processed:
            return
        cfg = self.cfg
        inst = self._instrument(sym)
        ema_vals = s.ema([c.c for c in cs], cfg.ema_length)
        atr_vals = s.atr(cs, cfg.atr_length)
        sig = s.check_signal(cs, len(cs) - 1, ema_vals, atr_vals, cfg, inst["price_step"])
        if sig is None:
            self.store.set(key, last.t)
            return

        log.info("%s signal on %s: close %s ema %s atr %s sl %s tp %s", sym, _fmt_time(last.t),
                 sig.close, sig.ema, sig.atr, sig.sl, sig.tp)
        client_id = f"se_{sym}_{last.t}"
        late_min = (now - (last.t + s.CANDLE_MS)) / 60000
        if self.store.trade_by_client_id(client_id):
            pass  # already acted on (e.g. restart right after the entry)
        elif late_min > cfg.late_entry_minutes:
            self._skip(sym, sig, f"too late ({late_min:.0f} min after the candle closed; limit "
                                 f"{cfg.late_entry_minutes} min)")
        elif self.store.active_trade(sym):
            log.info("%s: signal ignored — the worker already has a trade on it", sym)
        elif self.paused:
            self._skip(sym, sig, "worker paused (/se_resume to resume)")
        elif os.path.exists(se_config.STOP_FILE):
            self._skip(sym, sig, "STOP file present in the bot folder")
        elif s.is_blocked(self._loss_state(), now, cfg.daily_loss_limit):
            self._skip(sym, sig, f"daily loss limit ({cfg.daily_loss_limit} losses in a row today)")
        else:
            self._enter(sym, sig, inst, now, client_id)
        # Only reached when nothing raised: a network error retries on the next tick
        # (the late rule then stops it after late_entry_minutes).
        self.store.set(key, last.t)

    def _signal_text(self, sym: str, sig: s.Signal) -> str:
        ema_txt = f"EMA{self.cfg.ema_length} {self._px(sym, sig.ema)}" if sig.ema is not None else "EMA -"
        atr_txt = f"ATR{self.cfg.atr_length} {self._px(sym, sig.atr)}" if sig.atr is not None else "ATR -"
        return (f"{sym} 4h candle {_fmt_time(sig.candle_time)}\n"
                f"Close {self._px(sym, sig.close)} | {ema_txt} | {atr_txt}\n"
                f"SL {self._px(sym, sig.sl)} | TP {self._px(sym, sig.tp)} ({self.cfg.rr}R)")

    def _skip(self, sym: str, sig: s.Signal, reason: str):
        log.info("%s signal skipped: %s", sym, reason)
        self.notify(f"⏭️ Signal skipped — {reason}\n{self._signal_text(sym, sig)}")

    def _enter(self, sym: str, sig: s.Signal, inst: dict, now: int, client_id: str):
        cfg = self.cfg
        mode = self.mode
        price = self._last_price(sym)
        if not sig.sl < price < sig.tp:
            self._skip(sym, sig, f"price {self._px(sym, price)} is already outside SL/TP")
            return
        equity, open_value, _ = self.equity(mode, {sym: price})
        sz = s.size_position(equity, cfg.budget_usdt, open_value, cfg.risk_pct, cfg.compound,
                             sig.risk_dist, price, inst["qty_step"], inst["min_qty"], cfg.min_order_usdt)
        if sz.skip_reason:
            self._skip(sym, sig, sz.skip_reason)
            return
        value = sz.qty * price
        # Bug-catcher caps (the spot cap above already keeps both well under these).
        cap = equity * cfg.max_position_equity_mult
        if value > cap or open_value + value > cap:
            self.error(f"cap:{sym}", f"{sym}: trade REFUSED — position ${value:,.2f} (open ${open_value:,.2f}) "
                                     f"would exceed the cap ${cap:,.2f} ({cfg.max_position_equity_mult}x equity "
                                     f"${equity:,.2f}). This points to a bug or a misread balance.")
            return

        sized = " — sized down (not enough free cash for full risk)" if sz.sized_down else ""
        risk_usd = sz.qty * sig.risk_dist
        sig_msg = (f"📈 Signal ({mode.upper()})\n{self._signal_text(sym, sig)}\n"
                   f"Risk ${risk_usd:,.2f} (target ${sz.risk_amount:,.2f}) | Qty {sz.qty} "
                   f"(~${value:,.2f}){sized}")

        if mode == "paper":
            entry_fee = s.fee(price, sz.qty, cfg.fee_pct)
            self.store.insert_trade(
                client_id=client_id, symbol=sym, mode="paper", status="open", candle_time=sig.candle_time,
                signal_close=sig.close, ema=sig.ema, atr=sig.atr, sl=sig.sl, tp=sig.tp, risk_dist=sig.risk_dist,
                risk_amount=sz.risk_amount, sized_down=sz.sized_down, qty=sz.qty, entry_price=price,
                entry_time=now, entry_fee=entry_fee)
            self.notify(sig_msg)
            self.notify(f"✅ Entry filled (paper) {sym}: {sz.qty} @ {self._px(sym, price)} = "
                        f"${value:,.2f}, fee ${entry_fee:,.4f}")
            return
        self._enter_live(sym, sig, sz, price, value, client_id, sig_msg)

    # ---------- live entry and protection ----------

    def _pick_leverage(self, sym: str, price: Decimal, sl: Decimal):
        """Leverage and margin mode exactly as /place picks them (LEVERAGE_TIERS, DEFAULT_MARGIN_MODE)."""
        import config as bot_config
        margin = "ISOLATED" if bot_config.DEFAULT_MARGIN_MODE.startswith("ISOLAT") else "CROSS"
        lev = bot_config.leverage_for_price(float(price))
        why = f"auto for price {price}"
        if margin == "ISOLATED":
            # Keep isolated liquidation at least twice the SL distance away.
            sl_distance = float((price - sl) / price)
            lev_cap = max(1, int(1 / (2 * sl_distance))) if sl_distance > 0 else lev
            if lev_cap < lev:
                lev, why = lev_cap, "lowered so isolated liquidation stays well past the SL"
        max_lev = self._ex().max_leverage(sym)
        if max_lev and lev > max_lev:
            lev, why = max_lev, f"Bitunix max {max_lev}x"
        return int(lev), margin, why

    def _enter_live(self, sym, sig, sz, price, value, client_id, sig_msg):
        ex = self._ex()
        foreign = ex.foreign_activity(sym)
        if foreign:
            self._skip(sym, sig, "foreign position/order on Bitunix (not created by S&E): " + "; ".join(foreign))
            return
        lev, margin, why = self._pick_leverage(sym, price, sig.sl)
        available = ex.available_usdt()
        need = value / lev + 2 * s.fee(price, sz.qty, self.cfg.fee_pct)
        if need > available:
            self._skip(sym, sig, f"not enough cash on Bitunix (available ${available:,.2f}, "
                                 f"needs ~${need:,.2f} margin+fees at {lev}x)")
            return
        ex.prepare_symbol(sym, lev, margin)
        tid = self.store.insert_trade(
            client_id=client_id, symbol=sym, mode="live", status="pending", candle_time=sig.candle_time,
            signal_close=sig.close, ema=sig.ema, atr=sig.atr, sl=sig.sl, tp=sig.tp, risk_dist=sig.risk_dist,
            risk_amount=sz.risk_amount, sized_down=sz.sized_down, qty=sz.qty, leverage=lev)
        self.notify(sig_msg + f"\nLeverage {lev}x {margin} ({why})")
        try:
            order_id = ex.place_entry(sym, sz.qty, sig.sl, sig.tp, client_id)
        except Exception as e:
            existing = None
            try:
                existing = ex.find_order(sym, client_id)
            except Exception:
                pass
            if not existing:
                self.store.update_trade(tid, status="failed", note=f"entry failed: {e}")
                self.error(f"entry:{sym}", f"{sym}: entry order FAILED: {e}")
                return
            order_id = existing["order_id"]
        self.store.update_trade(tid, order_id=order_id)
        self._confirm_fill_and_protect(tid)

    def _confirm_fill_and_protect(self, tid: int):
        """After the market entry: wait up to 10s for the position with its SL+TP; fix or close it."""
        ex = self._ex()
        t = self.store.trade(tid)
        sym = t["symbol"]
        pos = None
        deadline = time.time() + PROTECT_WAIT_SECONDS
        while True:
            pos = ex.position(sym)
            if pos and pos["stop_loss"] > 0 and pos["take_profit"] > 0:
                break
            if time.time() >= deadline:
                break
            time.sleep(1.5)
        if not pos:
            order = ex.find_order(sym, t["client_id"])
            status = (order or {}).get("status", "")
            if order is None or "CANCEL" in status or "REJECT" in status:
                self.store.update_trade(tid, status="failed", note=f"no fill (order status {status or 'not found'})")
                self.error(f"nofill:{sym}", f"{sym}: market entry did not fill (status {status or 'not found'}).")
            else:
                log.warning("%s: order %s status %s but no position yet — reconcile will re-check",
                            sym, t["order_id"], status)
            return

        fills = None
        try:
            fills = ex.fill_details(sym, t["order_id"])
        except Exception as e:
            log.warning("%s: fill details unavailable (%s) — using the position", sym, e)
        entry = fills["price"] if fills else Decimal(str(pos["avg_price"]))
        qty = fills["qty"] if fills else Decimal(str(pos["size"]))
        entry_fee = fills["fee"] if fills else s.fee(entry, qty, self.cfg.fee_pct)
        self.store.update_trade(tid, status="open", entry_price=entry, qty=qty, entry_fee=entry_fee,
                                entry_time=self.now_ms(), position_id=pos["position_id"])
        self.notify(f"✅ Entry filled (LIVE) {sym}: {qty} @ {self._px(sym, entry)} = ${entry * qty:,.2f}, "
                    f"fee ${entry_fee:,.4f}")
        if pos["stop_loss"] > 0 and pos["take_profit"] > 0:
            self.store.update_trade(tid, protected=1)
            log.info("%s: SL %s / TP %s confirmed on Bitunix", sym, pos["sl_prices"], pos["tp_prices"])
            return
        self._protect(tid)

    def _protect(self, tid: int) -> bool:
        """Put the SL+TP on the position; if that fails after retries, close at market and alert."""
        ex = self._ex()
        t = self.store.trade(tid)
        sym = t["symbol"]
        last_err = None
        for attempt in range(3):
            try:
                ex.ensure_tpsl(sym, t["sl"], t["tp"])
                pos = ex.position(sym)
                if pos is None:
                    return True  # closed meanwhile; the close check records it
                if pos["stop_loss"] > 0 and pos["take_profit"] > 0:
                    self.store.update_trade(tid, protected=1)
                    self.notify(f"🛡️ {sym}: SL {self._px(sym, t['sl'])} / TP {self._px(sym, t['tp'])} "
                                f"were missing on Bitunix — placed and confirmed.")
                    return True
                last_err = f"SL/TP still not showing (SL {pos['sl_prices']}, TP {pos['tp_prices']})"
            except Exception as e:
                last_err = str(e)
            time.sleep(2 * (attempt + 1))
        try:
            ex.close_position(sym)
            self.store.update_trade(tid, status="closing", exit_reason="UNPROTECTED")
            self.notify(f"🚨 {sym}: could not place SL/TP ({last_err}). Position CLOSED at market.")
        except Exception as e:
            self.error(f"unprotected:{sym}", f"🚨 {sym}: NO SL/TP on Bitunix ({last_err}) and closing FAILED: "
                                             f"{e}. Close it in the Bitunix app NOW.")
        return False

    # ---------- exits ----------

    def _manage_open_trades(self, now: int):
        closes = []
        live_check_due = time.time() - self._last_live_check >= LIVE_CHECK_SECONDS
        raw_by_symbol = None
        for t in self.store.active_trades():
            try:
                if t["mode"] == "paper":
                    c = self._paper_check(t)
                    if c:
                        closes.append(c)
                elif live_check_due and t["status"] in ("open", "closing"):
                    if raw_by_symbol is None:
                        raw_by_symbol = {p.get("symbol"): p for p in self._ex().raw_positions()}
                    c = self._live_check(t, raw_by_symbol)
                    if c:
                        closes.append(c)
            except Exception as e:
                log.exception("exit check failed for %s", t["symbol"])
                self.error(f"exit:{t['symbol']}", f"{t['symbol']}: exit check failed: {e}")
        if live_check_due and raw_by_symbol is not None:
            self._last_live_check = time.time()
        # Apply closes in the order they happened (the daily loss limit counts in close order).
        for c in sorted(closes, key=lambda c: c["exit_time"]):
            self._finish_close(**c)

    def _paper_check(self, t: dict) -> Optional[dict]:
        cs = self.candles.get(t["symbol"]) or []
        entry_candle = t["entry_time"] - t["entry_time"] % s.CANDLE_MS
        checked = t["paper_checked_until"]
        last_seen = checked
        for cd in cs:
            if cd.t < entry_candle or (checked is not None and cd.t <= checked):
                continue
            last_seen = cd.t
            hit = s.paper_exit(cd, t["sl"], t["tp"],
                               open_price=t["entry_price"] if cd.t == entry_candle else None)
            if hit:
                reason, price = hit
                exit_fee = s.fee(price, t["qty"], self.cfg.fee_pct)
                pnl = (price - t["entry_price"]) * t["qty"] - t["entry_fee"] - exit_fee
                return dict(t=t, exit_price=price, exit_time=max(cd.t, t["entry_time"]), reason=reason,
                            pnl=pnl, fees=t["entry_fee"] + exit_fee, funding=Decimal(0))
        if last_seen != checked:
            self.store.update_trade(t["id"], paper_checked_until=last_seen)
        return None

    def _live_check(self, t: dict, raw_by_symbol: dict) -> Optional[dict]:
        sym = t["symbol"]
        raw = raw_by_symbol.get(sym)
        if raw and (not t["position_id"] or str(raw.get("positionId")) == str(t["position_id"])):
            if not t["protected"]:
                pos = self._ex().position(sym)
                if pos and pos["stop_loss"] > 0 and pos["take_profit"] > 0:
                    self.store.update_trade(t["id"], protected=1)
                else:
                    self._protect(t["id"])
            return None
        # Gone (or replaced by a different position): confirm, then read the closed position.
        time.sleep(2)
        again = self._ex().position(sym)
        if again and (not t["position_id"] or again["position_id"] == t["position_id"]):
            return None  # a brief gap in Bitunix's answer, not a close
        hp = self._ex().history_position(sym, t["position_id"])
        if hp is None:
            self.error(f"history:{sym}", f"{sym}: position is closed but Bitunix history doesn't list it "
                                         f"yet — will retry.")
            return None
        log.info("%s closed position history: %s", sym, hp["raw"])
        fees = abs(hp["fee"])
        funding = hp["funding"]
        # realizedPNL excludes fees and funding (per the docs). Funding is taken as
        # signed: positive = received. See the open questions in README.
        pnl = hp["realized_pnl"] - fees + funding
        price = hp["close_price"]
        if t["exit_reason"]:
            reason = t["exit_reason"]
        else:
            reason = "TP" if abs(price - t["tp"]) < abs(price - t["sl"]) else "SL"
        return dict(t=t, exit_price=price, exit_time=hp["close_time"], reason=reason, pnl=pnl,
                    fees=fees, funding=funding)

    def _finish_close(self, t: dict, exit_price: Decimal, exit_time: int, reason: str,
                      pnl: Decimal, fees: Decimal, funding: Decimal):
        sym = t["symbol"]
        r = s.r_multiple(pnl, t["risk_dist"], t["qty"])
        self.store.update_trade(t["id"], status="closed", exit_price=exit_price, exit_time=exit_time,
                                exit_reason=reason, pnl=pnl, fees=fees, funding=funding, r_mult=r)
        st, reached = s.record_close(self._loss_state(), pnl, exit_time, self.cfg.daily_loss_limit)
        self._save_loss_state(st)
        equity, _, _ = self.equity(t["mode"])
        icon = "🎯" if pnl >= 0 else "🛑"
        funding_txt = f", funding {_money(funding)}" if funding else ""
        self.notify(f"{icon} Trade closed ({t['mode'].upper()}) {sym} — {reason} @ {self._px(sym, exit_price)}\n"
                    f"P&L {_money(pnl)} ({r:+.2f}R) after fees ${fees:,.4f}{funding_txt}\n"
                    f"Equity now ${equity:,.2f}")
        if reached:
            self.store.set("resume_notified_for", None)
            self.notify(f"⛔ Daily loss limit reached: {st.count} losing trades in a row. No new trades until "
                        f"00:00 UTC. Open trades keep their TP and SL.")

    def _maybe_resume_message(self, now: int):
        st = self._loss_state()
        if not st.blocked_day or s.utc_day(now) <= st.blocked_day:
            return
        if self.store.get("resume_notified_for") == st.blocked_day:
            return
        self.store.set("resume_notified_for", st.blocked_day)
        self.notify("▶️ New UTC day — daily loss limit cleared, trading resumed.")

    # ---------- reconcile (live) ----------

    def reconcile(self, startup: bool = False):
        """Compare saved live trades with Bitunix: fills, positions and TP/SL. Fix and report."""
        self._last_reconcile = time.time()
        try:
            ex = self._ex()
        except Exception as e:
            self.error("keys", f"Live mode can't reach Bitunix: {e}")
            return
        issues = []
        mine = set()
        for t in self.store.active_trades():
            if t["mode"] != "live":
                continue
            sym = t["symbol"]
            mine.add(sym)
            try:
                if t["status"] == "pending":
                    pos = ex.position(sym)
                    order = ex.find_order(sym, t["client_id"])
                    if pos:
                        issues.append(f"{sym}: entry had filled but wasn't recorded — recorded now")
                        if not t["order_id"] and order:
                            self.store.update_trade(t["id"], order_id=order["order_id"])
                        self._confirm_fill_and_protect(t["id"])
                    elif order is None or "CANCEL" in order["status"] or "REJECT" in order["status"]:
                        self.store.update_trade(t["id"], status="failed", note="entry never filled")
                        issues.append(f"{sym}: entry order never filled — trade dropped")
                    continue
                pos = ex.position(sym)
                if not pos:
                    continue  # the close check records it
                if t["position_id"] and pos["position_id"] != t["position_id"]:
                    issues.append(f"{sym}: Bitunix position id {pos['position_id']} differs from saved "
                                  f"{t['position_id']}")
                if t["qty"] and abs(Decimal(str(pos["size"])) - t["qty"]) > t["qty"] * Decimal("0.0001"):
                    issues.append(f"{sym}: Bitunix size {pos['size']} differs from saved {t['qty']}")
                if not (pos["stop_loss"] > 0 and pos["take_profit"] > 0):
                    issues.append(f"{sym}: SL/TP missing on Bitunix (SL {pos['sl_prices']}, "
                                  f"TP {pos['tp_prices']}) — re-placing")
                    self._protect(t["id"])
            except Exception as e:
                issues.append(f"{sym}: reconcile failed: {e}")
        # Positions on the worker's symbols that it doesn't own: never touched, just reported.
        try:
            for p in ex.raw_positions():
                sym = p.get("symbol")
                if sym in self.cfg.symbols and sym not in mine:
                    key = f"foreign:{sym}"
                    if time.time() - self._last_error.get(key, 0) > 3600:
                        self._last_error[key] = time.time()
                        issues.append(f"{sym}: an open position not created by S&E is on Bitunix — "
                                      f"S&E skips {sym} signals while it's there")
        except Exception as e:
            issues.append(f"position list failed: {e}")
        if issues:
            self.notify(("🔎 Startup reconcile:\n" if startup else "🔎 Reconcile:\n") + "\n".join(issues))
        else:
            log.info("Reconcile OK")

    # ---------- daily summary ----------

    def _maybe_daily_summary(self, now: int):
        today = s.utc_day(now)
        day_start = now - now % DAY_MS
        if now - day_start < SUMMARY_AFTER_MS or self.store.get("last_summary_day") == today:
            return
        self.store.set("last_summary_day", today)
        lines = []
        for mode in ("live", "paper"):
            trades = [t for t in self.store.closed_between(day_start - DAY_MS, day_start) if t["mode"] == mode]
            if mode != self.mode and not trades:
                continue
            wins = sum(1 for t in trades if t["pnl"] >= 0)
            pnl = sum((t["pnl"] for t in trades), Decimal(0))
            equity, _, _ = self.equity(mode)
            lines.append(f"{mode.upper()}: equity ${equity:,.2f} | trades {len(trades)} | wins {wins} | "
                         f"losses {len(trades) - wins} | P&L {_money(pnl)}")
        self.notify(f"📅 Daily summary {s.utc_day(day_start - DAY_MS)}\n" + "\n".join(lines))

    # ---------- commands (called from Telegram handlers, in a thread) ----------

    def status_text(self) -> str:
        with self.lock:
            now = self.now_ms()
            mode = self.mode
            prices = {}
            for t in self.store.active_trades():
                try:
                    prices[t["symbol"]] = self._last_price(t["symbol"])
                except Exception:
                    pass
            equity, open_value, upnl = self.equity(mode, prices)
            st = self._loss_state()
            limit = self.cfg.daily_loss_limit
            blocked = s.is_blocked(st, now, limit)
            lines = [f"📊 S&E status — mode {mode.upper()}"
                     + (" | PAUSED" if self.paused else "")
                     + (" | STOP file present" if os.path.exists(se_config.STOP_FILE) else ""),
                     f"Budget ${self.cfg.budget_usdt} | equity ${equity:,.2f} | in positions ${open_value:,.2f} "
                     f"| unrealized {_money(upnl)}",
                     f"Losses in a row today: {s.losses_today(st, now)}/{limit if limit else 'off'}"
                     + (" — LIMIT ACTIVE until 00:00 UTC" if blocked else "")]
            active = self.store.active_trades()
            if not active:
                lines.append("No open positions.")
            for t in active:
                sym = t["symbol"]
                px = prices.get(sym)
                if t["entry_price"] and px:
                    u = (px - t["entry_price"]) * t["qty"]
                    upnl_txt = f"{_money(u)} ({s.r_multiple(u, t['risk_dist'], t['qty']):+.2f}R)"
                else:
                    upnl_txt = "-"
                lines.append(f"\n{sym} {t['mode'].upper()} [{t['status']}] qty {t['qty']}"
                             + (f" {t['leverage']}x" if t["leverage"] else "")
                             + f"\n  Entry {self._px(sym, t['entry_price'])} | now {self._px(sym, px)}"
                             + f"\n  SL {self._px(sym, t['sl'])} | TP {self._px(sym, t['tp'])} | uPnL {upnl_txt}")
            return "\n".join(lines)

    def trades_text(self, n: int = 10) -> str:
        trades = self.store.closed_trades(limit=n)
        if not trades:
            return "No closed S&E trades yet."
        lines = [f"Last {len(trades)} closed S&E trades:"]
        for t in trades:
            lines.append(f"{_fmt_time(t['exit_time'])} {t['symbol']} {t['mode']} {t['exit_reason']} "
                         f"{self._px(t['symbol'], t['entry_price'])}→{self._px(t['symbol'], t['exit_price'])} "
                         f"{_money(t['pnl'])} ({t['r_mult']:+.2f}R)")
        return "\n".join(lines)

    def set_paused(self, paused: bool) -> str:
        self.store.set("paused", paused)
        log.info("paused=%s", paused)
        return ("⏸️ S&E paused: no new entries. Open trades keep their TP/SL and stay managed."
                if paused else "▶️ S&E resumed: new signals will be traded.")

    def close_targets(self, arg: str) -> List[dict]:
        arg = arg.upper()
        if arg != "ALL" and not arg.endswith("USDT"):
            arg += "USDT"
        return [t for t in self.store.active_trades() if arg == "ALL" or t["symbol"] == arg]

    def close_trades(self, arg: str) -> str:
        """Close the worker's position(s) at market and cancel their TP/SL. Returns a report."""
        with self.lock:
            out = []
            for t in self.close_targets(arg):
                sym = t["symbol"]
                try:
                    if t["status"] == "pending":
                        out.append(f"{sym}: entry still being placed — try again in a minute")
                    elif t["mode"] == "paper":
                        price = self._last_price(sym)
                        exit_fee = s.fee(price, t["qty"], self.cfg.fee_pct)
                        pnl = (price - t["entry_price"]) * t["qty"] - t["entry_fee"] - exit_fee
                        self._finish_close(t, price, self.now_ms(), "MANUAL", pnl,
                                           t["entry_fee"] + exit_fee, Decimal(0))
                        out.append(f"{sym}: paper trade closed at {self._px(sym, price)}")
                    else:
                        ex = self._ex()
                        self.store.update_trade(t["id"], status="closing", exit_reason="MANUAL")
                        qty = ex.close_position(sym)
                        try:
                            ex.cancel_position_tpsl(sym, t["position_id"])
                        except Exception as e:
                            log.warning("%s: TP/SL cancel after close: %s", sym, e)
                        out.append(f"{sym}: closed {qty} at market — P&L follows when Bitunix reports it")
                except Exception as e:
                    log.exception("close failed for %s", sym)
                    out.append(f"{sym}: close FAILED: {e} — check Bitunix")
            return "\n".join(out) or "No S&E positions to close."

    def mode_switch_problem(self, mode: str) -> Optional[str]:
        if mode not in ("paper", "live"):
            return "Use /se_mode paper or /se_mode live."
        if mode == self.mode:
            return f"Already in {mode} mode."
        if mode == "live":
            if not self.cfg.live_allowed:
                return ("Live mode is locked: set SE_LIVE_CONFIRM=I_ACCEPT_THE_RISK in .env and restart the bot first.")
            if not os.getenv("BITUNIX_API_KEY") or not os.getenv("BITUNIX_API_SECRET"):
                return "Live mode needs BITUNIX_API_KEY and BITUNIX_API_SECRET in .env."
        return None

    def set_mode(self, mode: str) -> str:
        with self.lock:
            problem = self.mode_switch_problem(mode)
            if problem:
                return problem
            if mode == "live":
                try:
                    self._sync_time(force=True)
                    available = self._ex().available_usdt()
                except Exception as e:
                    return f"Could not reach Bitunix with the API key: {e} — still in paper mode."
            self.store.set("mode", mode)
            log.warning("Mode switched to %s", mode)
            if mode == "live":
                self.reconcile()
                return (f"🔴 S&E is now LIVE. Bitunix available balance ${available:,.2f}. Live equity starts at "
                        f"the ${self.cfg.budget_usdt} budget (paper results don't carry over). Open paper "
                        f"trades finish in paper.")
            return "🟡 S&E is now in PAPER mode. Open live trades stay managed live until they close."


def start(notify: Callable[[str], None]) -> Optional[SEWorker]:
    """
    Called from main.py. Builds the worker and starts it in a daemon thread
    with its own restart loop, so a crash here never stops the existing bot.
    Returns None if SE_ENABLED is false or the settings are invalid.
    """
    setup_logging()
    try:
        cfg = se_config.load()
    except Exception as e:
        log.error("Invalid S&E settings: %s", e)
        notify(f"[S&E] ⚠️ Not started — invalid settings in .env: {e}")
        return None
    if not cfg.enabled:
        log.info("S&E disabled (SE_ENABLED=false)")
        return None
    worker = SEWorker(cfg, Store(se_config.STATE_DB_PATH), notify)
    stop = threading.Event()

    def supervise():
        backoff = 5
        while not stop.is_set():
            started = time.time()
            try:
                worker.run(stop)
            except BaseException as e:  # never let anything escape the thread
                log.exception("Worker crashed")
                worker.error("crash", f"Worker crashed: {e} — restarting in {backoff}s")
            if stop.is_set():
                break
            if time.time() - started > 600:
                backoff = 5
            stop.wait(backoff)
            backoff = min(backoff * 2, 300)

    threading.Thread(target=supervise, daemon=True, name="se-worker").start()
    log.info("S&E worker thread started")
    return worker
