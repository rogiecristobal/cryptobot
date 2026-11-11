"""
Sweep & Engulf strategy maths. Pure functions only: no network, no clock,
no database. The live worker, the replay command and the unit tests all run
this same code, so live trades can be compared with the backtest.

Prices and quantities are Decimal throughout.
"""
import datetime
from dataclasses import dataclass, field, replace
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import List, Optional, Tuple

CANDLE_MS = 4 * 60 * 60 * 1000
ZERO = Decimal(0)
# Spot cap: never spend more than 99.5% of the free cash (room for fees).
CASH_BUFFER = Decimal("0.995")


@dataclass(frozen=True)
class Candle:
    t: int          # open time, Unix ms (UTC)
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal

    @property
    def bullish(self) -> bool:
        return self.c > self.o


# ---------- candles ----------

def last_closed_open_time(now_ms: int, candle_ms: int = CANDLE_MS) -> int:
    """Open time of the most recent fully closed candle at now_ms (UTC-aligned)."""
    current_open = now_ms - now_ms % candle_ms
    return current_open - candle_ms


def is_closed(candle_open_ms: int, now_ms: int, candle_ms: int = CANDLE_MS) -> bool:
    """A candle is closed when its open time + 4 hours <= now."""
    return candle_open_ms + candle_ms <= now_ms


# ---------- indicators (same maths as TradingView ta.ema / ta.atr) ----------

def ema(values: List[Decimal], length: int) -> List[Optional[Decimal]]:
    """
    EMA(length). The first value, on bar `length` (index length-1), is the
    simple average of the first `length` values; after that
    EMA = a * value + (1 - a) * previous EMA, with a = 2 / (length + 1).
    Bars before the first value are None.
    """
    out: List[Optional[Decimal]] = [None] * len(values)
    if length < 1 or len(values) < length:
        return out
    alpha = Decimal(2) / Decimal(length + 1)
    prev = sum(values[:length], ZERO) / Decimal(length)
    out[length - 1] = prev
    for i in range(length, len(values)):
        prev = alpha * values[i] + (1 - alpha) * prev
        out[i] = prev
    return out


def true_ranges(candles: List[Candle]) -> List[Decimal]:
    """high - low on the first candle; after that the largest of high - low,
    |high - previous close| and |low - previous close|."""
    out = []
    for i, c in enumerate(candles):
        if i == 0:
            out.append(c.h - c.l)
        else:
            pc = candles[i - 1].c
            out.append(max(c.h - c.l, abs(c.h - pc), abs(c.l - pc)))
    return out


def atr(candles: List[Candle], length: int) -> List[Optional[Decimal]]:
    """
    ATR(length) with Wilder's smoothing: the first value (index length-1) is
    the average of the first `length` true ranges; after that
    ATR = (previous ATR * (length - 1) + TR) / length.
    """
    trs = true_ranges(candles)
    out: List[Optional[Decimal]] = [None] * len(trs)
    if length < 1 or len(trs) < length:
        return out
    prev = sum(trs[:length], ZERO) / Decimal(length)
    out[length - 1] = prev
    for i in range(length, len(trs)):
        prev = (prev * (length - 1) + trs[i]) / Decimal(length)
        out[i] = prev
    return out


# ---------- rounding ----------

def round_to_step(value: Decimal, step: Optional[Decimal], rounding=ROUND_HALF_UP) -> Decimal:
    """Round to a multiple of step (nearest by default). step None/0: unchanged."""
    if not step:
        return value
    return (value / step).to_integral_value(rounding=rounding) * step


def floor_to_step(value: Decimal, step: Optional[Decimal]) -> Decimal:
    return round_to_step(value, step, ROUND_DOWN)


# ---------- signal ----------

@dataclass(frozen=True)
class Signal:
    index: int
    candle_time: int          # open time of the signal candle
    close: Decimal
    ema: Optional[Decimal]
    atr: Optional[Decimal]
    sl: Decimal               # rounded to the price step
    tp: Decimal               # rounded to the price step
    risk_dist: Decimal        # |close - SL| (rounded SL): the 1R distance used for sizing
    side: str = "LONG"        # LONG | SHORT (shorts: replay/backtest only, never traded live)


def find_signal(candles: List[Candle], i: int, ema_vals, atr_vals, cfg,
                price_step: Optional[Decimal] = None, direction: str = "long") -> Optional[Signal]:
    """The signal on candle i for direction long | short | both. Long and short
    can't both fire on one candle (close can't be above the previous high
    and below the previous low)."""
    if direction in ("long", "both"):
        sig = check_signal(candles, i, ema_vals, atr_vals, cfg, price_step)
        if sig:
            return sig
    if direction in ("short", "both"):
        return check_short_signal(candles, i, ema_vals, atr_vals, cfg, price_step)
    return None


def check_short_signal(candles: List[Candle], i: int, ema_vals, atr_vals, cfg,
                       price_step: Optional[Decimal] = None) -> Optional[Signal]:
    """
    Mirror image of check_signal, for the backtest/replay only (the live
    worker is long only): high[i] > high[i-1], close[i] < low[i-1], previous
    candle(s) red, close < EMA; SL = close + ATR x mult (or the candle high),
    TP = close - rr x risk.
    """
    if i < 1 or i >= len(candles):
        return None
    cur, prev = candles[i], candles[i - 1]
    if not cur.h > prev.h:          # 1. sweep of the previous high
        return None
    if not cur.c < prev.l:          # 2. engulf: close below the previous low
        return None
    mode = cfg.prev_candle_mode     # 3. previous candle filter (red)
    if mode == "same":
        if not prev.c < prev.o:
            return None
    elif mode == "three_in_a_row":
        if i < 2 or not all(c.c < c.o for c in (candles[i - 2], prev, cur)):
            return None
    elif mode != "mixed":
        raise ValueError(f"unknown prev_candle_mode {mode}")
    ema_i = ema_vals[i] if ema_vals else None
    if cfg.ema_filter:              # 4. trend filter: below the EMA
        if ema_i is None or not cur.c < ema_i:
            return None
    atr_i = atr_vals[i] if atr_vals else None
    if cfg.sl_mode == "atr":
        if atr_i is None:
            return None
        sl_raw = cur.c + atr_i * cfg.atr_mult
    elif cfg.sl_mode == "candle":
        sl_raw = cur.h
    else:
        raise ValueError(f"unknown sl_mode {cfg.sl_mode}")
    tp_raw = cur.c - cfg.rr * (sl_raw - cur.c)
    sl = round_to_step(sl_raw, price_step)
    tp = round_to_step(tp_raw, price_step)
    risk_dist = sl - cur.c
    if tp <= 0 or risk_dist <= 0 or tp >= cur.c:
        return None
    return Signal(index=i, candle_time=cur.t, close=cur.c, ema=ema_i, atr=atr_i,
                  sl=sl, tp=tp, risk_dist=risk_dist, side="SHORT")


def check_signal(candles: List[Candle], i: int, ema_vals, atr_vals, cfg,
                 price_step: Optional[Decimal] = None) -> Optional[Signal]:
    """
    Long signal on closed candle i (conditions 1-4 of the rules). Conditions
    5 (no open/pending trade on the coin) and 6 (daily loss limit) depend on
    state, so the caller checks them.

    cfg needs: prev_candle_mode, ema_filter, sl_mode, atr_mult, rr.
    """
    if i < 1 or i >= len(candles):
        return None
    cur, prev = candles[i], candles[i - 1]

    # 1. Sweep: this candle's low takes out the previous low.
    if not cur.l < prev.l:
        return None
    # 2. Engulf: and it closes above the previous high.
    if not cur.c > prev.h:
        return None
    # 3. Previous candle filter.
    mode = cfg.prev_candle_mode
    if mode == "same":
        if not prev.bullish:
            return None
    elif mode == "three_in_a_row":
        if i < 2 or not (candles[i - 2].bullish and prev.bullish and cur.bullish):
            return None
    elif mode != "mixed":
        raise ValueError(f"unknown prev_candle_mode {mode}")
    # 4. Trend filter: no EMA value yet means no signal.
    ema_i = ema_vals[i] if ema_vals else None
    if cfg.ema_filter:
        if ema_i is None or not cur.c > ema_i:
            return None

    # Stop and target, fixed from the signal candle's close.
    atr_i = atr_vals[i] if atr_vals else None
    if cfg.sl_mode == "atr":
        if atr_i is None:
            return None
        sl_raw = cur.c - atr_i * cfg.atr_mult
    elif cfg.sl_mode == "candle":
        sl_raw = cur.l
    else:
        raise ValueError(f"unknown sl_mode {cfg.sl_mode}")
    risk_raw = cur.c - sl_raw
    tp_raw = cur.c + cfg.rr * risk_raw
    sl = round_to_step(sl_raw, price_step)
    tp = round_to_step(tp_raw, price_step)
    risk_dist = cur.c - sl
    if sl <= 0 or risk_dist <= 0 or tp <= cur.c:
        return None
    return Signal(index=i, candle_time=cur.t, close=cur.c, ema=ema_i, atr=atr_i,
                  sl=sl, tp=tp, risk_dist=risk_dist)


# ---------- sizing ----------

@dataclass(frozen=True)
class Sizing:
    qty: Decimal               # rounded down to the qty step; 0 when skipped
    risk_amount: Decimal       # $ the trade is meant to risk
    qty_by_risk: Decimal
    qty_cap: Decimal           # spot cap from free cash
    free_cash: Decimal
    sized_down: bool           # the spot cap made it smaller than the risk size
    skip_reason: Optional[str]


def size_position(equity: Decimal, budget: Decimal, open_entry_value: Decimal,
                  risk_pct: Decimal, compound: bool, risk_dist: Decimal, price: Decimal,
                  qty_step: Optional[Decimal], min_qty: Decimal, min_order_usdt: Decimal) -> Sizing:
    """
    Risk amount = equity (budget when compound is off) x risk_pct.
    Quantity by risk = risk amount / risk distance.
    Spot cap: free cash = equity - entry value of open positions;
              max quantity = free cash x 0.995 / price.
    The smaller of the two, rounded DOWN to the qty step. Skipped if below
    the exchange minimum or worth less than min_order_usdt.
    """
    base = equity if compound else budget
    risk_amount = base * risk_pct / Decimal(100)
    qty_by_risk = risk_amount / risk_dist if risk_dist > 0 else ZERO
    free_cash = equity - open_entry_value
    qty_cap = free_cash * CASH_BUFFER / price if free_cash > 0 and price > 0 else ZERO
    sized_down = qty_cap < qty_by_risk
    qty = floor_to_step(min(qty_by_risk, qty_cap), qty_step)

    reason = None
    if qty <= 0 or (min_qty and qty < min_qty) or qty * price < min_order_usdt:
        if sized_down:
            reason = f"not enough cash (free ${free_cash:.2f})"
        else:
            reason = (f"below minimum size (qty {qty}, value ${qty * price:.2f}; "
                      f"minimum {min_qty} / ${min_order_usdt})")
        qty = ZERO
    return Sizing(qty=qty, risk_amount=risk_amount, qty_by_risk=qty_by_risk, qty_cap=qty_cap,
                  free_cash=free_cash, sized_down=sized_down, skip_reason=reason)


# ---------- P&L ----------

def fee(price: Decimal, qty: Decimal, fee_pct: Decimal) -> Decimal:
    return price * qty * fee_pct / Decimal(100)


def r_multiple(pnl: Decimal, risk_dist: Decimal, qty: Decimal) -> Decimal:
    risk = risk_dist * qty
    return pnl / risk if risk > 0 else ZERO


# ---------- daily loss limit ----------

def utc_day(ms: int) -> str:
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%d")


@dataclass(frozen=True)
class LossState:
    count: int = 0                    # losing trades in a row
    day: Optional[str] = None         # UTC day (of the exit) the count belongs to
    blocked_day: Optional[str] = None # UTC day on which no new entries may fill


def record_close(state: LossState, pnl: Decimal, exit_ms: int, limit: int) -> Tuple[LossState, bool]:
    """
    Count a closed trade, in the order trades close. A loss (net P&L < 0)
    adds 1; anything else resets to 0. A new UTC day (by exit time) starts
    the count again at 0. Returns (new state, True if the limit was just reached).
    """
    day = utc_day(exit_ms)
    count = state.count if state.day == day else 0
    count = count + 1 if pnl < 0 else 0
    blocked = state.blocked_day
    reached = False
    if limit > 0 and count >= limit and blocked != day:
        blocked = day
        reached = True
    return LossState(count=count, day=day, blocked_day=blocked), reached


def is_blocked(state: LossState, at_ms: int, limit: int) -> bool:
    """True if an entry filling at at_ms falls on a day the limit blocks."""
    return limit > 0 and state.blocked_day is not None and state.blocked_day == utc_day(at_ms)


def losses_today(state: LossState, now_ms: int) -> int:
    return state.count if state.day == utc_day(now_ms) else 0


# ---------- paper exits ----------

def paper_exit(candle: Candle, sl: Decimal, tp: Decimal,
               open_price: Optional[Decimal] = None, side: str = "LONG") -> Optional[Tuple[str, Decimal]]:
    """
    Did this candle close the trade? Returns ("SL"|"TP", exit price) or None.
    - A candle that opens beyond a level exits at that open.
    - If one candle touches both, the level closer to the candle's open counts
      as hit first (a tie counts as the SL).
    open_price overrides the candle's open, for the candle the entry filled in.
    side SHORT mirrors it (SL above, TP below).
    """
    o = candle.o if open_price is None else open_price
    if side == "SHORT":
        if o >= sl:
            return "SL", o
        if o <= tp:
            return "TP", o
        hit_sl = candle.h >= sl
        hit_tp = candle.l <= tp
        if hit_sl and hit_tp:
            return ("SL", sl) if (sl - o) <= (o - tp) else ("TP", tp)
        if hit_sl:
            return "SL", sl
        if hit_tp:
            return "TP", tp
        return None
    if o <= sl:
        return "SL", o
    if o >= tp:
        return "TP", o
    hit_sl = candle.l <= sl
    hit_tp = candle.h >= tp
    if hit_sl and hit_tp:
        return ("SL", sl) if (o - sl) <= (tp - o) else ("TP", tp)
    if hit_sl:
        return "SL", sl
    if hit_tp:
        return "TP", tp
    return None


# ---------- single-symbol backtest (replay + reference check) ----------

@dataclass
class BacktestTrade:
    signal: Signal
    entry_time: int
    entry: Decimal
    qty: Decimal
    sized_down: bool
    entry_fee: Decimal
    exit_time: Optional[int] = None
    exit: Optional[Decimal] = None
    reason: Optional[str] = None      # TP | SL | OPEN
    pnl: Optional[Decimal] = None
    r: Optional[Decimal] = None
    equity_after: Optional[Decimal] = None


@dataclass
class BacktestResult:
    trades: List[BacktestTrade] = field(default_factory=list)
    skipped: List[Tuple[Signal, str]] = field(default_factory=list)
    equity: Decimal = ZERO


def backtest(candles: List[Candle], cfg, start_equity: Decimal, fee_pct: Decimal,
             qty_step: Optional[Decimal] = None, min_qty: Decimal = ZERO,
             price_step: Optional[Decimal] = None, min_order_usdt: Optional[Decimal] = None,
             from_ms: Optional[int] = None) -> BacktestResult:
    """
    Replays the rules on one symbol's closed candles, the way the backtest
    does: signal on candle i's close, market entry at candle i+1's open, exit
    on the first candle (the entry candle included) that touches the SL or TP.
    Signals on candles before from_ms only warm up the indicators.
    """
    ema_vals = ema([c.c for c in candles], cfg.ema_length)
    atr_vals = atr(candles, cfg.atr_length)
    min_usdt = cfg.min_order_usdt if min_order_usdt is None else min_order_usdt
    result = BacktestResult(equity=start_equity)
    loss = LossState()
    pos: Optional[BacktestTrade] = None
    pending: Optional[Signal] = None

    for j, cd in enumerate(candles):
        # Entry at this candle's open for the signal of the previous candle.
        if pending is not None:
            sig, pending = pending, None
            if is_blocked(loss, cd.t, cfg.daily_loss_limit):
                result.skipped.append((sig, "daily loss limit"))
            else:
                sz = size_position(result.equity, start_equity, ZERO, cfg.risk_pct, cfg.compound,
                                   sig.risk_dist, cd.o, qty_step, min_qty, min_usdt)
                if sz.skip_reason:
                    result.skipped.append((sig, sz.skip_reason))
                else:
                    pos = BacktestTrade(signal=sig, entry_time=cd.t, entry=cd.o, qty=sz.qty,
                                        sized_down=sz.sized_down, entry_fee=fee(cd.o, sz.qty, fee_pct))
        # Exit check.
        if pos is not None:
            side = pos.signal.side
            hit = paper_exit(cd, pos.signal.sl, pos.signal.tp,
                             open_price=pos.entry if cd.t == pos.entry_time else None, side=side)
            if hit:
                reason, price = hit
                move = (pos.entry - price) if side == "SHORT" else (price - pos.entry)
                pnl = move * pos.qty - pos.entry_fee - fee(price, pos.qty, fee_pct)
                result.equity += pnl
                pos.exit_time, pos.exit, pos.reason, pos.pnl = cd.t, price, reason, pnl
                pos.r = r_multiple(pnl, pos.signal.risk_dist, pos.qty)
                pos.equity_after = result.equity
                loss, _ = record_close(loss, pnl, cd.t, cfg.daily_loss_limit)
                result.trades.append(pos)
                pos = None
        # New signal on this (closed) candle, only when flat with nothing pending.
        if pos is None and pending is None:
            sig = find_signal(candles, j, ema_vals, atr_vals, cfg, price_step,
                              direction=getattr(cfg, "direction", "long"))
            if sig and (from_ms is None or cd.t >= from_ms):
                if j + 1 < len(candles):
                    pending = sig
    if pos is not None:
        pos.reason = "OPEN"
        result.trades.append(pos)
    return result


def with_overrides(cfg, **changes):
    """A copy of a dataclass config with some fields changed (for tests/replay)."""
    return replace(cfg, **changes)
