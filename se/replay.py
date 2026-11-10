"""
Replay the Sweep & Engulf rules over Bitunix 4h history and print every trade.

    python -m se.replay --symbol BTCUSDT --from 2025-01-01
    python -m se.replay --symbol SOLUSDT --from 2025-01-01 --to 2025-06-30 --equity 10000 --fee-pct 0

Uses the SE_* settings from .env (same code as the live worker), so the
output can be compared with the TradingView strategy on the same coin. It
needs no API keys: klines and precision come from Bitunix's public API.
The 1,000 candles before --from only warm up the EMA/ATR.

Single symbol: the budget isn't shared with other coins here, so with
several coins open at once live sizing can be smaller (the spot cap).
"""
import argparse
import datetime
import sys
from decimal import Decimal

from se import config as se_config
from se import market
from se import strategy as s

WARMUP_CANDLES = 1000


def _ms(day: str) -> int:
    return int(datetime.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def _t(ms: int) -> str:
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Replay Sweep & Engulf on Bitunix 4h history")
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--to", dest="date_to", help="YYYY-MM-DD (UTC), default now")
    ap.add_argument("--equity", type=Decimal, help="starting equity (default SE_BUDGET_USDT)")
    ap.add_argument("--fee-pct", type=Decimal, help="fee per side in %% (default SE_FEE_PCT)")
    ap.add_argument("--no-rounding", action="store_true", help="don't round qty/prices to Bitunix steps")
    args = ap.parse_args(argv)

    cfg = se_config.load()
    symbol = args.symbol.upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    equity = args.equity if args.equity is not None else cfg.budget_usdt
    fee_pct = args.fee_pct if args.fee_pct is not None else cfg.fee_pct
    start = _ms(args.date_from)
    end = _ms(args.date_to) + 24 * 3600 * 1000 if args.date_to else None

    session = market.new_session()
    inst = market.fetch_instrument(session, symbol)
    candles = market.fetch_history(session, symbol, since_ms=start - WARMUP_CANDLES * s.CANDLE_MS)
    if end:
        candles = [c for c in candles if c.t < end]
    if not candles or candles[-1].t < start:
        print(f"No Bitunix 4h candles for {symbol} after {args.date_from}.")
        return 1

    steps = {} if args.no_rounding else dict(qty_step=inst["qty_step"], min_qty=inst["min_qty"],
                                             price_step=inst["price_step"])
    r = s.backtest(candles, cfg, equity, fee_pct, from_ms=start, **steps)

    print(f"Sweep & Engulf replay — {symbol} 4h, Bitunix last-price candles {_t(candles[0].t)} .. "
          f"{_t(candles[-1].t)} UTC ({len(candles)} candles; signals from {args.date_from})")
    print(f"Settings: {cfg.summary()}")
    print(f"Start equity ${equity}, fee {fee_pct}% per side, "
          + ("no rounding" if args.no_rounding else
             f"price step {inst['price_step']}, qty step {inst['qty_step']}, min qty {inst['min_qty']}"))
    print()
    hdr = (f"{'#':>3}  {'signal candle':16}  {'entry time':16}  {'entry':>11}  {'SL':>11}  {'TP':>11}  "
           f"{'qty':>10}  {'exit time':16}  {'exit':>11}  {'why':4}  {'P&L $':>10}  {'R':>6}  {'equity':>10}")
    print(hdr)
    print("-" * len(hdr))
    for n, t in enumerate(r.trades, 1):
        sd = "*" if t.sized_down else " "
        if t.reason == "OPEN":
            print(f"{n:>3}  {_t(t.signal.candle_time):16}  {_t(t.entry_time):16}  {t.entry:>11}  "
                  f"{t.signal.sl:>11}  {t.signal.tp:>11}  {t.qty:>9}{sd}  {'(still open)':16}")
            continue
        print(f"{n:>3}  {_t(t.signal.candle_time):16}  {_t(t.entry_time):16}  {t.entry:>11}  {t.signal.sl:>11}  "
              f"{t.signal.tp:>11}  {t.qty:>9}{sd}  {_t(t.exit_time):16}  {t.exit:>11}  {t.reason:4}  "
              f"{t.pnl:>10.2f}  {t.r:>6.2f}  {t.equity_after:>10.2f}")
    closed = [t for t in r.trades if t.reason in ("TP", "SL")]
    wins = sum(1 for t in closed if t.pnl > 0)
    print()
    print(f"Trades {len(closed)} closed (+{len(r.trades) - len(closed)} open) | wins {wins} | "
          f"losses {len(closed) - wins} | win rate {wins / len(closed) * 100 if closed else 0:.1f}% | "
          f"net P&L ${r.equity - equity:,.2f} | final equity ${r.equity:,.2f}")
    if r.skipped:
        print(f"Skipped signals: {len(r.skipped)}")
        for sig, why in r.skipped:
            print(f"  {_t(sig.candle_time)}  {why}")
    print("* = sized down by the spot cap")
    return 0


if __name__ == "__main__":
    sys.exit(main())
