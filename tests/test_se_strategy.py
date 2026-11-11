"""
Unit tests for se/strategy.py. No network. Run from the bot folder:
    python -m unittest discover -s tests -v
"""
import unittest
from decimal import Decimal as D

from se.config import SEConfig
from se import strategy as s
from se.strategy import Candle

H = s.CANDLE_MS


def candles(rows, start=0):
    """rows: (open, high, low, close) tuples -> Candles 4h apart."""
    return [Candle(t=start + i * H, o=D(str(o)), h=D(str(h)), l=D(str(l)), c=D(str(c)))
            for i, (o, h, l, c) in enumerate(rows)]


def cfg(**kw):
    base = dict(prev_candle_mode="mixed", ema_filter=False, sl_mode="candle", rr=D("2"),
                atr_mult=D("1"), ema_length=200, atr_length=14, daily_loss_limit=2,
                risk_pct=D("2"), compound=True, min_order_usdt=D("0"))
    base.update(kw)
    return SEConfig(**base)


class TestIndicators(unittest.TestCase):
    def test_ema_seed_and_recursion(self):
        vals = [D(x) for x in ("1", "2", "3", "4", "5")]
        out = s.ema(vals, 3)
        self.assertIsNone(out[0])
        self.assertIsNone(out[1])
        self.assertEqual(out[2], D(2))                       # SMA of 1,2,3
        self.assertEqual(out[3], D("0.5") * 4 + D("0.5") * 2)  # a = 2/4 = 0.5 -> 3
        self.assertEqual(out[4], D("0.5") * 5 + D("0.5") * 3)  # 4

    def test_ema_not_enough_values(self):
        self.assertEqual(s.ema([D(1), D(2)], 3), [None, None])

    def test_ema_200_alpha(self):
        vals = [D(100)] * 200 + [D(301)]
        out = s.ema(vals, 200)
        self.assertEqual(out[199], D(100))
        self.assertLess(abs(out[200] - D(102)), D("1e-20"))  # 2/201 * 301 + 199/201 * 100 = 102

    def test_true_range(self):
        cs = candles([(10, 12, 9, 11), (11, 15, 10.5, 14), (14, 14.5, 8, 9), (9, 9.5, 8.5, 9)])
        self.assertEqual(s.true_ranges(cs), [D(3), D(4.5), D("6.5"), D(1)])
        # candle 1: h-l 4.5, |15-11| 4, |10.5-11| 0.5 -> 4.5
        # candle 2: h-l 6.5, |14.5-14| .5, |8-14| 6 -> 6.5
        # candle 3: h-l 1, |9.5-9| .5, |8.5-9| .5 -> 1

    def test_atr_wilder(self):
        cs = candles([(10, 12, 9, 11), (11, 15, 10.5, 14), (14, 14.5, 8, 9), (9, 9.5, 8.5, 9)])
        out = s.atr(cs, 3)
        self.assertEqual(out[:2], [None, None])
        first = (D(3) + D("4.5") + D("6.5")) / 3            # 14/3
        self.assertEqual(out[2], first)
        self.assertEqual(out[3], (first * 2 + D(1)) / 3)


class TestSignal(unittest.TestCase):
    # prev candle bullish (100->102), signal candle sweeps 99 < 99.5 and closes 104 > 103
    BASE = [(100, 103, 99.5, 102), (102, 104.5, 99, 104)]

    def sig(self, rows, i=None, **kw):
        cs = candles(rows)
        c = cfg(**kw)
        e = s.ema([x.c for x in cs], c.ema_length)
        a = s.atr(cs, c.atr_length)
        return s.check_signal(cs, len(cs) - 1 if i is None else i, e, a, c)

    def test_basic_signal(self):
        sig = self.sig(self.BASE)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.sl, D(99))
        self.assertEqual(sig.risk_dist, D(5))
        self.assertEqual(sig.tp, D(114))  # 104 + 2 * 5

    def test_no_sweep(self):
        self.assertIsNone(self.sig([(100, 103, 99.5, 102), (102, 104.5, 99.5, 104)]))  # low not below

    def test_no_engulf(self):
        self.assertIsNone(self.sig([(100, 103, 99.5, 102), (102, 104.5, 99, 103)]))  # close == prev high

    def test_prev_same_requires_bullish_prev(self):
        bearish_prev = [(102, 103, 99.5, 100), (100, 104.5, 99, 104)]
        self.assertIsNone(self.sig(bearish_prev, prev_candle_mode="same"))
        self.assertIsNotNone(self.sig(bearish_prev, prev_candle_mode="mixed"))
        self.assertIsNotNone(self.sig(self.BASE, prev_candle_mode="same"))

    def test_three_in_a_row(self):
        ok = [(98, 100, 97, 99.5)] + self.BASE
        bad = [(100, 100.5, 97, 99)] + self.BASE   # i-2 bearish
        self.assertIsNotNone(self.sig(ok, prev_candle_mode="three_in_a_row"))
        self.assertIsNone(self.sig(bad, prev_candle_mode="three_in_a_row"))
        self.assertIsNone(self.sig(self.BASE, prev_candle_mode="three_in_a_row"))  # no i-2 candle

    def test_ema_filter(self):
        # EMA(2): seed = (102 + 104)/2 = 103 on the last candle; close 104 > 103 -> signal
        self.assertIsNotNone(self.sig(self.BASE, ema_filter=True, ema_length=2))
        # no EMA value yet -> no signal
        self.assertIsNone(self.sig(self.BASE, ema_filter=True, ema_length=3))
        # close below EMA -> no signal
        rows = [(200, 201, 199, 200), (200, 201, 199, 200)] + self.BASE
        self.assertIsNone(self.sig(rows, ema_filter=True, ema_length=3))

    def test_atr_sl(self):
        # ATR(1) on the last candle = its TR = max(5.5, |104.5-102|, |99-102|) = 5.5
        sig = self.sig(self.BASE, sl_mode="atr", atr_length=1, atr_mult=D("1"))
        self.assertEqual(sig.sl, D("98.5"))
        self.assertEqual(sig.tp, D("115"))   # 104 + 2 * 5.5
        self.assertIsNone(self.sig(self.BASE, sl_mode="atr", atr_length=5))  # no ATR yet

    def test_price_step_rounding(self):
        cs = candles(self.BASE)
        c = cfg(sl_mode="atr", atr_length=1, atr_mult=D("0.33"))
        sig = s.check_signal(cs, 1, None, s.atr(cs, 1), c, price_step=D("0.1"))
        # SL = 104 - 5.5*0.33 = 102.185 -> 102.2 ; TP = 104 + 2*1.815 = 107.63 -> 107.6
        self.assertEqual(sig.sl, D("102.2"))
        self.assertEqual(sig.tp, D("107.6"))
        self.assertEqual(sig.risk_dist, D("1.8"))


class TestSizing(unittest.TestCase):
    def size(self, **kw):
        args = dict(equity=D(100), budget=D(100), open_entry_value=D(0), risk_pct=D(3), compound=True,
                    risk_dist=D(2), price=D(100), qty_step=D("0.001"), min_qty=D("0.001"),
                    min_order_usdt=D(10))
        args.update(kw)
        return s.size_position(**args)

    def test_by_risk_not_capped(self):
        z = self.size()                 # risk $3 / 2 = 1.5 ; cap 100*.995/100 = 0.995 -> capped!
        self.assertTrue(z.sized_down)
        self.assertEqual(z.qty, D("0.995"))
        z = self.size(risk_dist=D(10))  # 3/10 = 0.3 < 0.995
        self.assertFalse(z.sized_down)
        self.assertEqual(z.qty, D("0.300"))

    def test_spot_cap_uses_open_positions(self):
        z = self.size(open_entry_value=D(60))   # free 40 -> cap 0.398
        self.assertTrue(z.sized_down)
        self.assertEqual(z.qty, D("0.398"))
        self.assertEqual(z.free_cash, D(40))

    def test_rounds_down(self):
        z = self.size(risk_dist=D(7))          # 3/7 = 0.428571... -> 0.428
        self.assertEqual(z.qty, D("0.428"))

    def test_minimum_order_value(self):
        z = self.size(risk_dist=D(50))         # 0.06 qty * 100 = $6 < $10
        self.assertEqual(z.qty, 0)
        self.assertIn("below minimum", z.skip_reason)

    def test_exchange_minimum_qty(self):
        z = self.size(risk_dist=D(10), min_qty=D("0.5"))
        self.assertEqual(z.qty, 0)
        self.assertIn("below minimum", z.skip_reason)

    def test_not_enough_cash(self):
        z = self.size(open_entry_value=D(95))  # free 5 -> $4.975 < $10
        self.assertEqual(z.qty, 0)
        self.assertIn("not enough cash", z.skip_reason)

    def test_compounding(self):
        on = self.size(equity=D(200), risk_dist=D(20), compound=True)
        off = self.size(equity=D(200), risk_dist=D(20), compound=False)
        self.assertEqual(on.risk_amount, D(6))   # 3% of equity 200
        self.assertEqual(off.risk_amount, D(3))  # 3% of budget 100
        self.assertEqual(on.qty, D("0.300"))
        self.assertEqual(off.qty, D("0.150"))


class TestDailyLossLimit(unittest.TestCase):
    DAY1 = 1_767_225_600_000          # 2026-01-01 00:00 UTC
    DAY2 = DAY1 + 24 * 3600 * 1000

    def test_counts_and_blocks(self):
        st = s.LossState()
        st, reached = s.record_close(st, D(-1), self.DAY1 + 1000, 2)
        self.assertEqual((st.count, reached), (1, False))
        self.assertFalse(s.is_blocked(st, self.DAY1 + 5000, 2))
        st, reached = s.record_close(st, D(-1), self.DAY1 + 2000, 2)
        self.assertEqual((st.count, reached), (2, True))
        self.assertTrue(s.is_blocked(st, self.DAY1 + 3 * 3600 * 1000, 2))
        self.assertFalse(s.is_blocked(st, self.DAY2 + 1, 2))   # next UTC day: trading resumes
        st, reached = s.record_close(st, D(-1), self.DAY1 + 3000, 2)
        self.assertFalse(reached)                              # already blocked: no second alert

    def test_win_resets(self):
        st = s.LossState()
        st, _ = s.record_close(st, D(-1), self.DAY1, 2)
        st, _ = s.record_close(st, D(5), self.DAY1 + 1, 2)
        self.assertEqual(st.count, 0)
        st, reached = s.record_close(st, D(-1), self.DAY1 + 2, 2)
        self.assertEqual((st.count, reached), (1, False))

    def test_zero_pnl_is_not_a_loss(self):
        st, _ = s.record_close(s.LossState(count=1, day=s.utc_day(self.DAY1)), D(0), self.DAY1, 2)
        self.assertEqual(st.count, 0)

    def test_new_day_resets(self):
        st = s.LossState()
        st, _ = s.record_close(st, D(-1), self.DAY1 + 23 * 3600 * 1000, 2)
        st, reached = s.record_close(st, D(-1), self.DAY2 + 1000, 2)   # new UTC day by exit time
        self.assertEqual((st.count, reached), (1, False))

    def test_off(self):
        st = s.LossState()
        for k in range(5):
            st, reached = s.record_close(st, D(-1), self.DAY1 + k, 0)
            self.assertFalse(reached)
        self.assertFalse(s.is_blocked(st, self.DAY1 + 10, 0))


class TestPaperExits(unittest.TestCase):
    def c(self, o, h, l, cl):
        return Candle(t=0, o=D(str(o)), h=D(str(h)), l=D(str(l)), c=D(str(cl)))

    def test_no_touch(self):
        self.assertIsNone(s.paper_exit(self.c(100, 105, 95, 101), D(90), D(110)))

    def test_tp_and_sl_single(self):
        self.assertEqual(s.paper_exit(self.c(100, 111, 95, 101), D(90), D(110)), ("TP", D(110)))
        self.assertEqual(s.paper_exit(self.c(100, 105, 89, 101), D(90), D(110)), ("SL", D(90)))

    def test_both_touched_closer_to_open_wins(self):
        # open 107: TP 110 is 3 away, SL 90 is 17 away -> TP first
        self.assertEqual(s.paper_exit(self.c(107, 111, 89, 100), D(90), D(110)), ("TP", D(110)))
        # open 93: SL first
        self.assertEqual(s.paper_exit(self.c(93, 111, 89, 100), D(90), D(110)), ("SL", D(90)))
        # tie counts as SL
        self.assertEqual(s.paper_exit(self.c(100, 111, 89, 100), D(90), D(110)), ("SL", D(90)))

    def test_open_beyond_level(self):
        self.assertEqual(s.paper_exit(self.c(88, 95, 85, 94), D(90), D(110)), ("SL", D(88)))
        self.assertEqual(s.paper_exit(self.c(112, 115, 108, 113), D(90), D(110)), ("TP", D(112)))

    def test_open_price_override(self):
        # entry candle: entry price 100 used instead of the candle open 88
        self.assertIsNone(s.paper_exit(self.c(88, 105, 95, 101), D(90), D(110), open_price=D(100)))


class TestReferenceCheck(unittest.TestCase):
    """The reference trade from the backtest (see the prompt)."""
    ROWS = [(100, 101, 99, 100.5), (100.5, 102, 100, 101.5), (101.5, 103, 101, 102.5),
            (102.5, 104, 100.8, 103.6), (103.6, 105, 103, 104.5), (104.5, 109, 104, 108),
            (108, 109.5, 107, 109), (109, 110, 108, 109.5)]

    def setUp(self):
        self.cfg = cfg(prev_candle_mode="three_in_a_row", ema_filter=False, sl_mode="candle",
                       rr=D("2"), risk_pct=D("2"), daily_loss_limit=0)
        self.cs = candles(self.ROWS)

    def test_exactly_one_signal_on_candle_3(self):
        found = [i for i in range(len(self.cs))
                 if s.check_signal(self.cs, i, None, s.atr(self.cs, 14), self.cfg)]
        self.assertEqual(found, [3])

    def test_trade(self):
        r = s.backtest(self.cs, self.cfg, start_equity=D(10000), fee_pct=D(0))
        self.assertEqual(len(r.trades), 1)
        t = r.trades[0]
        self.assertEqual(t.signal.index, 3)
        self.assertEqual(t.entry, D("103.6"))           # candle 4's open
        self.assertEqual(t.signal.sl, D("100.8"))
        self.assertEqual(t.signal.tp, D("109.2"))
        self.assertEqual(round(t.qty, 2), D("71.43"))
        self.assertFalse(t.sized_down)
        self.assertEqual(t.reason, "TP")
        self.assertEqual(t.exit_time, self.cs[6].t)     # exits on candle 6
        self.assertEqual(round(t.pnl, 2), D("400.00"))
        self.assertEqual(round(t.r, 2), D("2.00"))


class TestShortSide(unittest.TestCase):
    """Shorts exist for the replay only. Mirror the reference trade: p -> 200 - p."""

    def mirrored(self):
        return candles([(200 - o, 200 - l, 200 - h, 200 - c) for o, h, l, c in TestReferenceCheck.ROWS])

    def test_mirrored_reference_trade(self):
        c = cfg(prev_candle_mode="three_in_a_row", ema_filter=False, sl_mode="candle", rr=D("2"),
                risk_pct=D("2"), daily_loss_limit=0, direction="both")
        r = s.backtest(self.mirrored(), c, start_equity=D(10000), fee_pct=D(0))
        self.assertEqual(len(r.trades), 1)
        t = r.trades[0]
        self.assertEqual(t.signal.side, "SHORT")
        self.assertEqual(t.signal.index, 3)
        self.assertEqual((t.entry, t.signal.sl, t.signal.tp), (D("96.4"), D("99.2"), D("90.8")))
        self.assertEqual(t.reason, "TP")
        self.assertEqual(round(t.pnl, 2), D("400.00"))
        self.assertEqual(round(t.r, 2), D("2.00"))

    def test_long_only_ignores_shorts(self):
        c = cfg(prev_candle_mode="three_in_a_row", ema_filter=False, sl_mode="candle", rr=D("2"),
                daily_loss_limit=0, direction="long")
        self.assertEqual(s.backtest(self.mirrored(), c, D(10000), D(0)).trades, [])

    def test_short_paper_exits(self):
        c = Candle(t=0, o=D(100), h=D(111), l=D(89), c=D(100))
        self.assertEqual(s.paper_exit(c, D(110), D(90), side="SHORT"), ("SL", D(110)))  # tie -> SL
        c = Candle(t=0, o=D(93), h=D(111), l=D(89), c=D(100))
        self.assertEqual(s.paper_exit(c, D(110), D(90), side="SHORT"), ("TP", D(90)))   # TP nearer open
        c = Candle(t=0, o=D(112), h=D(113), l=D(105), c=D(106))
        self.assertEqual(s.paper_exit(c, D(110), D(90), side="SHORT"), ("SL", D(112)))  # opens past SL


class TestCandleClock(unittest.TestCase):
    def test_last_closed(self):
        t0 = 1_767_225_600_000  # 00:00 UTC
        self.assertEqual(s.last_closed_open_time(t0 + 4 * H + 1), t0 + 3 * H)
        self.assertEqual(s.last_closed_open_time(t0 + 4 * H), t0 + 3 * H)
        self.assertTrue(s.is_closed(t0, t0 + H))
        self.assertFalse(s.is_closed(t0, t0 + H - 1))


if __name__ == "__main__":
    unittest.main()
