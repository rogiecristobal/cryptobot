"""
Worker flow tests with a fake market and a fake Bitunix — no network.
    python -m unittest discover -s tests -v
"""
import os
import tempfile
import unittest
from decimal import Decimal as D
from unittest import mock

# The live path reads the bot's own config (LEVERAGE_TIERS, DEFAULT_MARGIN_MODE),
# which needs these to import. Dummy values; nothing is sent anywhere.
for k, v in {"BITUNIX_API_KEY": "test", "BITUNIX_API_SECRET": "test", "TELEGRAM_BOT_TOKEN": "1:test",
             "TELEGRAM_CHAT_ID": "1", "EXCHANGE": "bitunix"}.items():
    os.environ.setdefault(k, v)

from se import strategy as s
from se.config import SEConfig
from se.store import Store
from se.strategy import Candle
from se.worker import SEWorker

H = s.CANDLE_MS
T0 = 1_767_225_600_000  # 2026-01-01 00:00 UTC


def flat(n, start=T0, price=100):
    return [Candle(t=start + i * H, o=D(price), h=D(price + 1), l=D(price - 1), c=D(price)) for i in range(n)]


def with_signal(n=30):
    """n-2 flat candles, a bullish candle, then a sweep & engulf on the last one."""
    cs = flat(n - 2)
    t = cs[-1].t
    cs.append(Candle(t=t + H, o=D(100), h=D(102), l=D(99.5), c=D(101.5)))   # bullish prev
    cs.append(Candle(t=t + 2 * H, o=D(101.5), h=D(104), l=D(99), c=D(103)))  # sweep 99<99.5, close 103>102
    return cs   # signal: SL (candle) 99, risk 4, TP 2R = 111


class FakeMarket:
    def __init__(self, candles, price=D(103)):
        self.candles = list(candles)
        self.price = price
        self.instrument = {"price_step": D("0.1"), "qty_step": D("0.001"), "min_qty": D("0.001"), "status": "OPEN"}

    def history(self, session, symbol, count=None, since_ms=None, now_ms=None):
        return [c for c in self.candles if now_ms is None or c.t + H <= now_ms][-(count or 10**9):]

    def klines(self, session, symbol, end_ms=None, limit=200, interval="4h"):
        return self.candles[-limit:]

    def patch(self):
        return [mock.patch("se.market.fetch_history", self.history),
                mock.patch("se.market.fetch_klines", self.klines),
                mock.patch("se.market.fetch_instrument", lambda *a, **k: self.instrument),
                mock.patch("se.market.fetch_last_price", lambda *a, **k: self.price)]


class FakeExchange:
    def __init__(self):
        self.foreign = []
        self.available = D(1000)
        self.pos = None             # normalized position dict, or None
        self.orders = []
        self.protect_fails = False
        self.closed = False
        self.history = None
        self.calls = []

    def sync_time(self): return 0.0
    def available_usdt(self): return self.available
    def foreign_activity(self, symbol): return list(self.foreign)
    def max_leverage(self, symbol): return 100
    def prepare_symbol(self, symbol, lev, margin): self.calls.append(("prepare", symbol, lev, margin))
    def raw_positions(self):
        return [{"symbol": self.pos["symbol"], "positionId": self.pos["position_id"]}] if self.pos else []
    def position(self, symbol): return self.pos if self.pos and self.pos["symbol"] == symbol else None
    def find_order(self, symbol, client_id): return {"order_id": "o1", "status": "FILLED"}
    def fill_details(self, symbol, order_id): return {"price": D("103.1"), "qty": self.pos["size_d"], "fee": D("0.05")}
    def history_position(self, symbol, position_id): return self.history
    def cancel_position_tpsl(self, symbol, position_id): return 0

    def place_entry(self, symbol, qty, sl, tp, client_id):
        self.calls.append(("place", symbol, qty, sl, tp, client_id))
        self.pos = {"symbol": symbol, "position_id": "p1", "size": float(qty), "size_d": qty,
                    "avg_price": 103.1, "stop_loss": float(sl), "take_profit": float(tp),
                    "sl_prices": [float(sl)], "tp_prices": [float(tp)]}
        return "o1"

    def ensure_tpsl(self, symbol, sl, tp):
        self.calls.append(("ensure_tpsl", symbol, sl, tp))
        if self.protect_fails:
            raise RuntimeError("Bitunix said no")
        self.pos.update(stop_loss=float(sl), take_profit=float(tp))

    def close_position(self, symbol):
        self.calls.append(("close", symbol))
        self.closed = True
        self.pos = None
        return D(1)


class WorkerTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "se_state.db"))
        self.messages = []
        self.now = 0
        self.ex = FakeExchange()
        self.cfg = SEConfig(symbols=["BTCUSDT"], prev_candle_mode="same", ema_filter=False, sl_mode="candle",
                            rr=D(2), budget_usdt=D(100), risk_pct=D(3), fee_pct=D(0), live_confirm="I_ACCEPT_THE_RISK")
        self.market = FakeMarket(with_signal())
        self.patches = self.market.patch() + [mock.patch("se.worker.time.sleep", lambda *_: None),
                                              mock.patch("se.worker.PROTECT_WAIT_SECONDS", 0)]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.store.close()
        self.tmp.cleanup()

    def worker(self, mode="paper"):
        self.store.set("mode", mode)
        w = SEWorker(self.cfg, self.store, self.messages.append, clock=lambda: self.now / 1000,
                     exchange_factory=lambda: self.ex, session=object())
        return w

    def at_close_of_last(self, minutes_after=1):
        self.now = self.market.candles[-1].t + H + minutes_after * 60_000

    def text(self):
        return "\n".join(self.messages)


class TestPaperFlow(WorkerTestBase):
    def test_signal_entry_and_tp_exit(self):
        self.at_close_of_last()
        w = self.worker()
        w.startup()
        w.tick()
        t = self.store.active_trade("BTCUSDT")
        self.assertIsNotNone(t)
        self.assertEqual((t["mode"], t["status"]), ("paper", "open"))
        self.assertEqual((t["sl"], t["tp"]), (D(99), D(111)))
        # risk $3 / 4 = 0.75 qty, cap 99.5/103 = 0.966 -> not capped
        self.assertEqual(t["qty"], D("0.750"))
        self.assertTrue(all(m.startswith("[S&E] ") for m in self.messages))
        self.assertIn("Signal (PAPER)", self.text())
        self.assertIn("Entry filled (paper)", self.text())

        # The same candle is not acted on twice.
        w.tick()
        self.assertEqual(len([m for m in self.messages if "Signal (" in m]), 1)

        # Next candle runs to the TP.
        last = self.market.candles[-1]
        self.market.candles.append(Candle(t=last.t + H, o=D(103), h=D(112), l=D(102), c=D(110)))
        self.at_close_of_last()
        w.tick()
        t = self.store.closed_trades()[0]
        self.assertEqual((t["exit_reason"], t["exit_price"]), ("TP", D(111)))
        self.assertEqual(t["pnl"], (D(111) - D(103)) * D("0.75"))
        self.assertEqual(t["r_mult"], D(2))
        self.assertIn("Trade closed (PAPER)", self.text())
        eq, _, _ = w.equity()
        self.assertEqual(eq, D(106))

    def test_too_late(self):
        self.at_close_of_last(minutes_after=20)
        w = self.worker()
        w.startup()
        w.tick()
        self.assertIsNone(self.store.active_trade("BTCUSDT"))
        self.assertIn("too late", self.text())

    def test_paused(self):
        self.at_close_of_last()
        w = self.worker()
        w.startup()
        w.set_paused(True)
        w.tick()
        self.assertIsNone(self.store.active_trade("BTCUSDT"))
        self.assertIn("paused", self.text())

    def test_daily_loss_limit_blocks_and_resumes(self):
        self.at_close_of_last()
        w = self.worker()
        w.startup()
        day_ms = self.now - self.now % (24 * H // 4)
        w._save_loss_state(s.LossState(count=2, day=s.utc_day(self.now), blocked_day=s.utc_day(self.now)))
        w.tick()
        self.assertIsNone(self.store.active_trade("BTCUSDT"))
        self.assertIn("daily loss limit", self.text())
        self.now = day_ms + 24 * 3600 * 1000 + 1000  # next UTC day
        w._maybe_resume_message(self.now)
        self.assertIn("trading resumed", self.text())

    def test_two_losses_reach_the_limit(self):
        self.at_close_of_last()
        w = self.worker()
        w.startup()
        w.tick()
        t = self.store.active_trade("BTCUSDT")
        w._finish_close(t, D(99), self.now, "SL", D(-3), D(0), D(0))
        self.assertNotIn("Daily loss limit reached", self.text())
        t2_id = self.store.insert_trade(client_id="se_X_1", symbol="ETHUSDT", mode="paper", status="open",
                                        candle_time=0, sl=D(1), tp=D(3), risk_dist=D(1), qty=D(1),
                                        entry_price=D(2), entry_time=self.now, entry_fee=D(0))
        w._finish_close(self.store.trade(t2_id), D(1), self.now + 1, "SL", D(-1), D(0), D(0))
        self.assertIn("Daily loss limit reached", self.text())

    def test_manual_close_paper(self):
        self.at_close_of_last()
        w = self.worker()
        w.startup()
        w.tick()
        self.market.price = D(105)
        out = w.close_trades("btc")
        self.assertIn("paper trade closed", out)
        self.assertEqual(self.store.closed_trades()[0]["exit_reason"], "MANUAL")


class TestLiveFlow(WorkerTestBase):
    def test_live_entry_protected(self):
        self.at_close_of_last()
        w = self.worker("live")
        w.startup()
        w.tick()
        t = self.store.active_trade("BTCUSDT")
        self.assertEqual((t["mode"], t["status"], t["protected"]), ("live", "open", 1))
        self.assertEqual(t["entry_price"], D("103.1"))
        place = [c for c in self.ex.calls if c[0] == "place"][0]
        self.assertEqual(place[5], f"se_BTCUSDT_{self.market.candles[-1].t}")   # fixed client order ID
        self.assertEqual((place[3], place[4]), (D(99), D(111)))                # SL/TP in the same request
        prep = [c for c in self.ex.calls if c[0] == "prepare"][0]
        self.assertIn(prep[3], ("CROSS", "ISOLATED"))
        self.assertIn("Entry filled (LIVE)", self.text())

    def test_foreign_position_skips(self):
        self.ex.foreign = ["open LONG position of 0.01"]
        self.at_close_of_last()
        w = self.worker("live")
        w.startup()
        w.tick()
        self.assertIsNone(self.store.active_trade("BTCUSDT"))
        self.assertFalse(any(c[0] == "place" for c in self.ex.calls))
        self.assertIn("foreign position", self.text())

    def test_balance_check(self):
        self.ex.available = D("0.5")
        self.at_close_of_last()
        w = self.worker("live")
        w.startup()
        w.tick()
        self.assertFalse(any(c[0] == "place" for c in self.ex.calls))
        self.assertIn("not enough cash on Bitunix", self.text())

    def test_missing_tpsl_is_replaced(self):
        orig = self.ex.place_entry

        def place_without_tpsl(*a, **k):
            oid = orig(*a, **k)
            self.ex.pos.update(stop_loss=0.0, take_profit=0.0)
            return oid
        self.ex.place_entry = place_without_tpsl
        self.at_close_of_last()
        w = self.worker("live")
        w.startup()
        w.tick()
        self.assertTrue(any(c[0] == "ensure_tpsl" for c in self.ex.calls))
        self.assertEqual(self.store.active_trade("BTCUSDT")["protected"], 1)
        self.assertIn("placed and confirmed", self.text())

    def test_unprotectable_position_is_closed(self):
        orig = self.ex.place_entry

        def place_without_tpsl(*a, **k):
            oid = orig(*a, **k)
            self.ex.pos.update(stop_loss=0.0, take_profit=0.0)
            return oid
        self.ex.place_entry = place_without_tpsl
        self.ex.protect_fails = True
        self.at_close_of_last()
        w = self.worker("live")
        w.startup()
        w.tick()
        self.assertTrue(self.ex.closed)
        self.assertIn("Position CLOSED at market", self.text())

    def test_live_close_recorded_from_history(self):
        self.at_close_of_last()
        w = self.worker("live")
        w.startup()
        w.tick()
        t = self.store.active_trade("BTCUSDT")
        # TP fires on Bitunix while the worker runs.
        self.ex.pos = None
        self.ex.history = {"close_price": D(111), "entry_price": D("103.1"), "fee": D("0.10"),
                           "funding": D("-0.02"), "realized_pnl": (D(111) - D("103.1")) * t["qty"],
                           "qty": t["qty"], "close_time": self.now + 3600_000, "raw": {}}
        w._last_live_check = 0
        w.tick()
        c = self.store.closed_trades()[0]
        self.assertEqual(c["exit_reason"], "TP")
        self.assertEqual(c["pnl"], (D(111) - D("103.1")) * t["qty"] - D("0.10") - D("0.02"))
        self.assertIn("Trade closed (LIVE)", self.text())

    def test_live_mode_locked_without_confirm_phrase(self):
        self.cfg.live_confirm = ""
        w = self.worker("paper")
        self.assertIn("locked", w.mode_switch_problem("live"))


if __name__ == "__main__":
    unittest.main()
