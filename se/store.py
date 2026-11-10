"""
The worker's own SQLite state, data/se_state.db. Nothing here is shared with
the existing bot's data/trades.db.

Tables:
  kv      small settings and markers: mode, paused, last processed candle per
          symbol, daily-loss state, last daily summary, etc.
  trades  one row per signal the worker acted on (paper or live).

Trade status: pending (order being placed) -> open -> closing (close
requested) -> closed, or failed (the entry never filled).
"""
import json
import os
import pathlib
import sqlite3
import threading
import time
from decimal import Decimal
from typing import List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT UNIQUE NOT NULL,     -- se_<SYMBOL>_<signal candle open time>
    symbol TEXT NOT NULL,
    mode TEXT NOT NULL,                 -- paper | live
    status TEXT NOT NULL,               -- pending | open | closing | closed | failed
    candle_time INTEGER NOT NULL,       -- signal candle open time (ms)
    signal_close TEXT, ema TEXT, atr TEXT,
    sl TEXT NOT NULL, tp TEXT NOT NULL, risk_dist TEXT NOT NULL,
    risk_amount TEXT, sized_down INTEGER DEFAULT 0, leverage INTEGER,
    qty TEXT,                           -- filled quantity
    entry_price TEXT, entry_time INTEGER, entry_fee TEXT,
    order_id TEXT, position_id TEXT,
    protected INTEGER DEFAULT 0,        -- live: SL+TP confirmed on Bitunix
    paper_checked_until INTEGER,        -- paper: last candle open time checked for exits
    exit_price TEXT, exit_time INTEGER, exit_reason TEXT,
    fees TEXT, funding TEXT, pnl TEXT, r_mult TEXT,
    note TEXT,
    created_at REAL, updated_at REAL
);
"""

DECIMAL_FIELDS = {"signal_close", "ema", "atr", "sl", "tp", "risk_dist", "risk_amount", "qty",
                  "entry_price", "entry_fee", "exit_price", "fees", "funding", "pnl", "r_mult"}
ACTIVE = ("pending", "open", "closing")


def _to_db(v):
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bool):
        return int(v)
    return v


class Store:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ---------- kv ----------

    def get(self, key: str, default=None):
        with self.lock:
            row = self.conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value):
        with self.lock:
            self.conn.execute("INSERT INTO kv(key, value) VALUES(?, ?) "
                              "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                              (key, json.dumps(value)))
            self.conn.commit()

    # ---------- trades ----------

    def _row(self, cur, row) -> dict:
        d = dict(zip([c[0] for c in cur.description], row))
        for k in DECIMAL_FIELDS:
            if d.get(k) is not None:
                d[k] = Decimal(d[k])
        return d

    def insert_trade(self, **fields) -> int:
        now = time.time()
        fields.update(created_at=now, updated_at=now)
        keys = list(fields)
        with self.lock:
            cur = self.conn.execute(
                f"INSERT INTO trades ({','.join(keys)}) VALUES ({','.join('?' for _ in keys)})",
                [_to_db(fields[k]) for k in keys])
            self.conn.commit()
            return cur.lastrowid

    def update_trade(self, trade_id: int, **fields):
        fields["updated_at"] = time.time()
        sets = ", ".join(f"{k}=?" for k in fields)
        with self.lock:
            self.conn.execute(f"UPDATE trades SET {sets} WHERE id=?",
                              [_to_db(v) for v in fields.values()] + [trade_id])
            self.conn.commit()

    def _query(self, sql: str, args=()) -> List[dict]:
        with self.lock:
            cur = self.conn.execute(sql, args)
            return [self._row(cur, r) for r in cur.fetchall()]

    def trade(self, trade_id: int) -> Optional[dict]:
        rows = self._query("SELECT * FROM trades WHERE id=?", (trade_id,))
        return rows[0] if rows else None

    def trade_by_client_id(self, client_id: str) -> Optional[dict]:
        rows = self._query("SELECT * FROM trades WHERE client_id=?", (client_id,))
        return rows[0] if rows else None

    def active_trades(self) -> List[dict]:
        return self._query("SELECT * FROM trades WHERE status IN ('pending','open','closing') ORDER BY id")

    def active_trade(self, symbol: str) -> Optional[dict]:
        rows = self._query("SELECT * FROM trades WHERE symbol=? AND status IN ('pending','open','closing') "
                           "ORDER BY id DESC LIMIT 1", (symbol,))
        return rows[0] if rows else None

    def closed_trades(self, limit: Optional[int] = None) -> List[dict]:
        sql = "SELECT * FROM trades WHERE status='closed' ORDER BY exit_time DESC, id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return self._query(sql)

    def closed_between(self, start_ms: int, end_ms: int) -> List[dict]:
        return self._query("SELECT * FROM trades WHERE status='closed' AND exit_time>=? AND exit_time<? "
                           "ORDER BY exit_time", (start_ms, end_ms))

    def realized_pnl(self) -> Decimal:
        total = Decimal(0)
        for t in self._query("SELECT pnl FROM trades WHERE status='closed' AND pnl IS NOT NULL"):
            total += t["pnl"]
        return total

    def close(self):
        with self.lock:
            self.conn.close()


def live_owned_symbols(path: str) -> List[str]:
    """
    Symbols where the worker has a live trade that may hold a Bitunix
    position. Read-only; used by the existing bot's reconcile() so it does not
    adopt the worker's positions as orphans. [] if the file doesn't exist.
    """
    if not os.path.isfile(path):
        return []
    try:
        conn = sqlite3.connect(pathlib.Path(path).resolve().as_uri() + "?mode=ro", uri=True)
        try:
            rows = conn.execute("SELECT DISTINCT symbol FROM trades WHERE mode='live' "
                                "AND status IN ('pending','open','closing')").fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()
    except Exception:
        return []
