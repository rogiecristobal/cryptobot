"""
Persists trade state so a bot restart doesn't lose track of open positions,
which orders belong to which asset, or whether breakeven has already fired.
"""
import sqlite3
import json
import time
import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    symbol TEXT PRIMARY KEY,
    position TEXT NOT NULL,
    status TEXT NOT NULL,              -- 'pending_confirm' | 'active' | 'closed'
    entry_order_id TEXT,
    dca_order_id TEXT,
    sl_order_id TEXT,
    tp_order_ids TEXT,                 -- JSON list
    entry_price REAL,
    sl_price REAL,
    original_sl_price REAL,
    tp_prices TEXT,                    -- JSON list
    breakeven_moved INTEGER DEFAULT 0,
    raw_signal TEXT,
    created_at REAL,
    updated_at REAL
);
"""


class StateDB:
    def __init__(self, path=config.DB_PATH):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute(SCHEMA)
        self.conn.commit()

    def get(self, symbol: str):
        row = self.conn.execute("SELECT * FROM trades WHERE symbol=?", (symbol,)).fetchone()
        if not row:
            return None
        cols = [d[0] for d in self.conn.execute("SELECT * FROM trades LIMIT 0").description]
        return dict(zip(cols, row))

    def upsert(self, symbol: str, **fields):
        existing = self.get(symbol)
        now = time.time()
        if existing:
            sets = ", ".join(f"{k}=?" for k in fields)
            vals = list(fields.values()) + [now, symbol]
            self.conn.execute(f"UPDATE trades SET {sets}, updated_at=? WHERE symbol=?", vals)
        else:
            fields.setdefault("status", "pending_confirm")
            keys = ["symbol"] + list(fields.keys()) + ["created_at", "updated_at"]
            vals = [symbol] + list(fields.values()) + [now, now]
            placeholders = ",".join("?" for _ in keys)
            self.conn.execute(f"INSERT INTO trades ({','.join(keys)}) VALUES ({placeholders})", vals)
        self.conn.commit()

    def delete(self, symbol: str):
        self.conn.execute("DELETE FROM trades WHERE symbol=?", (symbol,))
        self.conn.commit()

    def all_active(self):
        rows = self.conn.execute("SELECT symbol FROM trades WHERE status='active'").fetchall()
        return [r[0] for r in rows]

    @staticmethod
    def dumps(obj) -> str:
        return json.dumps(obj)

    @staticmethod
    def loads(s: str):
        return json.loads(s) if s else []
