"""
Persists trade state so a bot restart doesn't lose track of open positions,
which orders belong to which asset, or whether breakeven has already fired.
"""
import sqlite3
import json
import logging
import threading
import time
import config

log = logging.getLogger("state_db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    symbol TEXT PRIMARY KEY,
    position TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_order_id TEXT,
    dca_order_id TEXT,
    sl_order_id TEXT,
    tp_order_ids TEXT,
    entry_price REAL,
    sl_price REAL,
    original_sl_price REAL,
    tp_prices TEXT,
    breakeven_moved INTEGER DEFAULT 0,
    raw_signal TEXT,
    created_at REAL,
    updated_at REAL
);
"""


class StateDB:
    def __init__(self, path=config.DB_PATH):
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(SCHEMA)
        self._integrity_check()
        self._migrate()

    def _integrity_check(self):
        try:
            row = self.conn.execute("PRAGMA integrity_check").fetchone()
            if row and row[0] != "ok":
                log.warning("DB integrity check failed: %s", row[0])
        except Exception as e:
            log.warning("Could not check DB integrity: %s", e)

    def _migrate(self):
        migrations = [
            ("ALTER TABLE trades ADD COLUMN dca_price REAL", "dca_price"),
            ("ALTER TABLE trades ADD COLUMN filled_tp_prices TEXT DEFAULT '[]'", "filled_tp_prices"),
            ("ALTER TABLE trades ADD COLUMN manual_tp_count INTEGER DEFAULT 0", "manual_tp_count"),
            ("ALTER TABLE trades ADD COLUMN breakeven_prompt_msg_id INTEGER", "breakeven_prompt_msg_id"),
            ("ALTER TABLE trades ADD COLUMN trailing_distance REAL", "trailing_distance"),
            ("ALTER TABLE trades ADD COLUMN trailing_r_mult REAL", "trailing_r_mult"),
            # 1 once a fill has actually opened the position. Until then a limit
            # entry is still resting, so "size 0" does NOT mean "position closed".
            ("ALTER TABLE trades ADD COLUMN position_opened INTEGER DEFAULT 0", "position_opened"),
            ("ALTER TABLE trades ADD COLUMN risk_amount REAL", "risk_amount"),
            ("ALTER TABLE trades ADD COLUMN entry_qty REAL", "entry_qty"),
            # Best mark price since the bot-side trailing stop engaged (Bitunix has no native one).
            ("ALTER TABLE trades ADD COLUMN trailing_peak REAL", "trailing_peak"),
        ]
        for sql, col in migrations:
            try:
                self.conn.execute(sql)
                log.info("Schema migration: added column %s", col)
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    pass
                else:
                    log.warning("Schema migration error for %s: %s", col, e)
        self.conn.commit()

    def get(self, symbol: str):
        with self.lock:
            row = self.conn.execute("SELECT * FROM trades WHERE symbol=?", (symbol,)).fetchone()
            if not row:
                return None
            cols = [d[0] for d in self.conn.execute("SELECT * FROM trades LIMIT 0").description]
            return dict(zip(cols, row))

    def get_many(self, symbols: list):
        if not symbols:
            return {}
        with self.lock:
            placeholders = ",".join("?" for _ in symbols)
            rows = self.conn.execute(
                f"SELECT * FROM trades WHERE symbol IN ({placeholders})", symbols
            ).fetchall()
            if not rows:
                return {}
            cols = [d[0] for d in self.conn.execute("SELECT * FROM trades LIMIT 0").description]
            return {row[0]: dict(zip(cols, row)) for row in rows}

    def upsert(self, symbol: str, **fields):
        with self.lock:
            existing = self.conn.execute("SELECT symbol FROM trades WHERE symbol=?", (symbol,)).fetchone()
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
        with self.lock:
            self.conn.execute("DELETE FROM trades WHERE symbol=?", (symbol,))
            self.conn.commit()

    def all_active(self):
        with self.lock:
            rows = self.conn.execute("SELECT symbol FROM trades WHERE status='active'").fetchall()
            return [r[0] for r in rows]

    def close(self):
        with self.lock:
            self.conn.close()

    @staticmethod
    def dumps(obj) -> str:
        return json.dumps(obj)

    @staticmethod
    def loads(s: str):
        return json.loads(s) if s else []
