"""
Sweep & Engulf (short name "se"): an isolated 4h strategy worker.

It runs inside the existing bot's process (started from main.py), trades
BTCUSDT, ETHUSDT and SOLUSDT on Bitunix futures from its own small budget,
and never touches orders or positions it did not create.

Modules:
  config.py        SE_* settings from .env
  strategy.py      pure strategy maths (no network): indicators, signal, sizing,
                   daily loss limit, paper exits, single-symbol backtest
  market.py        public Bitunix market data (klines, trading pairs)
  store.py         the worker's own SQLite state (data/se_state.db)
  exchange.py      the worker's private Bitunix calls (live mode only)
  worker.py        the background loop: candles, entries, protection, exits
  telegram_cmds.py /se_* commands, registered on the existing Telegram bot
  replay.py        python -m se.replay --symbol BTCUSDT --from 2025-01-01
"""
