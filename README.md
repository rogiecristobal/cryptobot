# Bybit Signal Trading Bot

Pastes a signal into Telegram → you reply ✅ → bot places entry/DCA on Bybit →
auto-manages SL/TP, moves SL to breakeven after TP1, and cancels everything for
that asset if SL hits.

## ⚠️ Before you touch mainnet keys

- This has **not** been live-tested against Bybit (no network access to Bybit
  from the environment that built it). Read every file, then test with the
  smallest possible position size first.
- Give the Bybit API key **trade permissions only** — never withdrawal.
- `.env` holds live secrets. Never commit it. `.gitignore` is already set up
  for that.
- If your phone/Termux loses network or the process dies, the bot stops
  watching fills — a position could sit unprotected (SL still resting on the
  exchange is fine; but breakeven-move / cascade-cancel logic requires the
  bot to be running). A small always-on VPS is safer than a phone for this.

## Setup

```bash
pip install -r requirements.txt --break-system-packages   # Termux
# or: pip install -r requirements.txt                     # normal venv

cp .env.example .env
# fill in BYBIT_API_KEY, BYBIT_API_SECRET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
```

Get `TELEGRAM_CHAT_ID` by messaging your bot once, then hitting
`https://api.telegram.org/bot<TOKEN>/getUpdates` and reading `chat.id`.

Run:
```bash
python3 main.py
```

## How it behaves

- **Only your `TELEGRAM_CHAT_ID`** can trigger anything — every other chat is ignored.
- Paste a signal → bot parses it, checks for an existing open position/order on
  that asset (rejects if one exists), computes position size from **risk %**
  (not margin %) against the entry→SL distance, and replies with a summary.
- Reply `✅` within `CONFIRM_TIMEOUT_SECONDS` (default 120s) to actually place
  orders. Anything else, or timeout, and nothing happens.
- No stop loss detected in the signal → **hard rejected**, no trade placed, no exceptions.
- If a DCA level is present, position size is split between the entry order
  and the DCA order per `DCA_SPLIT_RATIO` (default 0.5 = 50/50), sized so that
  if *both* fill, your total risk still lands near your target `RISK_PERCENT`.
- Take-profit size is split evenly across however many TPs the signal has.
- First TP fill → SL is cancelled and replaced at entry price (breakeven).
- SL fill → all remaining orders for that symbol (DCA, unfilled TPs) are cancelled.

## Known simplifications (read before relying on this)

- **Race conditions**: if entry and DCA fill in the same instant, or a TP
  fills right as a DCA fill is being processed, the "cancel + recompute from
  actual position size" pattern in `sync_protective_orders()` is designed to
  self-correct, but it hasn't been stress-tested under real fill timing.
- **Restarts mid-trade**: state is persisted in SQLite (`data/trades.db`), so
  a restart won't forget a position exists — but the bot needs to be running
  to catch fills as they happen. Consider a reconciliation pass on startup
  (checking Bybit's actual open positions/orders against the DB) before
  trusting a restart mid-trade — this isn't built yet.
- **Position mode**: assumes Bybit one-way mode (not hedge mode). If your
  account is in hedge mode, order placement will need `positionIdx` added.
- **No partial-fill handling on the entry order itself** — it assumes entry
  and DCA orders each either fully fill or don't.

## Files

| File | Purpose |
|---|---|
| `config.py` | loads `.env`, all tunables in one place |
| `signal_parser.py` | text → structured signal (same logic as the web formatter) |
| `bybit_client.py` | all Bybit v5 REST/WebSocket calls |
| `state_db.py` | SQLite persistence for open trade state |
| `trade_manager.py` | core lifecycle: stage → confirm → sync protective orders → breakeven → SL-cascade |
| `telegram_bot.py` | Telegram handlers, chat-ID authorization |
| `main.py` | wires everything together and runs |
