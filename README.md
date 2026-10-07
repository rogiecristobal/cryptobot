# Bitunix Signal Trading Bot

Pastes a signal into Telegram → you reply ✅ → bot places entry/DCA on Bitunix (USDT-M futures) →
auto-manages SL/TP, moves SL to breakeven after TP1, and cancels everything for
that asset if SL hits.

## ⚠️ Before you touch mainnet keys

- The Bitunix client was live-tested with tiny XRP orders (limit + SL, SL
  amend, cancel, market entry with SL/TP, SL move, flash close, WebSocket
  events). Bitunix has **no testnet**, so still start with small sizes.
- Give the Bitunix API key **futures trade permission only** — never
  withdrawal. If you set an IP whitelist, add the bot's IP.
- One-way position mode is recommended. Hedge mode also works (verified
  live), as long as you never hold a long and a short on the same symbol.
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
# fill in BITUNIX_API_KEY, BITUNIX_API_SECRET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
```

Get `TELEGRAM_CHAT_ID` by messaging your bot once, then hitting
`https://api.telegram.org/bot<TOKEN>/getUpdates` and reading `chat.id`.

Run:
```bash
python3 main.py
```

## Updating on Termux

`git pull` updates the code, but **not `.env`**: it is gitignored, so any
setting change has to be made by hand on the phone.

```bash
# 1. Stop the running bot (Ctrl+C). Two copies polling Telegram at once fail
#    with "Conflict: terminated by other getUpdates request".
cd ~/cryptobot            # wherever you cloned it
git pull
pip install -r requirements.txt --break-system-packages   # only if requirements.txt changed
# 2. Apply any .env changes listed below
# 3. Start it again
python3 main.py
```

Open trades survive the restart: on startup `reconcile()` re-checks every
trade against Bitunix and re-applies its SL and TP.

### `.env` changes by version

- **Margin mode defaults to cross.** If your `.env` still has
  `DEFAULT_MARGIN_MODE=ISOLATED`, set it to `CROSS` (or delete the line):
  ```bash
  sed -i 's/^DEFAULT_MARGIN_MODE=.*/DEFAULT_MARGIN_MODE=CROSS/' .env
  ```
  This only affects new trades. Bitunix can't change the margin mode of a
  coin that has an open position or order, so those stay isolated until closed.
- **Telegram bot token no longer logged.** Older versions wrote the bot token
  into `logs/bot.log` on every request. Clear the old log with
  `: > logs/bot.log`, and if that file was ever shared or backed up, revoke
  the token in @BotFather and put the new one in `TELEGRAM_BOT_TOKEN`.

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
- **Only TP1 goes on Bitunix**, for the whole position. It is attached to every
  opening order (market, limit, DCA) like the SL. `/tp SYMBOL p1 [p2 ...]` moves it
  (TP1 = p1), and the reply says whether Bitunix actually accepted it. Further TPs
  are only kept in the bot's state. The watchdog puts TP1 back if it goes missing
  or is changed on Bitunix, so change it with `/tp` rather than the Bitunix app.
- First TP fill → SL is cancelled and replaced at entry price (breakeven).
- SL fill → all remaining orders for that symbol (DCA, unfilled TPs) are cancelled.
- The SL is attached to every opening order (market, limit entry, DCA), so Bitunix
  creates its TP/SL order on the fill itself even if the bot is offline at that moment.
- A watchdog re-checks every trade every `WATCHDOG_INTERVAL_SECONDS` (default 30s):
  a position with no SL gets it re-applied (with a Telegram alert), fills or
  closes missed while the WebSocket was down are processed, an SL on Bitunix
  that differs from the bot's is reset, and a dead Bitunix WebSocket is restarted.
- **Trailing stop is bot-side.** Bitunix has no trailing-stop API, so every
  `TRAILING_INTERVAL_SECONDS` (default 5s) the bot reads mark price and ratchets
  the SL to peak − distance. It only trails while the bot is running; if the
  bot goes offline, the last SL it set stays on Bitunix.

## Known simplifications (read before relying on this)

- **Race conditions**: if entry and DCA fill in the same instant, or a TP
  fills right as a DCA fill is being processed, the "cancel + recompute from
  actual position size" pattern in `sync_protective_orders()` is designed to
  self-correct, but it hasn't been stress-tested under real fill timing.
- **Restarts mid-trade**: state is persisted in SQLite (`data/trades.db`). On
  startup `reconcile()` compares every tracked trade with Bitunix: open positions
  get their SL re-applied, limit entries still waiting to fill are kept, and
  trades that closed while offline are cleaned up.
- **Position mode**: fills are classified by order side (same side as the
  trade = entry/DCA, opposite = close). Verified in hedge mode too, but holding
  a long and a short on the same symbol would confuse it.
- **TP vs SL labels**: Bitunix fill events don't say which trigger fired, so a
  closing market fill is labelled TP or SL by which price it landed closest to.
- **Attached TP/SL**: on fill, Bitunix turns the SL (and TP) attached to an
  entry into separate TP/SL orders sized to that fill. Moving the SL updates
  all of them; the watchdog tops up coverage if any part of the position has
  no SL.
- **No partial-fill handling on the entry order itself** — it assumes entry
  and DCA orders each either fully fill or don't.

## Files

| File | Purpose |
|---|---|
| `config.py` | loads `.env`, all tunables in one place |
| `signal_parser.py` | text → structured signal (same logic as the web formatter) |
| `bitunix_client.py` | all Bitunix futures REST/WebSocket calls; returns normalized dicts |
| `state_db.py` | SQLite persistence for open trade state |
| `trade_manager.py` | core lifecycle: stage → confirm → sync protective orders → breakeven → SL-cascade |
| `telegram_bot.py` | Telegram handlers, chat-ID authorization |
| `main.py` | wires everything together and runs |
