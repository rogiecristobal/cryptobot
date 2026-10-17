# Bitunix Signal Trading Bot

Pastes a signal into Telegram → you reply ✅ → bot places entry/DCA on Bitunix (USDT-M futures) →
auto-manages SL/TP, steps the SL up as price reaches each TP, and cancels everything for
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
setting change has to be made by hand on the phone. The update notes below
say what each update needs.

```bash
# 1. Note your open trades first: send /status to the bot in Telegram.
# 2. Stop the running bot (Ctrl+C). Two copies polling Telegram at once fail
#    with "Conflict: terminated by other getUpdates request".
cd ~/cryptobot            # wherever you cloned it
git log --oneline -1      # the version you're on now — note the 7-character id
git pull
pip install -r requirements.txt --break-system-packages   # only if requirements.txt changed
# 3. Apply the update notes below for every update NEWER than the id you noted
# 4. Start it again
python3 main.py
# 5. Check the "reconciled" messages in Telegram, or send /status
```

Open trades survive the restart: on startup `reconcile()` re-checks every
trade against Bitunix and re-applies its SL and TP. Database changes are
applied automatically on startup — there is nothing to migrate by hand.

### Update notes (newest first)

- **Default risk 2%, trailing and editing fixes** (this update).
  - The default `RISK_PERCENT` is now **2%**, but your `.env` overrides it.
    Check yours with `grep RISK_PERCENT .env`. To use 2%:
    ```bash
    sed -i 's/^RISK_PERCENT=.*/RISK_PERCENT=2.0/' .env
    ```
  - `/trail <SYMBOL> off` now leaves the SL where it is. It used to move it
    back to the original SL, giving back profit already locked in.
  - Trailing now works on low-priced coins. The distance used to be rounded
    to 2 decimals, which switched trailing off on coins priced like PEPE.
  - Before you tap Confirm, `/sl`, `/tp`, `/dca` and `/entry` send a fresh
    trade card with the change, re-checked and re-sized. `/entry` works again.
  - The "Bitunix connection" alert now says if Bitunix rejected the login,
    and repeats at most every 10 minutes.
- **Safety fixes** (`da752e4`). Optional `.env` addition:
  `MAX_RISK_PERCENT=5.0` (the default if missing) — no trade can risk more.
  - Only **one copy** of the bot can run. A second copy on the same device
    stops at once; one on another device stops after a ~10s check at startup
    and says so in Telegram. Startup is ~10s slower because of that check.
  - Confirmation cards from before this update no longer work — send the
    command again.
  - The trade card now shows the risk of the size **actually ordered**.
    Sizes are rounded down, never up. If Bitunix's minimum order size pushes
    a trade above `MAX_RISK_PERCENT`, it is rejected.
  - A failed close is no longer reported as closed: the bot keeps tracking
    the trade and retries every 30s.

- **TPs move the SL instead of taking profit** (`42d7886`). No `.env` change.
  - Only the **last** TP closes the position; earlier TPs only move the SL.
  - If you were on `2b8f590` (which split the position across TPs), open
    trades have their partial TP orders **cancelled on the first restart**,
    so nothing closes before the last TP. Their SL and last TP stay on
    Bitunix. A failed cancel shows a Telegram alert and is retried every 30s.
  - If you were on an older version, open trades with several TPs saved get
    only the last TP on Bitunix; the earlier ones become SL checkpoints.
  - Check after the restart: in the Bitunix app each open position should
    show one TP/SL with the last TP — no partial TPs.
- **Split TPs and SL step-up** (`2b8f590`). Replaced by `42d7886` above —
  nothing to do for it.
- **Margin mode defaults to cross** (`55edff4`). If your `.env` still has
  `DEFAULT_MARGIN_MODE=ISOLATED`, set it to `CROSS` (or delete the line):
  ```bash
  sed -i 's/^DEFAULT_MARGIN_MODE=.*/DEFAULT_MARGIN_MODE=CROSS/' .env
  ```
  This only affects new trades. Bitunix can't change the margin mode of a
  coin that has an open position or order, so those stay isolated until closed.
- **Telegram bot token no longer logged** (`56ae98f`). Older versions wrote
  the bot token into `logs/bot.log` on every request. Clear the old log with
  `: > logs/bot.log`, and if that file was ever shared or backed up, revoke
  the token in @BotFather and put the new one in `TELEGRAM_BOT_TOKEN`.

## How it behaves

- **Only your `TELEGRAM_CHAT_ID`** can trigger anything — every other chat is ignored.
- Paste a signal → bot parses it, checks for an existing open position/order on
  that asset (rejects if one exists), computes position size from **risk %**
  (not margin %) against the entry→SL distance, and replies with a summary.
- Reply `✅` within `CONFIRM_TIMEOUT_SECONDS` (default 120s) to actually place
  orders. Anything else, or timeout, and nothing happens.
- Before you confirm, `/sl`, `/tp`, `/dca` or `/entry` on that symbol sends a
  fresh card with the change, re-checked and re-sized, and retires the old one.
- No stop loss detected in the signal → **hard rejected**, no trade placed, no exceptions.
- If a DCA level is present, position size is split between the entry order
  and the DCA order per `DCA_SPLIT_RATIO` (default 0.5 = 50/50), sized so that
  if *both* fill, your total risk still lands near your target `RISK_PERCENT`.
- The card shows the risk of the quantities actually ordered. Quantities are
  rounded down; Bitunix's minimum order size can still push risk above target,
  and anything above `MAX_RISK_PERCENT` (default 5%) is rejected.
- Each card's buttons only act on the trade or change that card shows. If a
  newer card for the same symbol replaced it, they do nothing.
- **Only the last TP takes profit.** With TPs `71000 72000 73000` (in `/place …
  TP 71000 72000 73000` or `/tp BTC 71000 72000 73000`), TP3 is a real TP on
  Bitunix that closes the whole position. It is attached to every opening
  order (market, limit, DCA) like the SL, so it works even if the bot is offline.
  TPs are sorted nearest-first, so their order in the command doesn't matter.
- **Earlier TPs are SL checkpoints**: nothing is closed there. When mark price
  reaches TP1 the SL moves to entry; at TP2 it moves to TP1, and so on. It
  never loosens an SL, so if the trailing stop is already past that level it
  stays. You get a Telegram message for each checkpoint. Like the trailing
  stop, this is bot-side (checked every `TRAILING_INTERVAL_SECONDS`) and only
  works while the bot runs; on restart it catches up with the current price.
- `/tp` replaces the TPs still ahead (reached ones are kept), and the reply says
  whether Bitunix accepted it. The watchdog puts back a last TP that goes
  missing or is changed on Bitunix, so change TPs with `/tp`, not the Bitunix app.
- SL fill → all remaining orders for that symbol (DCA, unfilled TPs) are cancelled.
- The SL is attached to every opening order (market, limit entry, DCA), so Bitunix
  creates its TP/SL order on the fill itself even if the bot is offline at that moment.
- A watchdog re-checks every trade every `WATCHDOG_INTERVAL_SECONDS` (default 30s):
  a position with no SL gets it re-applied (with a Telegram alert), fills or
  closes missed while the WebSocket was down are processed, an SL on Bitunix
  that differs from the bot's is reset, and a dead Bitunix WebSocket is restarted.
- **Trailing stop is bot-side.** Bitunix has no trailing-stop API, so every
  `TRAILING_INTERVAL_SECONDS` (default 5s) the bot reads mark price and ratchets
  the SL to peak − distance. `/trail <SYMBOL> off` stops trailing and leaves
  the SL where it is. It only trails while the bot is running; if the
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
  close is labelled from the TP/SL push, or by which price it landed closest to.
- **Partial closes you make in the Bitunix app** look like an SL chunk firing:
  the bot closes the rest of the position. Use `/close` instead.
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
