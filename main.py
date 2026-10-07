"""
Entrypoint. Runs the Telegram bot (polling) and the Bitunix private WebSocket
(order/position fills) side by side in one process.
"""
import asyncio
import logging
import os
import ssl
import sys
import threading
import time

import requests
import urllib3.exceptions

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import config
from bitunix_client import BitunixClient
from state_db import StateDB
from trade_manager import TradeManager
from telegram_bot import build_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("logs/bot.log")],
)
log = logging.getLogger("main")
# httpx logs every request URL at INFO, and Telegram URLs contain the bot token.
logging.getLogger("httpx").setLevel(logging.WARNING)

_ssl_path = os.environ.get("SSL_CERT_FILE", "not set")
log.info("SSL CA bundle: %s (exists=%s)", _ssl_path,
         os.path.isfile(_ssl_path) if os.environ.get("SSL_CERT_FILE") else "N/A")
log.info("OpenSSL default verify paths: cafile=%s", ssl.get_default_verify_paths().cafile)


class ManagerRef:
    tm = None


def _fire_and_forget(coro, loop):
    """Schedule a coroutine on the event loop and log any unhandled exceptions."""
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    future.add_done_callback(lambda f: log.error("Unhandled exception in background task: %s", f.exception()) if f.exception() else None)


async def _handle_manual_tp(symbol: str, trade_manager: TradeManager, tg_app, db: StateDB):
    """Async handler for a detected manual TP fill — notifies and prompts for breakeven on TP1."""
    count = trade_manager.handle_manual_tp_fill(symbol)
    if count != 1:
        return

    keyboard = [
        [
            InlineKeyboardButton("✅ Yes — move SL to entry", callback_data=f"breakeven_yes:{symbol}"),
            InlineKeyboardButton("❌ No", callback_data=f"breakeven_no:{symbol}"),
        ]
    ]
    try:
        msg = await tg_app.bot.send_message(
            chat_id=config.TELEGRAM_CHAT_ID,
            text=f"🎯 TP1 hit on {symbol}\n\nMove SL to entry price?",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        log.error("Failed to send breakeven prompt for %s: %s", symbol, e)
        return

    db.upsert(symbol, breakeven_prompt_msg_id=msg.message_id)

    async def _auto_breakeven():
        await asyncio.sleep(config.BREAKEVEN_TIMEOUT_SECONDS)
        state = db.get(symbol)
        if state and state.get("breakeven_prompt_msg_id") == msg.message_id:
            ok = await asyncio.to_thread(trade_manager.apply_breakeven, symbol)
            text = (f"⏱️ Timeout — SL auto-moved to entry for {symbol}." if ok
                    else f"⏱️ Timeout — tried to move SL to entry for {symbol} but it FAILED. Check Bitunix.")
            try:
                await tg_app.bot.edit_message_text(
                    chat_id=config.TELEGRAM_CHAT_ID,
                    message_id=msg.message_id,
                    text=text,
                )
            except Exception:
                pass

    asyncio.get_event_loop().create_task(_auto_breakeven())


FILL_STATUSES = ("FILLED", "PART_FILLED", "PART_FILLED_CANCELED")


async def _handle_fill_async(item: dict, state: dict | None, trade_manager: TradeManager, tg_app, db: StateDB):
    """Async wrapper for fill handling, offloads blocking work via to_thread.
    item is a normalized order event from BitunixClient."""
    symbol = item["symbol"]
    status = item["status"]
    order_id = item["order_id"]
    if status not in FILL_STATUSES:
        return

    state = state or db.get(symbol)
    if not state:
        log.info("Fill for untracked symbol %s (%s %s) — ignored", symbol, order_id, status)
        return

    # Bitunix order pushes carry no reduceOnly flag or trigger direction, so a
    # fill is classified by its side: same side as the trade opens/adds to it,
    # opposite side closes it. Assumes one-way position mode (checked at startup).
    opening_side = "BUY" if state["position"] == "LONG" else "SELL"
    if order_id in (state["entry_order_id"], state["dca_order_id"]) or item["side"] == opening_side:
        # Any position-increasing fill on a tracked symbol. Matching on side
        # too covers a fill that lands before confirm() has stored the order
        # ID. Partial fills count: the filled part needs its SL now.
        log.info("Entry/DCA fill: %s %s (%s)", symbol, order_id, status)
        await asyncio.to_thread(trade_manager.handle_entry_or_dca_fill, symbol)
    elif status != "FILLED":
        return
    elif state["status"] == "active":
        if item["order_type"] == "MARKET":
            # TP/SL triggers execute as market orders on Bitunix.
            source = await asyncio.to_thread(trade_manager.classify_exit, symbol, item["avg_price"])
            log.info("%s fill detected: %s %s at %s", source, symbol, order_id, item["avg_price"])
            await asyncio.to_thread(trade_manager.handle_sl_fill, symbol, source)
        else:
            log.info("Manual TP fill detected: %s %s", symbol, order_id)
            await _handle_manual_tp(symbol, trade_manager, tg_app, db)


async def _handle_position_close(symbol: str, trade_manager: TradeManager, db: StateDB):
    """Check and handle position closure from the event loop thread so db.get()
    sees the state after any simultaneous order handler has deleted it."""
    state = db.get(symbol)
    # Before the first fill a limit entry is still resting and size is 0 —
    # Bitunix also pushes size-0 updates for leverage/margin changes. Only a
    # position that actually opened can "close".
    if state and state["status"] == "active" and state.get("position_opened"):
        source = trade_manager.exchange.recent_trigger(symbol) or "Position close"
        if source == "SL" and state.get("trailing_distance") and state.get("breakeven_moved"):
            source = "Trailing SL"
        await asyncio.to_thread(trade_manager.handle_sl_fill, symbol, source)


_last_position_sync = {}


async def _handle_position_open(item: dict, trade_manager: TradeManager, db: StateDB):
    """Backup path for SL: a position that opened (or grew) runs the fill
    handler, even if the order event was missed. Bitunix position pushes
    don't include the SL, so the handler checks it over REST."""
    symbol = item["symbol"]
    now = time.time()
    if now - _last_position_sync.get(symbol, 0) < 5:
        return
    state = db.get(symbol)
    if not state or state["status"] != "active":
        return
    if not state.get("position_opened") or item["event"] == "OPEN":
        _last_position_sync[symbol] = now
        log.info("Position %s for %s: size=%s opened=%s — syncing",
                 item["event"], symbol, item["size"], state.get("position_opened"))
        await asyncio.to_thread(trade_manager.handle_entry_or_dca_fill, symbol)


def _log_startup_diagnostics():
    import platform
    from importlib import metadata

    def ver(pkg):
        try:
            return metadata.version(pkg)
        except metadata.PackageNotFoundError:
            return "not installed"

    log.info("Python %s on %s (%s)", sys.version.split()[0], platform.platform(), platform.machine())
    log.info("Libraries: python-telegram-bot=%s requests=%s websocket-client=%s certifi=%s",
             ver("python-telegram-bot"), ver("requests"), ver("websocket-client"), ver("certifi"))
    log.info("CWD=%s  DB=%s", os.getcwd(), config.DB_PATH)
    log.info("Config: exchange=Bitunix risk=%s%% dca_split=%s leverage=%s margin=%s confirm_timeout=%ss "
             "breakeven_timeout=%ss watchdog=%ss trailing=%ss",
             config.RISK_PERCENT, config.DCA_SPLIT_RATIO, config.DEFAULT_LEVERAGE,
             config.DEFAULT_MARGIN_MODE, config.CONFIRM_TIMEOUT_SECONDS, config.BREAKEVEN_TIMEOUT_SECONDS,
             config.WATCHDOG_INTERVAL_SECONDS, config.TRAILING_INTERVAL_SECONDS)
    log.info("System clock: %s (epoch %.0f) — Bitunix rejects requests if this drifts", time.strftime("%Y-%m-%d %H:%M:%S %z"), time.time())


def _log_exchange_diagnostics(exchange: BitunixClient, notify):
    """Account-side state that explains most Bitunix failures: clock drift, position mode, API access."""
    drift = exchange.clock_drift_seconds()
    if drift is not None:
        log.info("Clock drift vs Bitunix: %+.1fs", drift)
        if abs(drift) > 5:
            log.warning("Clock drift %.1fs — signed requests may be rejected (error 10007). Sync the device clock.", drift)
            notify(f"⚠️ Device clock is {drift:+.0f}s off Bitunix time — orders may be rejected. Sync the clock.")
    try:
        mode = exchange.get_position_mode()
        wallet = exchange.get_wallet_info()
        log.info("Bitunix account: position_mode=%s balance=%.2f available=%.2f upnl=%.2f",
                 mode, wallet["equity"], wallet["available"], wallet["unrealized_pnl"])
        if mode != "ONE_WAY":
            # Verified live: in hedge mode closing orders still use the opposite side,
            # so fill routing works. Only holding a long AND a short on one symbol breaks it.
            log.warning("Bitunix position mode is %s — fine as long as each symbol is held in one direction", mode)
    except Exception as e:
        log.error("Bitunix account check failed: %s", e)
        notify(f"⚠️ Could not read the Bitunix account: {e}\nCheck the API key, its permissions and IP whitelist.")


def _start_watchdog(exchange: BitunixClient, trade_manager: TradeManager, notify):
    """Background thread: re-checks every trade against Bitunix and revives a dead WebSocket."""
    ws_down_since = [None]
    restarting = threading.Event()

    def restart_ws():
        try:
            log.warning("Restarting Bitunix private WebSocket...")
            exchange.restart_private_ws()
            log.info("Bitunix private WebSocket restarted")
            notify("🔌 Bitunix connection restored. Positions re-checked.")
            trade_manager.ensure_protection()
        except Exception as e:
            log.error("WebSocket restart failed: %s", e)
        finally:
            restarting.clear()

    def run():
        while True:
            time.sleep(config.WATCHDOG_INTERVAL_SECONDS)
            try:
                if exchange.ws_is_healthy():
                    ws_down_since[0] = None
                elif ws_down_since[0] is None:
                    ws_down_since[0] = time.time()
                    log.warning("Bitunix private WebSocket is down")
                elif time.time() - ws_down_since[0] > 60 and not restarting.is_set():
                    notify("⚠️ Bitunix live connection was down >60s — fills may have been missed. Reconnecting; "
                           "watchdog keeps checking SLs meanwhile.")
                    ws_down_since[0] = time.time()
                    restarting.set()
                    threading.Thread(target=restart_ws, daemon=True, name="ws-restart").start()
                trade_manager.ensure_protection()
            except Exception as e:
                log.exception("Watchdog iteration failed: %s", e)

    threading.Thread(target=run, daemon=True, name="watchdog").start()
    log.info("Watchdog started (every %ss)", config.WATCHDOG_INTERVAL_SECONDS)


def _start_trailing_loop(trade_manager: TradeManager):
    """Background thread: bot-side trailing stop (Bitunix has no native one)."""
    def run():
        while True:
            time.sleep(config.TRAILING_INTERVAL_SECONDS)
            try:
                trade_manager.update_trailing_stops()
            except Exception as e:
                log.warning("Trailing iteration failed: %s", e)

    threading.Thread(target=run, daemon=True, name="trailing").start()
    log.info("Trailing loop started (every %ss)", config.TRAILING_INTERVAL_SECONDS)


def main():
    _log_startup_diagnostics()
    exchange = BitunixClient()
    db = StateDB()

    manager_ref = ManagerRef()
    tg_app = build_app(manager_ref)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def notify(text: str):
        _fire_and_forget(
            tg_app.bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=text),
            loop,
        )

    trade_manager = TradeManager(exchange, db, notify)
    manager_ref.tm = trade_manager

    # Reconcile active positions on startup
    try:
        for msg in trade_manager.reconcile():
            notify(msg)
    except (requests.exceptions.SSLError, urllib3.exceptions.SSLError) as e:
        log.critical("SSL connection failed on startup: %s", e)
        log.critical("Try: pkg install ca-certificates && update-ca-certificates")
        notify(f"SSL Error on startup: {e}\nCheck CA certificates on this device.")
        return
    except Exception as e:
        log.error("Reconciliation failed: %s", e)

    # Patch stage_signal to auto-expire confirmation buttons after timeout
    _original_stage = trade_manager.stage_signal
    def _patched_stage(signal):
        symbol = signal.asset
        result = _original_stage(signal)
        async def _expire():
            await asyncio.sleep(config.CONFIRM_TIMEOUT_SECONDS)
            entry = trade_manager.get_pending(symbol)
            if entry and entry.get("message_id") and time.time() > entry.get("expiry", 0):
                try:
                    await tg_app.bot.edit_message_text(
                        chat_id=entry["chat_id"],
                        message_id=entry["message_id"],
                        text=f"⏱️ Confirmation for {symbol} expired.",
                    )
                except Exception:
                    pass
        asyncio.run_coroutine_threadsafe(_expire(), loop)
        return result
    trade_manager.stage_signal = _patched_stage

    _log_exchange_diagnostics(exchange, notify)

    # These run on the Bitunix WebSocket thread and receive one normalized
    # event each (see bitunix_client.py). They must never raise.
    def on_order_update(item):
        try:
            if not item.get("symbol"):
                return
            # Snapshot state now: a concurrent handler may delete it before the coroutine runs.
            state = db.get(item["symbol"])
            _fire_and_forget(
                _handle_fill_async(item, state, trade_manager, tg_app, db),
                loop,
            )
        except Exception:
            log.exception("on_order_update failed for event: %s", item)

    def on_position_update(item):
        try:
            symbol = item.get("symbol")
            if not symbol:
                return
            if item["size"] == 0:
                log.info("Position %s on %s (size 0)", item["event"], symbol)
                _fire_and_forget(
                    _handle_position_close(symbol, trade_manager, db),
                    loop,
                )
            else:
                _fire_and_forget(
                    _handle_position_open(item, trade_manager, db),
                    loop,
                )
        except Exception:
            log.exception("on_position_update failed for event: %s", item)

    exchange.start_private_ws(on_order=on_order_update, on_position=on_position_update)
    _start_watchdog(exchange, trade_manager, notify)
    _start_trailing_loop(trade_manager)
    log.info("Bitunix private WebSocket connected. Starting Telegram polling...")
    try:
        tg_app.run_polling(bootstrap_retries=-1)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutting down...")
        exchange.stop_ws()
        db.close()
        logging.shutdown()


if __name__ == "__main__":
    main()
