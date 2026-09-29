"""
Entrypoint. Runs the Telegram bot (polling) and the Bybit private WebSocket
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
from bybit_client import BybitClient
from state_db import StateDB
from trade_manager import TradeManager
from telegram_bot import build_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("logs/bot.log")],
)
log = logging.getLogger("main")

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
                    else f"⏱️ Timeout — tried to move SL to entry for {symbol} but it FAILED. Check Bybit.")
            try:
                await tg_app.bot.edit_message_text(
                    chat_id=config.TELEGRAM_CHAT_ID,
                    message_id=msg.message_id,
                    text=text,
                )
            except Exception:
                pass

    asyncio.get_event_loop().create_task(_auto_breakeven())


async def _handle_fill_async(item: dict, states: dict, trade_manager: TradeManager, tg_app, db: StateDB, loop):
    """Async wrapper for fill handling, offloads blocking work via to_thread."""
    symbol = item.get("symbol")
    status = item.get("orderStatus")
    order_id = item.get("orderId")
    if status not in ("Filled", "PartiallyFilled"):
        return

    state = states.get(symbol) or db.get(symbol)
    if not state:
        log.info("Fill for untracked symbol %s (%s %s) — ignored", symbol, order_id, status)
        return

    if order_id in (state["entry_order_id"], state["dca_order_id"]) or not item.get("reduceOnly"):
        # Any position-increasing fill on a tracked symbol. Matching on
        # reduceOnly too covers a fill that lands before confirm() has stored
        # the order ID. Partial fills count: the filled part needs its SL now.
        log.info("Entry/DCA fill: %s %s (%s)", symbol, order_id, status)
        await asyncio.to_thread(trade_manager.handle_entry_or_dca_fill, symbol)
    elif status != "Filled":
        return
    elif item.get("reduceOnly") and state["status"] == "active":
        order_type = item.get("orderType", "")
        if order_type == "Market":
            td = item.get("triggerDirection")
            is_native_tp = (
                (state["position"] == "LONG" and td == 1)
                or (state["position"] == "SHORT" and td == 2)
            ) if td else False
            if is_native_tp:
                log.info("Native TP fill detected: %s %s", symbol, order_id)
                await asyncio.to_thread(trade_manager.handle_sl_fill, symbol, "TP")
            else:
                log.info("SL fill detected: %s %s", symbol, order_id)
                await asyncio.to_thread(trade_manager.handle_sl_fill, symbol)
        else:
            log.info("Manual TP fill detected: %s %s", symbol, order_id)
            await _handle_manual_tp(symbol, trade_manager, tg_app, db)


async def _handle_position_close(symbol: str, trade_manager: TradeManager, db: StateDB):
    """Check and handle position closure from the event loop thread so db.get()
    sees the state after any simultaneous order handler has deleted it."""
    state = db.get(symbol)
    # Before the first fill a limit entry is still resting and size is 0 —
    # Bybit also pushes size-0 updates for leverage/margin changes. Only a
    # position that actually opened can "close".
    if state and state["status"] == "active" and state.get("position_opened"):
        await asyncio.to_thread(trade_manager.handle_sl_fill, symbol, "Position")


_last_position_sync = {}


async def _handle_position_open(item: dict, trade_manager: TradeManager, db: StateDB):
    """Backup path for SL: a position update showing size > 0 with no SL
    triggers the fill handler, even if the order event was missed."""
    symbol = item.get("symbol")
    now = time.time()
    if now - _last_position_sync.get(symbol, 0) < 5:
        return
    state = db.get(symbol)
    if not state or state["status"] != "active":
        return
    has_sl = float(item.get("stopLoss") or 0) > 0
    if not state.get("position_opened") or not has_sl:
        _last_position_sync[symbol] = now
        log.info("Position update for %s: size=%s stopLoss=%r opened=%s — syncing",
                 symbol, item.get("size"), item.get("stopLoss"), state.get("position_opened"))
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
    log.info("Libraries: pybit=%s python-telegram-bot=%s requests=%s websocket-client=%s certifi=%s",
             ver("pybit"), ver("python-telegram-bot"), ver("requests"), ver("websocket-client"), ver("certifi"))
    log.info("CWD=%s  DB=%s", os.getcwd(), config.DB_PATH)
    log.info("Config: category=%s risk=%s%% dca_split=%s leverage=%s margin=%s confirm_timeout=%ss "
             "breakeven_timeout=%ss watchdog=%ss ws_retries=%s",
             config.BYBIT_CATEGORY, config.RISK_PERCENT, config.DCA_SPLIT_RATIO, config.DEFAULT_LEVERAGE,
             config.DEFAULT_MARGIN_MODE, config.CONFIRM_TIMEOUT_SECONDS, config.BREAKEVEN_TIMEOUT_SECONDS,
             config.WATCHDOG_INTERVAL_SECONDS, BybitClient.WS_RETRIES or "infinite")
    log.info("System clock: %s (epoch %.0f) — Bybit rejects requests if this drifts", time.strftime("%Y-%m-%d %H:%M:%S %z"), time.time())


def _start_watchdog(bybit: BybitClient, trade_manager: TradeManager, notify):
    """Background thread: re-checks every trade against Bybit and revives a dead WebSocket."""
    ws_down_since = [None]
    restarting = threading.Event()

    def restart_ws():
        try:
            log.warning("Restarting Bybit private WebSocket...")
            bybit.restart_private_ws()
            log.info("Bybit private WebSocket restarted")
            notify("🔌 Bybit connection restored. Positions re-checked.")
            trade_manager.ensure_protection()
        except Exception as e:
            log.error("WebSocket restart failed: %s", e)
        finally:
            restarting.clear()

    def run():
        while True:
            time.sleep(config.WATCHDOG_INTERVAL_SECONDS)
            try:
                if bybit.ws_is_healthy():
                    ws_down_since[0] = None
                elif ws_down_since[0] is None:
                    ws_down_since[0] = time.time()
                    log.warning("Bybit private WebSocket is down")
                elif time.time() - ws_down_since[0] > 60 and not restarting.is_set():
                    notify("⚠️ Bybit live connection was down >60s — fills may have been missed. Reconnecting; "
                           "watchdog keeps checking SLs meanwhile.")
                    ws_down_since[0] = time.time()
                    restarting.set()
                    threading.Thread(target=restart_ws, daemon=True, name="ws-restart").start()
                trade_manager.ensure_protection()
            except Exception as e:
                log.exception("Watchdog iteration failed: %s", e)

    threading.Thread(target=run, daemon=True, name="watchdog").start()
    log.info("Watchdog started (every %ss)", config.WATCHDOG_INTERVAL_SECONDS)


def main():
    _log_startup_diagnostics()
    bybit = BybitClient()
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

    trade_manager = TradeManager(bybit, db, notify)
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

    # These run on pybit's WebSocket thread. An exception escaping them makes
    # pybit shut the socket down for good, so they must never raise.
    def on_order_update(msg):
        try:
            items = msg.get("data", [])
            if not items:
                return
            symbols = [item.get("symbol") for item in items if item.get("symbol")]
            states = db.get_many(symbols) if symbols else {}
            for item in items:
                _fire_and_forget(
                    _handle_fill_async(item, states, trade_manager, tg_app, db, loop),
                    loop,
                )
        except Exception:
            log.exception("on_order_update failed for message: %s", msg)

    def on_position_update(msg):
        try:
            for item in msg.get("data", []):
                symbol = item.get("symbol")
                size = float(item.get("size") or 0)
                if size == 0:
                    log.info("Position size 0 on %s (position update)", symbol)
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
            log.exception("on_position_update failed for message: %s", msg)

    bybit.start_private_ws(on_order=on_order_update, on_position=on_position_update)
    _start_watchdog(bybit, trade_manager, notify)
    log.info("Bybit private WebSocket connected. Starting Telegram polling...")
    try:
        tg_app.run_polling(bootstrap_retries=-1)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutting down...")
        bybit.stop_ws()
        db.close()
        logging.shutdown()


if __name__ == "__main__":
    main()
