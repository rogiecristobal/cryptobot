"""
Entrypoint. Runs the Telegram bot (polling) and the exchange private WebSocket
(order/position fills) side by side in one process.
"""
import asyncio
import logging
import os
import platform
import ssl
import sys
import threading
import time

import requests
import urllib3.exceptions

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import config
from state_db import StateDB
from trade_manager import TradeManager
from telegram_bot import build_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("logs/bot.log")],
)
log = logging.getLogger("main")
EX = config.EXCHANGE_NAME  # in messages
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
            InlineKeyboardButton("✅ Yes — move SL to breakeven", callback_data=f"breakeven_yes:{symbol}"),
            InlineKeyboardButton("❌ No", callback_data=f"breakeven_no:{symbol}"),
        ]
    ]
    try:
        msg = await tg_app.bot.send_message(
            chat_id=config.TELEGRAM_CHAT_ID,
            text=f"🎯 TP1 hit on {symbol}\n\nMove SL to breakeven (entry + fees)?",
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
            text = (f"⏱️ Timeout — SL auto-moved to breakeven for {symbol}." if ok
                    else f"⏱️ Timeout — tried to move SL to breakeven for {symbol} but it FAILED. Check {EX}.")
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
    item is a normalized order event from the exchange client."""
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
            # TP/SL triggers execute as market orders, but so does a reduce made
            # in the exchange app — handle_close_fill tells them apart.
            log.info("Closing market fill: %s %s qty %s at %s", symbol, order_id, item["qty"], item["avg_price"])
            await asyncio.to_thread(trade_manager.handle_close_fill, symbol, item["avg_price"], item["qty"])
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
        # handle_sl_fill market-closes whatever is open, so a size-0 push that
        # isn't a real close must not reach it. A real close the REST API is
        # slow to show is picked up by the watchdog instead.
        if not await asyncio.to_thread(trade_manager.position_gone, symbol):
            log.warning(f"Size-0 push for %s but {EX} still shows the position — not closing it", symbol)
            return
        source = trade_manager.exchange.recent_trigger(symbol) or "Position close"
        if source == "SL" and state.get("trailing_distance") and state.get("breakeven_moved"):
            source = "Trailing SL"
        await asyncio.to_thread(trade_manager.handle_sl_fill, symbol, source)


_last_position_sync = {}


async def _handle_position_open(item: dict, trade_manager: TradeManager, db: StateDB):
    """Backup path for SL: a position that opened (or grew) runs the fill
    handler, even if the order event was missed. Position pushes
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


_instance_lock_fd = None  # held open for the life of the process
_LOCK_OFFSET = 1024  # Windows locks a byte past the PID text, so a refused copy can still read the PID


def _acquire_instance_lock() -> bool:
    """
    One copy per device. Two copies manage the same exchange positions, each
    with its own idea of the SL, and undo each other's changes. The OS frees
    the lock when the process dies, however it dies.
    """
    global _instance_lock_fd
    path = os.path.join(os.path.dirname(config.DB_PATH), "bot.lock")
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, _LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            other = os.read(fd, 64).decode(errors="replace").strip() or "unknown"
        except OSError:
            other = "unknown"
        os.close(fd)
        log.critical("Another copy of the bot is already running on this device (pid %s, lock %s). "
                     "Stop it first.", other, path)
        return False
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, f"{os.getpid():<20}\n".encode())  # fixed width: overwrites an older PID fully
    _instance_lock_fd = fd
    return True


async def _other_copy_polling(seconds: float = 10) -> bool:
    """
    Detects a copy on ANOTHER device, which the lock file can't see. Our long
    poll cuts the other copy's short; when it polls again it cuts ours, and
    Telegram tells us with a Conflict. Passes no offset, so no update is consumed.
    """
    from telegram import Bot
    from telegram.error import Conflict

    try:
        async with Bot(config.TELEGRAM_BOT_TOKEN) as bot:
            deadline = time.monotonic() + seconds
            while (left := deadline - time.monotonic()) > 1:
                try:
                    updates = await bot.get_updates(timeout=int(left))
                except Conflict as e:
                    if "terminated by other getUpdates" not in str(e):
                        raise
                    await bot.send_message(
                        chat_id=config.TELEGRAM_CHAT_ID,
                        text=(f"⛔ A copy of the bot on {platform.node()} refused to start: another copy "
                              f"is already running. Stop that one first, then start this one again."))
                    return True
                if updates:
                    await asyncio.sleep(1)  # unconsumed updates come back at once; don't spin
    except Exception as e:
        log.warning("Could not check Telegram for another running copy: %s", e)
    return False


def _log_startup_diagnostics():
    from importlib import metadata

    def ver(pkg):
        try:
            return metadata.version(pkg)
        except metadata.PackageNotFoundError:
            return "not installed"

    log.info("Python %s on %s (%s), host=%s pid=%s", sys.version.split()[0], platform.platform(),
             platform.machine(), platform.node(), os.getpid())
    log.info("Libraries: python-telegram-bot=%s requests=%s websocket-client=%s certifi=%s",
             ver("python-telegram-bot"), ver("requests"), ver("websocket-client"), ver("certifi"))
    log.info("CWD=%s  DB=%s", os.getcwd(), config.DB_PATH)
    log.info("Exchange: %s (EXCHANGE=%s in .env)", EX, config.EXCHANGE)
    if os.getenv("DEFAULT_LEVERAGE"):
        log.warning("DEFAULT_LEVERAGE in .env is no longer used — leverage comes from LEVERAGE_TIERS. "
                    "Delete the line to silence this.")
    log.info(f"Config: exchange={EX} risk=%s%% max_risk=%s%% dca_split=%s leverage_tiers=%s margin=%s "
             "confirm_timeout=%ss breakeven_timeout=%ss breakeven_offset=%s%% watchdog=%ss trailing=%ss",
             config.RISK_PERCENT, config.MAX_RISK_PERCENT, config.DCA_SPLIT_RATIO,
             ",".join(f"{p:g}:{lev}" for p, lev in config.LEVERAGE_TIERS),
             config.DEFAULT_MARGIN_MODE, config.CONFIRM_TIMEOUT_SECONDS, config.BREAKEVEN_TIMEOUT_SECONDS,
             config.BREAKEVEN_OFFSET_PERCENT,
             config.WATCHDOG_INTERVAL_SECONDS, config.TRAILING_INTERVAL_SECONDS)
    log.info(f"System clock: %s (epoch %.0f) — {EX} rejects requests if this drifts", time.strftime("%Y-%m-%d %H:%M:%S %z"), time.time())


def _log_exchange_diagnostics(exchange, notify):
    """Account-side state that explains most exchange failures: clock drift, position mode, API access."""
    drift = exchange.clock_drift_seconds()
    if drift is not None:
        log.info(f"Clock drift vs {EX}: %+.1fs", drift)
        if abs(drift) > 5:
            log.warning("Clock drift %.1fs — signed requests may be rejected. Sync the device clock.", drift)
            notify(f"⚠️ Device clock is {drift:+.0f}s off {EX} time — orders may be rejected. Sync the clock.")
    try:
        for problem in exchange.check_account():
            log.error("%s", problem)
            notify(f"⚠️ {problem}")
        mode = exchange.get_position_mode()
        wallet = exchange.get_wallet_info()
        log.info(f"{EX} account: position_mode=%s balance=%.2f available=%.2f upnl=%.2f",
                 mode, wallet["equity"], wallet["available"], wallet["unrealized_pnl"])
        if mode != "ONE_WAY":
            # Verified live: in hedge mode closing orders still use the opposite side,
            # so fill routing works. Only holding a long AND a short on one symbol breaks it.
            log.warning(f"{EX} position mode is %s — fine as long as each symbol is held in one direction", mode)
    except Exception as e:
        log.error(f"{EX} account check failed: %s", e)
        notify(f"⚠️ Could not read the {EX} account: {e}\nCheck the API key, its permissions and IP whitelist.")


def _make_exchange():
    """The client for EXCHANGE in .env. Both expose the same interface (see bitunix_client.py)."""
    if config.EXCHANGE == "bybit":
        from bybit_client import BybitClient
        return BybitClient()
    from bitunix_client import BitunixClient
    return BitunixClient()


def _warn_other_exchange_trades(notify):
    """
    Trades still open on the exchange the bot is NOT running on keep their
    SL/TP there, but nothing trails them, steps up their SL or cleans up
    after them until the bot runs on that exchange again.
    """
    for name, filename in config.DB_FILES.items():
        if name == config.EXCHANGE:
            continue
        path = os.path.join(os.path.dirname(config.DB_PATH), filename)
        symbols = StateDB.active_symbols_in(path)
        if symbols:
            other = config.EXCHANGE_NAMES[name]
            log.warning("%d %s trade(s) still open and NOT managed: %s", len(symbols), other, symbols)
            notify(f"⚠️ {len(symbols)} {other} trade(s) still tracked ({', '.join(symbols)}) but the bot now runs "
                   f"on {EX}. Their SL/TP stay on {other}, but no trailing, SL step-ups or cleanup until "
                   f"you switch back (EXCHANGE={name}).")


def _start_watchdog(exchange, trade_manager: TradeManager, notify):
    """Background thread: re-checks every trade against the exchange and revives a dead WebSocket."""
    ws_down_since = [None]
    last_down_notice = [0.0]
    restarting = threading.Event()

    def restart_ws():
        try:
            log.warning(f"Restarting {EX} private WebSocket...")
            exchange.restart_private_ws()  # waits up to 30s for the login
            if exchange.ws_is_healthy():
                log.info(f"{EX} private WebSocket restarted")
                if last_down_notice[0]:
                    notify(f"🔌 {EX} connection restored. Positions re-checked.")
                    last_down_notice[0] = 0.0
            else:
                log.warning(f"{EX} private WebSocket restarted but not logged in yet (%s)",
                            exchange.ws_login_error or "no login reply")
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
                    log.warning(f"{EX} private WebSocket is down")
                elif time.time() - ws_down_since[0] > 60 and not restarting.is_set():
                    # Reconnect every minute, but alert at most every 10 min.
                    if time.time() - last_down_notice[0] > 600:
                        reason = (f"\n{EX} rejected the login: {exchange.ws_login_error}\n"
                                  f"Check the API key and the device clock."
                                  if exchange.ws_login_error else "")
                        notify(f"⚠️ {EX} live connection was down >60s — fills may have been missed. "
                               "Reconnecting; watchdog keeps checking SLs meanwhile." + reason)
                        last_down_notice[0] = time.time()
                    ws_down_since[0] = time.time()
                    restarting.set()
                    threading.Thread(target=restart_ws, daemon=True, name="ws-restart").start()
                trade_manager.ensure_protection()
            except Exception as e:
                log.exception("Watchdog iteration failed: %s", e)

    threading.Thread(target=run, daemon=True, name="watchdog").start()
    log.info("Watchdog started (every %ss)", config.WATCHDOG_INTERVAL_SECONDS)


def _start_trailing_loop(trade_manager: TradeManager):
    """Background thread: bot-side trailing stop and TP checkpoints (exchange-native ones aren't used)."""
    def run():
        while True:
            time.sleep(config.TRAILING_INTERVAL_SECONDS)
            try:
                trade_manager.check_tp_checkpoints()
            except Exception as e:
                log.warning("TP checkpoint iteration failed: %s", e)
            try:
                trade_manager.update_trailing_stops()
            except Exception as e:
                log.warning("Trailing iteration failed: %s", e)

    threading.Thread(target=run, daemon=True, name="trailing").start()
    log.info("Trailing loop started (every %ss)", config.TRAILING_INTERVAL_SECONDS)


def main():
    if not _acquire_instance_lock():
        sys.exit(1)
    _log_startup_diagnostics()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Before reconcile(): that already rewrites SLs on Bitunix from this copy's DB.
    log.info("Checking Telegram for a copy of the bot running on another device (~10s)...")
    if loop.run_until_complete(_other_copy_polling()):
        log.critical("Another copy of the bot is already running (on another device) — not starting. "
                     "Stop that one first.")
        sys.exit(1)

    exchange = _make_exchange()
    db = StateDB()

    manager_ref = ManagerRef()
    tg_app = build_app(manager_ref)

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
        _, token = result
        async def _expire():
            await asyncio.sleep(config.CONFIRM_TIMEOUT_SECONDS)
            entry = trade_manager.get_pending(symbol)
            if (entry and entry.get("token") == token and entry.get("message_id")
                    and time.time() > entry.get("expiry", 0)):
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
    _warn_other_exchange_trades(notify)

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
    log.info(f"{EX} private WebSocket connected. Starting Telegram polling...")
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
