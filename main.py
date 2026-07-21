"""
Entrypoint. Runs the Telegram bot (polling) and the Bybit private WebSocket
(order/position fills) side by side in one process.
"""
import asyncio
import logging
import sys
import time

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


class ManagerRef:
    tm = None


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
            trade_manager.apply_breakeven(symbol)
            try:
                await tg_app.bot.edit_message_text(
                    chat_id=config.TELEGRAM_CHAT_ID,
                    message_id=msg.message_id,
                    text=f"⏱️ Timeout — SL auto-moved to entry for {symbol}.",
                )
            except Exception:
                pass

    asyncio.get_event_loop().create_task(_auto_breakeven())


async def _handle_fill_async(item: dict, trade_manager: TradeManager, tg_app, db: StateDB, loop):
    """Async wrapper for fill handling, offloads blocking work via to_thread."""
    symbol = item.get("symbol")
    status = item.get("orderStatus")
    order_id = item.get("orderId")
    if status != "Filled":
        return

    state = db.get(symbol)
    if not state:
        return

    if order_id == state["entry_order_id"] or order_id == state["dca_order_id"]:
        log.info("Entry/DCA fill: %s %s", symbol, order_id)
        await asyncio.to_thread(trade_manager.handle_entry_or_dca_fill, symbol)
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


def main():
    bybit = BybitClient()
    db = StateDB()

    manager_ref = ManagerRef()
    tg_app = build_app(manager_ref)

    loop = asyncio.get_event_loop()

    def notify(text: str):
        try:
            asyncio.run_coroutine_threadsafe(
                tg_app.bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=text),
                loop,
            )
        except Exception as e:
            log.error("notify() failed: %s", e)

    trade_manager = TradeManager(bybit, db, notify)
    manager_ref.tm = trade_manager

    # Reconcile active positions on startup
    for msg in trade_manager.reconcile():
        notify(msg)

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
        asyncio.get_event_loop().create_task(_expire())
        return result
    trade_manager.stage_signal = _patched_stage

    def on_order_update(msg):
        for item in msg.get("data", []):
            asyncio.run_coroutine_threadsafe(
                _handle_fill_async(item, trade_manager, tg_app, db, loop),
                loop,
            )

    def on_position_update(msg):
        for item in msg.get("data", []):
            symbol = item.get("symbol")
            size = float(item.get("size", 0))
            state = db.get(symbol)
            if state and state["status"] == "active" and size == 0:
                log.info("Position fully closed on %s (position update)", symbol)
                asyncio.run_coroutine_threadsafe(
                    asyncio.to_thread(trade_manager.handle_sl_fill, symbol, "Position"),
                    loop,
                )

    bybit.start_private_ws(on_order=on_order_update, on_position=on_position_update)
    log.info("Bybit private WebSocket connected. Starting Telegram polling...")
    tg_app.run_polling(bootstrap_retries=-1)


if __name__ == "__main__":
    main()
