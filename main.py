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

    # TP1 — send interactive breakeven prompt
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes — move SL to entry", callback_data=f"breakeven_yes:{symbol}"),
            InlineKeyboardButton("❌ No", callback_data=f"breakeven_no:{symbol}"),
        ]
    ]
    msg = await tg_app.bot.send_message(
        chat_id=config.TELEGRAM_CHAT_ID,
        text=f"🎯 TP1 hit on {symbol}\n\nMove SL to entry price?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    db.upsert(symbol, breakeven_prompt_msg_id=msg.message_id)

    # Schedule auto-breakeven on timeout
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
                pass  # message may have been deleted already

    asyncio.get_event_loop().create_task(_auto_breakeven())


def main():
    bybit = BybitClient()
    db = StateDB()

    manager_ref = ManagerRef()
    tg_app = build_app(manager_ref)  # bot now exists, so notify() can use tg_app.bot

    def notify(text: str):
        asyncio.get_event_loop().create_task(
            tg_app.bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=text)
        )

    trade_manager = TradeManager(bybit, db, notify)
    manager_ref.tm = trade_manager

    # Patch stage_signal to auto-expire confirmation buttons after timeout
    _original_stage = trade_manager.stage_signal
    def _patched_stage(signal):
        symbol = signal.asset
        result = _original_stage(signal)
        pending = trade_manager.pending.get(symbol)
        if pending and pending.get("chat_id"):
            async def _expire():
                await asyncio.sleep(config.CONFIRM_TIMEOUT_SECONDS)
                entry = trade_manager.pending.get(symbol)
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
            symbol = item.get("symbol")
            status = item.get("orderStatus")
            order_id = item.get("orderId")
            if status != "Filled":
                continue

            state = db.get(symbol)
            if not state:
                continue

            if order_id == state["entry_order_id"] or order_id == state["dca_order_id"]:
                log.info(f"Entry/DCA fill: {symbol} {order_id}")
                trade_manager.handle_entry_or_dca_fill(symbol)
            elif order_id == state["sl_order_id"]:
                log.info(f"SL fill: {symbol} {order_id}")
                trade_manager.handle_sl_fill(symbol)
            else:
                tp_ids = db.loads(state["tp_order_ids"])
                if order_id in tp_ids:
                    log.info(f"TP fill: {symbol} {order_id}")
                    trade_manager.handle_tp_fill(symbol, order_id)
                elif item.get("reduceOnly") and state["status"] == "active":
                    log.info(f"Manual TP fill detected: {symbol} {order_id}")
                    asyncio.get_event_loop().create_task(
                        _handle_manual_tp(symbol, trade_manager, tg_app, db)
                    )
 
    def on_position_update(msg):
        # reserved for future use (e.g. detecting manual intervention on the exchange UI)
        pass
 
    bybit.start_private_ws(on_order=on_order_update, on_position=on_position_update)
    log.info("Bybit private WebSocket connected. Starting Telegram polling...")
    # bootstrap_retries=-1 = retry indefinitely on startup connection failures
    # (e.g. Termux briefly losing network) instead of crashing the whole process.
    tg_app.run_polling(bootstrap_retries=-1)
 
 
if __name__ == "__main__":
    main()