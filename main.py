"""
Entrypoint. Runs the Telegram bot (polling) and the Bybit private WebSocket
(order/position fills) side by side in one process.
"""
import asyncio
import logging
import sys
 
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