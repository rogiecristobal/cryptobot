"""
/se_* commands, registered on the EXISTING Telegram Application (no second
polling loop: Telegram allows only one per token).

They are added in handler group -1, which runs before the existing bot's
handlers in group 0, and they raise ApplicationHandlerStop so the existing
catch-all button handler never sees an S&E button. Only TELEGRAM_CHAT_ID
(the owner) can use them, the same check as the existing commands.
"""
import asyncio
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationHandlerStop, CallbackQueryHandler, CommandHandler, ContextTypes

import config

log = logging.getLogger("se.telegram")
GROUP = -1
P = "[S&E] "

HELP = (P + "Sweep & Engulf commands\n\n"
        "/se_status — mode, budget, equity, open positions, today's losses\n"
        "/se_trades [n] — last n closed trades (default 10), P&L in $ and R\n"
        "/se_pause — stop new entries (open trades stay managed)\n"
        "/se_resume — allow new entries again\n"
        "/se_close <BTC|ETH|SOL|all> — close S&E position(s) at market, after a confirm\n"
        "/se_mode paper|live — switch mode, after a confirm\n"
        "/se_help — this message\n\n"
        "Kill switch: a file named STOP in the bot folder also stops new S&E entries.")


def register(app, worker):
    def authorized(update: Update) -> bool:
        return bool(update.effective_chat) and update.effective_chat.id == config.TELEGRAM_CHAT_ID

    def command(fn):
        async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
            if authorized(update):
                try:
                    await fn(update, context)
                except Exception as e:
                    log.exception("S&E command failed")
                    await update.effective_message.reply_text(f"{P}⚠️ Error: {e}")
            raise ApplicationHandlerStop
        return handler

    async def reply(update: Update, text: str, markup=None):
        await update.effective_message.reply_text(P + text if not text.startswith(P) else text,
                                                  reply_markup=markup)

    @command
    async def se_status(update, context):
        await reply(update, await asyncio.to_thread(worker.status_text))

    @command
    async def se_trades(update, context):
        n = 10
        if context.args:
            try:
                n = max(1, min(50, int(context.args[0])))
            except ValueError:
                await reply(update, "Usage: /se_trades [n]")
                return
        await reply(update, await asyncio.to_thread(worker.trades_text, n))

    @command
    async def se_pause(update, context):
        await reply(update, worker.set_paused(True))

    @command
    async def se_resume(update, context):
        await reply(update, worker.set_paused(False))

    @command
    async def se_close(update, context):
        if not context.args:
            await reply(update, "Usage: /se_close <BTC|ETH|SOL|all>")
            return
        arg = context.args[0].upper()
        targets = await asyncio.to_thread(worker.close_targets, arg)
        if not targets:
            await reply(update, f"No open S&E position for {arg}.")
            return
        names = ", ".join(f"{t['symbol']} ({t['mode']})" for t in targets)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ Close at market", callback_data=f"se:close:{arg}"),
                                    InlineKeyboardButton("Cancel", callback_data="se:cancel")]])
        await reply(update, f"Close {names} at market and cancel its TP/SL?", kb)

    @command
    async def se_mode(update, context):
        mode = (context.args[0].lower() if context.args else "")
        problem = worker.mode_switch_problem(mode)
        if problem:
            await reply(update, problem + f"\nCurrent mode: {worker.mode}.")
            return
        warn = ("Switch to LIVE? Real orders will be placed on Bitunix with real money."
                if mode == "live" else "Switch to PAPER? New signals will only be simulated.")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"✅ Switch to {mode}", callback_data=f"se:mode:{mode}"),
                                    InlineKeyboardButton("Cancel", callback_data="se:cancel")]])
        await reply(update, warn, kb)

    @command
    async def se_help(update, context):
        await reply(update, HELP)

    async def se_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if authorized(update):
            query = update.callback_query
            try:
                await query.answer()
                _, action, *rest = query.data.split(":")
                if action == "close":
                    text = await asyncio.to_thread(worker.close_trades, rest[0])
                elif action == "mode":
                    text = await asyncio.to_thread(worker.set_mode, rest[0])
                else:
                    text = "Cancelled."
                await query.edit_message_text(P + text)
            except Exception as e:
                log.exception("S&E button failed")
                try:
                    await query.edit_message_text(f"{P}⚠️ Error: {e}")
                except Exception:
                    pass
        raise ApplicationHandlerStop

    for name, fn in (("se_status", se_status), ("se_trades", se_trades), ("se_pause", se_pause),
                     ("se_resume", se_resume), ("se_close", se_close), ("se_mode", se_mode),
                     ("se_help", se_help)):
        app.add_handler(CommandHandler(name, fn), group=GROUP)
    app.add_handler(CallbackQueryHandler(se_button, pattern=r"^se:"), group=GROUP)
