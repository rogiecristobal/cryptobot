"""
Telegram side. Only messages from TELEGRAM_CHAT_ID are ever acted on —
this is the single most important security boundary in the whole bot.
"""
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, ContextTypes, filters
from signal_parser import parse_signal
import config

log = logging.getLogger("telegram_bot")


def _authorized(update: Update) -> bool:
    return update.effective_chat.id == config.TELEGRAM_CHAT_ID


def build_app(manager_ref):
    """manager_ref is an object with a `.tm` attribute holding the TradeManager,
    set by the caller after the bot (and therefore notify()) exists."""
    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        await update.message.reply_text(
            "Bot online. Paste a signal to stage it, then reply ✅ to confirm within "
            f"{config.CONFIRM_TIMEOUT_SECONDS}s."
        )

    async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        text = update.message.text.strip()

        if text in ("✅", "confirm", "yes"):
            if not trade_manager.pending:
                await update.message.reply_text("Nothing pending to confirm.")
                return
            symbol = next(iter(trade_manager.pending))
            result = trade_manager.confirm(symbol)
            await update.message.reply_text(result)
            return

        signal = parse_signal(text)
        try:
            prompt = trade_manager.stage_signal(signal)
            await update.message.reply_text(prompt)
        except ValueError as e:
            await update.message.reply_text(str(e))

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    return app
