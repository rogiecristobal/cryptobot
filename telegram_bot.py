"""
Telegram side. Only messages from TELEGRAM_CHAT_ID are ever acted on —
this is the single most important security boundary in the whole bot.
"""
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, ContextTypes, filters
from signal_parser import parse_signal
import ocr
import config
 
log = logging.getLogger("telegram_bot")
 
 
def _authorized(update: Update) -> bool:
    return update.effective_chat.id == config.TELEGRAM_CHAT_ID
 
 
async def _stage_and_reply(update: Update, trade_manager, text: str):
    signal = parse_signal(text)
    try:
        prompt = trade_manager.stage_signal(signal)
        await update.message.reply_text(prompt)
    except ValueError as e:
        await update.message.reply_text(str(e))
    except Exception as e:
        log.exception("Unexpected error staging signal")
        await update.message.reply_text(f"Something went wrong staging that signal: {e}")
 
 
def build_app(manager_ref):
    """manager_ref is an object with a `.tm` attribute holding the TradeManager,
    set by the caller after the bot (and therefore notify()) exists."""
    app = (
        Application.builder()
        .token(config.TELEGRAM_BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .get_updates_connect_timeout(30)
        .get_updates_read_timeout(30)
        .build()
    )
 
    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        screenshot_note = " or send a screenshot" if ocr.is_enabled() else ""
        await update.message.reply_text(
            f"Bot online. Paste a signal{screenshot_note} to stage it, then reply ✅ "
            f"to confirm within {config.CONFIRM_TIMEOUT_SECONDS}s."
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
            try:
                result = trade_manager.confirm(symbol)
                await update.message.reply_text(result)
            except Exception as e:
                log.exception("Unexpected error confirming trade")
                await update.message.reply_text(
                    f"⚠️ Error placing the trade for {symbol}: {e}\n"
                    f"Check Bybit directly to confirm nothing partial went through."
                )
            return
 
        await _stage_and_reply(update, trade_manager, text)
 
    async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
 
        if not ocr.is_enabled():
            await update.message.reply_text(
                "Screenshot input isn't enabled — install Tesseract (see README) to turn it on. "
                "You can still paste the signal as text."
            )
            return
 
        photo = update.message.photo[-1]  # largest resolution Telegram sent
        tg_file = await context.bot.get_file(photo.file_id)
        image_bytes = bytes(await tg_file.download_as_bytearray())
 
        try:
            text = ocr.extract_text_from_image(image_bytes)
        except Exception as e:
            log.exception("OCR failed")
            await update.message.reply_text(f"Couldn't read that screenshot: {e}")
            return
 
        if not text:
            await update.message.reply_text("Couldn't find any readable text in that screenshot.")
            return
 
        # Always show the transcription back — this is the one chance to catch
        # a misread number before it turns into a staged trade.
        await update.message.reply_text(f"Transcribed:\n{text}")
        await _stage_and_reply(update, trade_manager, text)
 
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    return app