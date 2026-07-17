"""
Telegram side. Only messages from TELEGRAM_CHAT_ID are ever acted on —
this is the single most important security boundary in the whole bot.
"""
import logging
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from signal_parser import ParsedSignal
import config

log = logging.getLogger("telegram_bot")


def _authorized(update: Update) -> bool:
    return update.effective_chat.id == config.TELEGRAM_CHAT_ID


def _parse_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


_LABELS = {"sl", "tp", "dca"}


async def _stage_signal(update: Update, trade_manager, signal: ParsedSignal):
    """Stage a pre-built ParsedSignal and reply with the confirmation card."""
    try:
        prompt = trade_manager.stage_signal(signal)
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{signal.asset}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{signal.asset}"),
            ]
        ]
        sent_msg = await update.message.reply_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard))
        if signal.asset in trade_manager.pending:
            trade_manager.pending[signal.asset]["chat_id"] = sent_msg.chat_id
            trade_manager.pending[signal.asset]["message_id"] = sent_msg.message_id
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

    # ---------- /place ----------

    async def place_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args

        if len(args) < 4:
            await update.message.reply_text(
                "Usage:\n"
                "  /place <ASSET> LONG|SHORT <ENTRY|market> SL <SL> [TP <TP>] [DCA <DCA>] [LEVERAGEx]\n"
                "Examples:\n"
                "  /place BTC LONG 69000 SL 67000 TP 71000 5x\n"
                "  /place BTC LONG 69000 SL 67000 5x\n"
                "  /place ETH LONG 3500 SL 3400 TP 3600 DCA 3450 3x\n"
                "  /place SOL LONG market SL 140"
            )
            return

        asset = args[0].upper()
        if not asset.endswith("USDT"):
            asset += "USDT"

        direction = args[1].upper()
        if direction not in ("LONG", "SHORT"):
            await update.message.reply_text("Direction must be LONG or SHORT.")
            return

        entry_str = args[2].lower()
        entry_is_market = entry_str in ("market", "now")
        entry = None if entry_is_market else _parse_float(args[2])
        if not entry_is_market and entry is None:
            await update.message.reply_text(f"Invalid entry: {args[2]}")
            return

        # Parse labeled args from remaining tokens
        sl = None
        tp = None
        dca = None
        leverage = config.DEFAULT_LEVERAGE

        rest = args[3:]
        i = 0
        while i < len(rest):
            token = rest[i]
            label = token.lower()
            if label in _LABELS:
                if i + 1 >= len(rest):
                    await update.message.reply_text(f"Missing value for {token}.")
                    return
                i += 1
                val = _parse_float(rest[i])
                if val is None:
                    await update.message.reply_text(f"Invalid {token} value: {rest[i]}")
                    return
                if label == "sl":
                    sl = val
                elif label == "tp":
                    tp = val
                elif label == "dca":
                    dca = val
            elif token.lower().endswith("x"):
                try:
                    leverage = int(float(token[:-1]))
                except ValueError:
                    await update.message.reply_text(f"Invalid leverage: {token}")
                    return
            else:
                await update.message.reply_text(
                    f"Unknown argument: {token}. Use labels: SL, TP, DCA"
                )
                return
            i += 1

        if sl is None:
            await update.message.reply_text("SL is required. Use: SL <price>")
            return

        # Validate direction consistency
        if not entry_is_market:
            if direction == "LONG" and sl >= entry:
                await update.message.reply_text("For LONG, SL must be below entry.")
                return
            if direction == "SHORT" and sl <= entry:
                await update.message.reply_text("For SHORT, SL must be above entry.")
                return
            if tp is not None:
                if direction == "LONG" and tp <= entry:
                    await update.message.reply_text(f"For LONG, TP {tp} must be above entry {entry}.")
                    return
                if direction == "SHORT" and tp >= entry:
                    await update.message.reply_text(f"For SHORT, TP {tp} must be below entry {entry}.")
                    return
            if dca is not None:
                if direction == "LONG" and dca >= entry:
                    await update.message.reply_text(f"For LONG, DCA {dca} must be below entry {entry}.")
                    return
                if direction == "SHORT" and dca <= entry:
                    await update.message.reply_text(f"For SHORT, DCA {dca} must be above entry {entry}.")
                    return

        tps = [tp] if tp is not None else []

        signal = ParsedSignal(
            asset=asset,
            position=direction,
            entry=entry,
            entry_is_market=entry_is_market,
            dca=dca,
            leverage=leverage,
            sl=sl,
            tps=tps,
            errors=[],
        )

        await _stage_signal(update, trade_manager, signal)

    # ---------- Inline button callbacks ----------

    async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        query = update.callback_query
        await query.answer()
        action, symbol = query.data.split(":", 1)
        trade_manager = manager_ref.tm
        chat_id = query.message.chat_id
        message_id = query.message.message_id

        if action == "confirm":
            try:
                result = trade_manager.confirm(symbol)
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=result,
                )
            except Exception as e:
                log.exception("Error confirming trade")
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=f"⚠️ Error placing the trade for {symbol}: {e}\n"
                         f"Check Bybit directly to confirm nothing partial went through.",
                )
        elif action == "cancel":
            result = trade_manager.cancel(symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "confirm_mod":
            result = trade_manager.apply_modification(symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "cancel_mod":
            result = trade_manager.cancel_modification(symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "breakeven_yes":
            trade_manager.apply_breakeven(symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=f"✅ SL moved to entry for {symbol}.",
            )
        elif action == "breakeven_no":
            trade_manager.clear_breakeven_prompt(symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=f"❌ Keeping original SL for {symbol}.",
            )

    # ---------- Register handlers ----------

    app.add_handler(CommandHandler("place", place_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    return app
