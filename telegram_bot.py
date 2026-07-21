"""
Telegram side. Only messages from TELEGRAM_CHAT_ID are ever acted on —
this is the single most important security boundary in the whole bot.
"""
import asyncio
import logging
from typing import Optional, List
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


_LABELS = {"sl", "tp", "dca", "risk"}


async def _stage_signal(update: Update, trade_manager, signal: ParsedSignal):
    """Stage a pre-built ParsedSignal and reply with the confirmation card."""
    try:
        prompt = await asyncio.to_thread(trade_manager.stage_signal, signal)
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{signal.asset}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{signal.asset}"),
            ]
        ]
        sent_msg = await update.message.reply_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard))
        trade_manager.set_pending_metadata(signal.asset, sent_msg.chat_id, sent_msg.message_id)
    except ValueError as e:
        await update.message.reply_text(str(e))
    except Exception as e:
        log.exception("Unexpected error staging signal")
        await update.message.reply_text(f"Something went wrong staging that signal: {e}")


async def _stage_modification(update: Update, trade_manager, mod_func, *args):
    """Generic helper to stage any modification command and reply with a confirmation card."""
    try:
        prompt = await asyncio.to_thread(mod_func, *args)
        symbol = args[0]
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_mod:{symbol}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_mod:{symbol}"),
            ]
        ]
        sent_msg = await update.message.reply_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard))
        trade_manager.set_pending_mod_metadata(symbol, sent_msg.chat_id, sent_msg.message_id)
    except ValueError as e:
        await update.message.reply_text(str(e))
    except Exception as e:
        log.exception("Unexpected error staging modification")
        await update.message.reply_text(f"Something went wrong: {e}")


def _parse_leverage(token: str) -> Optional[int]:
    if token.lower().endswith("x"):
        try:
            return int(float(token[:-1]))
        except ValueError:
            return None
    return None


def build_app(manager_ref):
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
                "  /place <ASSET> LONG|SHORT <ENTRY|market> SL <SL> [TP <TP>] [DCA <DCA>] [RISK <%>] [LEVERAGEx]\n"
                "Examples:\n"
                "  /place BTC LONG 69000 SL 67000 TP 71000 5x\n"
                "  /place BTC LONG 69000 SL 67000 5x\n"
                "  /place ETH LONG 3500 SL 3400 TP 3600 DCA 3450 RISK 5 3x\n"
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

        sl = None
        tp = None
        dca = None
        risk_pct = None
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
                elif label == "risk":
                    risk_pct = val
            elif token.lower().endswith("x"):
                lev = _parse_leverage(token)
                if lev is None:
                    await update.message.reply_text(f"Invalid leverage: {token}")
                    return
                leverage = lev
            else:
                await update.message.reply_text(
                    f"Unknown argument: {token}. Use labels: SL, TP, DCA, RISK"
                )
                return
            i += 1

        if sl is None:
            await update.message.reply_text("SL is required. Use: SL <price>")
            return

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
                    await update.message.reply_text(f"For SHORT, DCA {dca} must be below entry {entry}.")
                    return

        tps = [tp] if tp is not None else []

        signal = ParsedSignal(
            asset=asset,
            position=direction,
            entry=entry,
            entry_is_market=entry_is_market,
            dca=dca,
            leverage=leverage,
            margin_percent=risk_pct,
            raw_text=update.message.text,
            sl=sl,
            tps=tps,
            errors=[],
        )

        await _stage_signal(update, trade_manager, signal)

    # ---------- /sl, /tp, /dca, /entry ----------

    async def sl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /sl <SYMBOL> <new_price>")
            return
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        price = _parse_float(args[1])
        if price is None:
            await update.message.reply_text(f"Invalid price: {args[1]}")
            return
        await _stage_modification(update, manager_ref.tm, manager_ref.tm.stage_modify_sl, symbol, price)

    async def tp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /tp <SYMBOL> <price1> [price2 ...]")
            return
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        prices: List[float] = []
        for a in args[1:]:
            p = _parse_float(a)
            if p is None:
                await update.message.reply_text(f"Invalid price: {a}")
                return
            prices.append(p)
        await _stage_modification(update, manager_ref.tm, manager_ref.tm.stage_modify_tp, symbol, prices)

    async def dca_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        args = context.args
        if len(args) < 1:
            await update.message.reply_text("Usage: /dca <SYMBOL> [price]  (omit price to remove DCA)")
            return
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        if len(args) < 2 or args[1].lower() in ("remove", "none", "0"):
            await _stage_modification(update, manager_ref.tm, manager_ref.tm.stage_modify_dca, symbol, None)
        else:
            price = _parse_float(args[1])
            if price is None:
                await update.message.reply_text(f"Invalid price: {args[1]}")
                return
            await _stage_modification(update, manager_ref.tm, manager_ref.tm.stage_modify_dca, symbol, price)

    async def entry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /entry <SYMBOL> <price|market>")
            return
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        is_market = args[1].lower() in ("market", "now")
        new_price = None if is_market else _parse_float(args[1])
        if not is_market and new_price is None:
            await update.message.reply_text(f"Invalid price: {args[1]}")
            return
        await _stage_modification(update, manager_ref.tm, manager_ref.tm.stage_modify_entry, symbol, new_price, is_market)

    # ---------- /help ----------

    async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        await update.message.reply_text(
            "🤖 CryptoBot Commands\n\n"
            "/place <ASSET> LONG|SHORT <ENTRY|market> SL <SL> [options]\n"
            "  Options: TP <price> DCA <price> RISK <%> LEVERAGEx\n"
            "  Example: /place BTC LONG market SL 67000 TP 71000 RISK 3 5x\n\n"
            "/sl <SYMBOL> <price>     \u2014 Modify stop loss\n"
            "/tp <SYMBOL> <p1> [p2]   \u2014 Modify take profit prices\n"
            "/dca <SYMBOL> [price]    \u2014 Add/remove DCA order\n"
            "/entry <SYMBOL> <price>  \u2014 Modify entry (pending only)\n\n"
            "/status [SYMBOL]         \u2014 Show positions & P&L\n"
            "/close <SYMBOL|all>      \u2014 Close position(s)\n"
            "/help                    \u2014 This message"
        )

    # ---------- /status ----------

    async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        symbol = context.args[0].upper() if context.args else None
        if symbol and not symbol.endswith("USDT"):
            symbol += "USDT"
        try:
            result = await asyncio.to_thread(trade_manager.get_status, symbol)
            await update.message.reply_text(result)
        except Exception as e:
            log.exception("Error fetching status")
            await update.message.reply_text(f"Error: {e}")

    # ---------- /close ----------

    async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args

        if not args:
            await update.message.reply_text("Usage: /close <SYMBOL|all>")
            return

        if args[0].lower() == "all":
            symbols = trade_manager.db.all_active()
            if not symbols:
                await update.message.reply_text("No active positions to close.")
                return
            keyboard = [
                [InlineKeyboardButton("⚠️ Close all", callback_data="close_all:all")],
                [InlineKeyboardButton("Cancel", callback_data="cancel:all")],
            ]
            await update.message.reply_text(
                f"Close ALL {len(symbols)} active positions?",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        state = trade_manager.db.get(symbol)
        if not state or state["status"] != "active":
            await update.message.reply_text(f"No active position for {symbol}.")
            return

        keyboard = [
            [
                InlineKeyboardButton("✅ Close", callback_data=f"close:{symbol}"),
                InlineKeyboardButton("Cancel", callback_data=f"cancel:{symbol}"),
            ]
        ]
        await update.message.reply_text(
            f"Close {symbol} {state['position']}?",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

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
                result = await asyncio.to_thread(trade_manager.confirm, symbol)
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=result,
                )
            except Exception as e:
                log.exception("Error confirming trade")
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=f"⚠️ Error placing the trade for {symbol}: {e}",
                )
        elif action == "cancel":
            result = trade_manager.cancel(symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "confirm_mod":
            result = await asyncio.to_thread(trade_manager.apply_modification, symbol)
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
        elif action == "close":
            result = await asyncio.to_thread(trade_manager.close_position, symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=result,
            )
        elif action == "close_all":
            messages = []
            for sym in trade_manager.db.all_active():
                msg = await asyncio.to_thread(trade_manager.close_position, sym)
                messages.append(msg)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text="\n".join(messages),
            )

    # ---------- Register handlers ----------

    app.add_handler(CommandHandler("place", place_command))
    app.add_handler(CommandHandler("sl", sl_command))
    app.add_handler(CommandHandler("tp", tp_command))
    app.add_handler(CommandHandler("dca", dca_command))
    app.add_handler(CommandHandler("entry", entry_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("close", close_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    return app
