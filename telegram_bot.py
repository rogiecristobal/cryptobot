"""
Telegram side. Only messages from TELEGRAM_CHAT_ID are ever acted on —
this is the single most important security boundary in the whole bot.
"""
import asyncio
import logging
import math
import platform
import time
from typing import Optional, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Conflict
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from signal_parser import ParsedSignal
import config

log = logging.getLogger("telegram_bot")


def _authorized(update: Update) -> bool:
    return update.effective_chat.id == config.TELEGRAM_CHAT_ID


def _parse_float(s: str) -> Optional[float]:
    try:
        val = float(s)
    except (ValueError, TypeError):
        return None
    return val if math.isfinite(val) else None  # "nan"/"inf" parse as floats but aren't prices


_LABELS = {"sl", "tp", "dca", "risk", "trail"}


async def _stage_signal(update: Update, trade_manager, signal: ParsedSignal, note: str = ""):
    """Stage a pre-built ParsedSignal and reply with the confirmation card.
    A card it replaces (same symbol, not yet confirmed) is marked as replaced."""
    old = trade_manager.get_pending(signal.asset)
    old_card = (old["chat_id"], old["message_id"]) if old and old.get("message_id") else None
    try:
        prompt, token = await asyncio.to_thread(trade_manager.stage_signal, signal)
        if note:
            prompt = f"{note}\n\n{prompt}"
        # The token ties the buttons to this exact trade: a newer card for the
        # same symbol replaces it, and this card's buttons then do nothing.
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{signal.asset}:{token}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{signal.asset}:{token}"),
            ]
        ]
        sent_msg = await update.message.reply_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard))
        trade_manager.set_pending_metadata(signal.asset, sent_msg.chat_id, sent_msg.message_id)
        if old_card:
            try:
                await update.get_bot().edit_message_text(
                    chat_id=old_card[0], message_id=old_card[1],
                    text=f"✏️ Replaced by a newer {signal.asset} card below — use that one.")
            except Exception:
                pass
    except ValueError as e:
        await update.message.reply_text(str(e))
    except Exception as e:
        log.exception("Unexpected error staging signal")
        await update.message.reply_text(f"Something went wrong staging that signal: {e}")


async def _stage_modification(update: Update, trade_manager, mod_func, *args):
    """Generic helper to stage any modification command and reply with a confirmation card."""
    try:
        prompt, token = await asyncio.to_thread(mod_func, *args)
        symbol = args[0]
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_mod:{symbol}:{token}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_mod:{symbol}:{token}"),
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
                "  /place <ASSET> LONG|SHORT <ENTRY|market> SL <SL> [TP <TP1> [TP2 ...]] [DCA <DCA>] [RISK <%>] [LEVERAGEx] [TRAIL [mult]]\n"
                "Examples:\n"
                "  /place BTC LONG 69000 SL 67000 TP 71000 5x\n"
                "  /place BTC LONG 69000 SL 67000 5x\n"
                "  /place ETH LONG 3500 SL 3400 TP 3600 DCA 3450 RISK 5 3x\n"
                "  /place SOL LONG market SL 140\n"
                "  /place BTC LONG market SL 67000 TP 71000 TRAIL 2\n"
                "  /place BTC LONG 69000 SL 67000 TP 71000 72000 73000"
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
        tps: List[float] = []
        dca = None
        risk_pct = None
        leverage = config.DEFAULT_LEVERAGE
        trailing_mult = None

        rest = args[3:]
        i = 0
        while i < len(rest):
            token = rest[i]
            label = token.lower()
            if label == "trail":
                if i + 1 < len(rest):
                    try:
                        val = float(rest[i + 1])
                        if val > 0:
                            trailing_mult = val
                            i += 1
                    except (ValueError, TypeError):
                        pass
                if trailing_mult is None:
                    trailing_mult = 1.0
                i += 1
                continue
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
                    # TP takes one or more prices: TP 71000 72000 73000
                    tps.append(val)
                    while i + 1 < len(rest) and _parse_float(rest[i + 1]) is not None:
                        i += 1
                        tps.append(_parse_float(rest[i]))
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

        # Entry/SL/TP/DCA ordering is checked in stage_signal, so re-staged cards get it too.
        signal = ParsedSignal(
            asset=asset,
            position=direction,
            entry=entry,
            entry_is_market=entry_is_market,
            dca=dca,
            leverage=leverage,
            margin_percent=risk_pct,
            trailing_r_mult=trailing_mult,
            raw_text=update.message.text,
            sl=sl,
            tps=tps,
            errors=[],
        )

        await _stage_signal(update, trade_manager, signal)

    # ---------- /sl, /tp, /dca, /entry ----------

    async def _change(update: Update, symbol: str, note: str, mod_func, mod_args: tuple, **changes):
        """
        A trade still waiting for Confirm gets a fresh card with the change —
        re-validated and re-sized, replacing the old card. An active trade gets
        a modification card instead (mod_func None: not possible once active).
        """
        trade_manager = manager_ref.tm
        amended = trade_manager.amend_pending(symbol, **changes)
        if amended is not None:
            await _stage_signal(update, trade_manager, amended, note=note)
        elif mod_func is None:
            await update.message.reply_text(
                f"No unconfirmed trade for {symbol} — the entry can only be changed before you tap Confirm.")
        else:
            await _stage_modification(update, trade_manager, mod_func, symbol, *mod_args)

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
        await _change(update, symbol, f"✏️ SL changed to {price}.", manager_ref.tm.stage_modify_sl, (price,),
                      sl=price)

    async def tp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        args = context.args
        if len(args) < 2:
            await update.message.reply_text(
                "Usage: /tp <SYMBOL> <tp1> [tp2 tp3 ...]\n"
                "Only the last TP closes the position. Earlier TPs just move the SL:\n"
                "to entry when price reaches the first, then to the previous TP at each later one."
            )
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
        await _change(update, symbol, f"✏️ TPs changed to {', '.join(str(p) for p in prices)}.",
                      manager_ref.tm.stage_modify_tp, (prices,), tps=prices)

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
            price, note = None, "✏️ DCA removed."
        else:
            price = _parse_float(args[1])
            if price is None:
                await update.message.reply_text(f"Invalid price: {args[1]}")
                return
            note = f"✏️ DCA set to {price}."
        await _change(update, symbol, note, manager_ref.tm.stage_modify_dca, (price,), dca=price)

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
        note = "✏️ Entry changed to MARKET." if is_market else f"✏️ Entry changed to {new_price}."
        await _change(update, symbol, note, None, (), entry=new_price, entry_is_market=is_market)

    # ---------- /trail ----------

    async def trail_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args
        if len(args) < 1:
            await update.message.reply_text(
                "Usage:\n"
                "  /trail <SYMBOL> [mult]   \u2014 Set trailing at R \u00d7 mult (default 1R)\n"
                "  /trail <SYMBOL> off      \u2014 Cancel trailing, SL stays where it is\n\n"
                "Examples:\n"
                "  /trail BTCUSDT        \u2014 1R trailing\n"
                "  /trail BTCUSDT 2      \u2014 2R trailing\n"
                "  /trail BTCUSDT 0.5    \u2014 0.5R trailing\n"
                "  /trail BTCUSDT off    \u2014 Cancel"
            )
            return
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        if len(args) >= 2 and args[1].lower() in ("off", "cancel", "none", "0"):
            try:
                result = await asyncio.to_thread(trade_manager.cancel_trailing, symbol)
                await update.message.reply_text(result)
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")
            return
        mult = 1.0
        if len(args) >= 2:
            try:
                mult = float(args[1])
                if mult <= 0:
                    raise ValueError
            except ValueError:
                await update.message.reply_text(
                    f"Invalid multiplier: {args[1]}. Use a positive number or 'off'."
                )
                return
        await _stage_modification(update, trade_manager,
                                  trade_manager.stage_modify_trail, symbol, mult)

    # ---------- /help ----------

    async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        await update.message.reply_text(
            "🤖 CryptoBot Commands\n\n"
            "/place <ASSET> LONG|SHORT <ENTRY|market> SL <SL> [options]\n"
            "  Options: TP <p1> [p2 p3] DCA <price> RISK <%> LEVERAGEx\n"
            "  Example: /place BTC LONG market SL 67000 TP 71000 RISK 3 5x\n\n"
            "/sl <SYMBOL> <price>     \u2014 Modify stop loss\n"
            "/tp <SYMBOL> <p1> [p2 p3] \u2014 Set TPs (last one closes; earlier ones move SL)\n"
            "/dca <SYMBOL> [price]    \u2014 Add/remove DCA order\n"
            "/entry <SYMBOL> <price>  \u2014 Modify entry (pending only)\n"
            "/trail <SYMBOL> [mult]   \u2014 Set trailing stop (default 1R)\n"
            "/trail <SYMBOL> off      \u2014 Cancel trailing (SL stays put)\n\n"
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
                [InlineKeyboardButton("Cancel", callback_data="close_cancel:all")],
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
                InlineKeyboardButton("Cancel", callback_data=f"close_cancel:{symbol}"),
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
        # action:symbol[:token] — cards from before tokens existed have none and count as outdated.
        action, symbol, *rest = query.data.split(":")
        token = rest[0] if rest else None
        trade_manager = manager_ref.tm
        chat_id = query.message.chat_id
        message_id = query.message.message_id

        if action == "confirm":
            try:
                result = await asyncio.to_thread(trade_manager.confirm, symbol, token)
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
            result = trade_manager.cancel(symbol, token)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "confirm_mod":
            result = await asyncio.to_thread(trade_manager.apply_modification, symbol, token)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "cancel_mod":
            result = trade_manager.cancel_modification(symbol, token)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=result,
            )
        elif action == "breakeven_yes":
            ok = await asyncio.to_thread(trade_manager.apply_breakeven, symbol)
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=(f"✅ SL moved to entry for {symbol}." if ok
                      else f"⚠️ Could not move SL to entry for {symbol} — check Bitunix."),
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
        elif action == "close_cancel":
            # Only edits the card: a pending trade for the same symbol is left alone.
            text = "Close cancelled." if symbol == "all" else f"Close cancelled — {symbol} stays open."
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)

    # ---------- Errors ----------

    last_conflict_alert = [0.0]

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        err = context.error
        if isinstance(err, Conflict):
            # Another process is polling with this token. Both copies manage the
            # same Bitunix account with their own state and fight over SLs.
            log.error("Telegram polling conflict — another copy of the bot is running: %s", err)
            if time.time() - last_conflict_alert[0] > 600:
                last_conflict_alert[0] = time.time()
                try:
                    await context.bot.send_message(
                        chat_id=config.TELEGRAM_CHAT_ID,
                        text=(f"⚠️ Another copy of this bot is running with the same token "
                              f"(this one: {platform.node()}). Both manage the same Bitunix "
                              f"positions and will overwrite each other's SLs. Stop one of them."))
                except Exception as e:
                    log.warning("Could not send the conflict alert: %s", e)
            return
        if not isinstance(update, Update):
            # Polling/network errors: PTB retries on its own.
            log.warning("Telegram error: %s", err)
            return
        log.error("Unhandled error in Telegram handler", exc_info=err)
        # Tell the user, so a failed button press doesn't look like nothing happened.
        if update.effective_chat and update.effective_chat.id == config.TELEGRAM_CHAT_ID:
            try:
                await context.bot.send_message(chat_id=config.TELEGRAM_CHAT_ID,
                                               text=f"⚠️ Something went wrong: {err}\nCheck Bitunix and /status.")
            except Exception:
                pass

    # ---------- Register handlers ----------

    app.add_handler(CommandHandler("place", place_command))
    app.add_handler(CommandHandler("sl", sl_command))
    app.add_handler(CommandHandler("tp", tp_command))
    app.add_handler(CommandHandler("dca", dca_command))
    app.add_handler(CommandHandler("entry", entry_command))
    app.add_handler(CommandHandler("trail", trail_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("close", close_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)
    return app
