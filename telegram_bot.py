"""
Telegram side. Only messages from TELEGRAM_CHAT_ID are ever acted on —
this is the single most important security boundary in the whole bot.
"""
import logging
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, ContextTypes, filters
from signal_parser import parse_signal, ParsedSignal
import ocr
import config

log = logging.getLogger("telegram_bot")


def _authorized(update: Update) -> bool:
    return update.effective_chat.id == config.TELEGRAM_CHAT_ID


def _parse_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


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


async def _stage_and_reply(update: Update, trade_manager, text: str):
    signal = parse_signal(text)
    await _stage_signal(update, trade_manager, signal)


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

    # ---------- /help ----------

    async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        lines = [
            "Available commands:",
            "",
            "/help — Show this message",
            "",
            "/place <asset> <dir> <entry|market> <dca|none> <sl> <tp1> <tp2> ... [leverax]",
            "  Stage a new trade from inline args.",
            "  Example: /place BTC LONG 69000 none 67000 71000 72000 5x",
            "",
            "/sl <asset> <price>",
            "  Modify stop loss on a staged or active trade.",
            "",
            "/tp <asset> <price1> <price2> ...",
            "  Replace all take-profit levels.",
            "",
            "/dca <asset> <price|none>",
            "  Add, update, or remove a DCA limit order.",
            "",
            "/entry <asset> <price|market>",
            "  Modify entry (only before fill).",
            "",
            "High-cap assets (BTC, ETH, SOL, BNB, XRP, ADA) use 1.5% risk per position",
            "  (3% max with DCA). Other assets use config.RISK_PERCENT.",
            "",
            "You can also paste a signal text directly, or send a screenshot.",
            "Tap Confirm/Cancel on any prompt to execute or discard.",
        ]
        await update.message.reply_text("\n".join(lines))

    # ---------- /start ----------

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        screenshot_note = " or send a screenshot" if ocr.is_enabled() else ""
        keyboard = [
            [InlineKeyboardButton("❓ Help", callback_data="help:_")],
            [
                InlineKeyboardButton("📊 Status", callback_data="status:_"),
                InlineKeyboardButton("💰 Balance", callback_data="balance:_"),
            ],
        ]
        await update.message.reply_text(
            f"Bot online. Paste a signal{screenshot_note} to stage it, "
            f"then tap Confirm/Cancel within {config.CONFIRM_TIMEOUT_SECONDS}s.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # ---------- /place ----------

    async def place_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args

        if len(args) < 6:
            await update.message.reply_text(
                "Usage: /place <asset> <direction> <entry|market> <dca|none> <sl> <tp1> <tp2> ... [leverax]\n"
                "Example: /place BTC LONG 69000 none 67000 71000 72000 5x"
            )
            return

        # Parse optional leverage from last arg
        leverage = config.DEFAULT_LEVERAGE
        if args[-1].lower().endswith("x"):
            try:
                leverage = int(float(args[-1][:-1]))
                args = args[:-1]
            except ValueError:
                await update.message.reply_text(f"Invalid leverage: {args[-1]}")
                return

        if len(args) < 6:
            await update.message.reply_text("Need at least: asset, direction, entry, dca, sl, and one TP.")
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

        dca_str = args[3].lower()
        dca = None if dca_str in ("none", "n", "0") else _parse_float(dca_str)
        if dca_str not in ("none", "n", "0") and dca is None:
            await update.message.reply_text(f"Invalid DCA price: {args[3]}")
            return

        sl = _parse_float(args[4])
        if sl is None:
            await update.message.reply_text(f"Invalid SL: {args[4]}")
            return

        tps = []
        for tp_str in args[5:]:
            tp = _parse_float(tp_str)
            if tp is None:
                await update.message.reply_text(f"Invalid TP: {tp_str}")
                return
            tps.append(tp)

        if not tps:
            await update.message.reply_text("Need at least one TP.")
            return

        # Validate direction consistency with TP/SL
        if not entry_is_market:
            if direction == "LONG":
                if sl >= entry:
                    await update.message.reply_text("For LONG, SL must be below entry.")
                    return
                for tp in tps:
                    if tp <= entry:
                        await update.message.reply_text(f"For LONG, TP {tp} must be above entry {entry}.")
                        return
                if dca and dca >= entry:
                    await update.message.reply_text(f"For LONG, DCA {dca} must be below entry {entry}.")
                    return
            else:
                if sl <= entry:
                    await update.message.reply_text("For SHORT, SL must be above entry.")
                    return
                for tp in tps:
                    if tp >= entry:
                        await update.message.reply_text(f"For SHORT, TP {tp} must be below entry {entry}.")
                        return
                if dca and dca <= entry:
                    await update.message.reply_text(f"For SHORT, DCA {dca} must be above entry {entry}.")
                    return

        errors = []
        if not asset:
            errors.append("Invalid asset.")
        if not tps:
            errors.append("Need at least one TP.")

        signal = ParsedSignal(
            asset=asset,
            position=direction,
            entry=entry,
            entry_is_market=entry_is_market,
            dca=dca,
            leverage=leverage,
            sl=sl,
            tps=tps,
            errors=errors,
        )

        await _stage_signal(update, trade_manager, signal)

    # ---------- /sl, /tp, /dca, /entry ----------

    async def _show_mod_preview(update: Update, trade_manager, symbol: str, preview: str, mod_type: str, params: dict):
        """Show a modification preview with Confirm/Cancel buttons and store in pending_mods."""
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_mod:{symbol}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_mod:{symbol}"),
            ]
        ]
        sent_msg = await update.message.reply_text(preview, reply_markup=InlineKeyboardMarkup(keyboard))
        trade_manager.pending_mods[symbol] = {
            "type": mod_type,
            "params": params,
            "chat_id": sent_msg.chat_id,
            "message_id": sent_msg.message_id,
        }

    async def _handle_staged_mod(update: Update, context: ContextTypes.DEFAULT_TYPE, trade_manager, symbol: str, mod_type: str, params: dict):
        """Apply a modification directly on a staged trade, editing the existing confirm card in place."""
        signal = trade_manager.pending[symbol]["signal"]
        old_msg_id = trade_manager.pending[symbol].get("message_id")
        old_chat_id = trade_manager.pending[symbol].get("chat_id")

        if mod_type == "sl":
            signal.sl = params["new_price"]
        elif mod_type == "tp":
            signal.tps = list(params["new_prices"])
        elif mod_type == "dca":
            signal.dca = params["new_price"]
        elif mod_type == "entry":
            if params["is_market"]:
                signal.entry = None
                signal.entry_is_market = True
            else:
                signal.entry = params["new_price"]
                signal.entry_is_market = False

        trade_manager.pending[symbol]["signal"] = signal
        qty_entry, qty_dca = trade_manager._calc_qty(signal)
        lines = [
            f"⚠️ Confirm trade — tap below within {config.CONFIRM_TIMEOUT_SECONDS}s",
            f"{symbol} ({signal.position})",
            f"Entry: {'MARKET' if signal.entry_is_market else signal.entry}  (qty ~{qty_entry})",
        ]
        if signal.dca:
            lines.append(f"DCA: {signal.dca}  (qty ~{qty_dca})")
        lines.append(f"SL: {signal.sl}")
        lines.append(f"TPs: {', '.join(str(t) for t in signal.tps)}")
        lines.append(f"Leverage: {signal.leverage}x ({signal.leverage_mode or config.DEFAULT_MARGIN_MODE})")
        prompt = "\n".join(lines)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{symbol}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{symbol}"),
            ]
        ])
        try:
            if old_msg_id and old_chat_id:
                await context.bot.edit_message_text(chat_id=old_chat_id, message_id=old_msg_id, text=prompt, reply_markup=keyboard)
            else:
                sent_msg = await update.message.reply_text(prompt, reply_markup=keyboard)
                trade_manager.pending[symbol]["chat_id"] = sent_msg.chat_id
                trade_manager.pending[symbol]["message_id"] = sent_msg.message_id
        except Exception:
            sent_msg = await update.message.reply_text(prompt, reply_markup=keyboard)
            trade_manager.pending[symbol]["chat_id"] = sent_msg.chat_id
            trade_manager.pending[symbol]["message_id"] = sent_msg.message_id

    async def sl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /sl <asset> <price>\nExample: /sl BTC 65000")
            return

        asset = args[0].upper()
        if not asset.endswith("USDT"):
            asset += "USDT"

        new_sl = _parse_float(args[1])
        if new_sl is None:
            await update.message.reply_text(f"Invalid price: {args[1]}")
            return

        if asset in trade_manager.pending:
            await _handle_staged_mod(update, context, trade_manager, asset, "sl", {"new_price": new_sl})
        else:
            try:
                preview = trade_manager.stage_modify_sl(asset, new_sl)
                await _show_mod_preview(update, trade_manager, asset, preview, "sl", {"new_price": new_sl})
            except ValueError as e:
                await update.message.reply_text(str(e))

    async def tp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /tp <asset> <price1> <price2> ...\nExample: /tp BTC 71000 73000 75000")
            return

        asset = args[0].upper()
        if not asset.endswith("USDT"):
            asset += "USDT"

        prices = []
        for s in args[1:]:
            p = _parse_float(s)
            if p is None:
                await update.message.reply_text(f"Invalid price: {s}")
                return
            prices.append(p)

        if not prices:
            await update.message.reply_text("Need at least one TP price.")
            return

        if asset in trade_manager.pending:
            await _handle_staged_mod(update, context, trade_manager, asset, "tp", {"new_prices": prices})
        else:
            try:
                preview = trade_manager.stage_modify_tp(asset, prices)
                await _show_mod_preview(update, trade_manager, asset, preview, "tp", {"new_prices": prices})
            except ValueError as e:
                await update.message.reply_text(str(e))

    async def dca_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /dca <asset> <price|none>\nExample: /dca BTC 65000\n         /dca BTC none")
            return

        asset = args[0].upper()
        if not asset.endswith("USDT"):
            asset += "USDT"

        dca_str = args[1].lower()
        if dca_str in ("none", "n", "0"):
            dca_price = None
        else:
            dca_price = _parse_float(dca_str)
            if dca_price is None:
                await update.message.reply_text(f"Invalid price: {args[1]}")
                return

        if asset in trade_manager.pending:
            await _handle_staged_mod(update, context, trade_manager, asset, "dca", {"new_price": dca_price})
        else:
            try:
                preview = trade_manager.stage_modify_dca(asset, dca_price)
                await _show_mod_preview(update, trade_manager, asset, preview, "dca", {"new_price": dca_price})
            except ValueError as e:
                await update.message.reply_text(str(e))

    async def entry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not _authorized(update):
            return
        trade_manager = manager_ref.tm
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: /entry <asset> <price|market>\nExample: /entry BTC 68000\n         /entry BTC market")
            return

        asset = args[0].upper()
        if not asset.endswith("USDT"):
            asset += "USDT"

        entry_str = args[1].lower()
        is_market = entry_str in ("market", "now")
        new_price = None if is_market else _parse_float(entry_str)
        if not is_market and new_price is None:
            await update.message.reply_text(f"Invalid entry: {args[1]}")
            return

        if asset not in trade_manager.pending:
            await update.message.reply_text("Entry can only be modified before confirmation (no pending trade found).")
            return

        await _handle_staged_mod(update, context, trade_manager, asset, "entry", {"new_price": new_price, "is_market": is_market})

    # ---------- Text / photo messages ----------

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

        photo = update.message.photo[-1]
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

        await update.message.reply_text(f"Transcribed:\n{text}")
        await _stage_and_reply(update, trade_manager, text)

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
        elif action == "help":
            lines = [
                "Available commands:",
                "",
                "/help — Show this message",
                "",
                "/place <asset> <dir> <entry|market> <dca|none> <sl> <tp1> <tp2> ... [leverax]",
                "  Stage a new trade from inline args.",
                "",
                "/sl <asset> <price>",
                "  Modify stop loss on a staged or active trade.",
                "",
                "/tp <asset> <price1> <price2> ...",
                "  Replace all take-profit levels.",
                "",
                "/dca <asset> <price|none>",
                "  Add, update, or remove a DCA limit order.",
                "",
                "/entry <asset> <price|market>",
                "  Modify entry (only before fill).",
                "",
                "High-cap assets (BTC, ETH, SOL, BNB, XRP, ADA) use 1.5% risk per position",
                "  (3% max with DCA). Other assets use config.RISK_PERCENT.",
            ]
            await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
        elif action == "status":
            syms = trade_manager.db.all_active()
            if not syms:
                await context.bot.send_message(chat_id=chat_id, text="No active positions.")
            else:
                parts = ["📊 Active Positions:"]
                for sym in syms:
                    state = trade_manager.db.get(sym)
                    pos = trade_manager.bybit.get_open_position(sym)
                    if pos:
                        side = "LONG" if pos.get("side") == "Buy" else "SHORT"
                        size = float(pos.get("size", 0))
                        entry = float(pos.get("entryPrice", 0))
                        mark = float(pos.get("markPrice", 0))
                        upnl = float(pos.get("unrealisedPnl", 0))
                        upnl_pct = (mark - entry) / entry * 100 * (1 if side == "LONG" else -1)
                    else:
                        side = "LONG" if (state or {}).get("position") == "LONG" else "SHORT"
                        size = entry = mark = upnl = upnl_pct = None
                    parts.append(f"")
                    parts.append(f"{sym} {side}")
                    if size is not None:
                        parts.append(f"  Size: {size:.4f} | Entry: {entry:.2f}")
                        parts.append(f"  Mark: {mark:.2f} | P&L: {upnl:+.2f} ({upnl_pct:+.2f}%)")
                    else:
                        parts.append(f"  Size: N/A (position data offline)")
                    sl = state.get("sl_price") if state else None
                    bm = state.get("breakeven_moved", 0) if state else 0
                    if sl:
                        if bm:
                            orig = state.get("original_sl_price", sl)
                            parts.append(f"  SL: ✅ Breakeven (moved from {orig:.2f})")
                        else:
                            parts.append(f"  SL: {sl:.2f} ⬜")
                    filled = trade_manager.db.loads(state.get("filled_tp_prices")) if state and state.get("filled_tp_prices") else []
                    pending = trade_manager.db.loads(state["tp_prices"]) if state and state.get("tp_prices") else []
                    tp_parts = []
                    for p in filled:
                        tp_parts.append(f"{p:.2f} ✅")
                    for p in pending:
                        tp_parts.append(f"{p:.2f} ⬜")
                    if tp_parts:
                        parts.append(f"  TPs: {' | '.join(tp_parts)}")
                await context.bot.send_message(chat_id=chat_id, text="\n".join(parts))
        elif action == "balance":
            try:
                info = trade_manager.bybit.get_wallet_info()
                lines = [
                    "💰 Balance:",
                    f"  Equity: ${info['equity']:,.2f}",
                    f"  Available: ${info['available']:,.2f}",
                ]
                await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
            except Exception as e:
                log.exception("Error fetching balance")
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Could not fetch balance: {e}")

    # ---------- Register handlers ----------

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("place", place_command))
    app.add_handler(CommandHandler("sl", sl_command))
    app.add_handler(CommandHandler("tp", tp_command))
    app.add_handler(CommandHandler("dca", dca_command))
    app.add_handler(CommandHandler("entry", entry_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    return app
