"""
Sweep & Engulf settings, read from SE_* keys in .env.

The defaults match the backtest. This module does not import the bot's own
config.py, so the replay command and the tests run without any API keys.
"""
import os
from dataclasses import dataclass, field
from decimal import Decimal

from dotenv import load_dotenv

load_dotenv()

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BOT_DIR, "data")
LOG_DIR = os.path.join(BOT_DIR, "logs")
STATE_DB_PATH = os.path.join(DATA_DIR, "se_state.db")
LOG_PATH = os.path.join(LOG_DIR, "se.log")
# Kill switch: while a file named STOP exists in the bot folder, the worker
# opens no new trades. Open trades keep their TP/SL and stay managed.
STOP_FILE = os.path.join(BOT_DIR, "STOP")

TIMEFRAME = "4h"
CANDLE_MS = 4 * 60 * 60 * 1000
LIVE_CONFIRM_PHRASE = "I_ACCEPT_THE_RISK"

PREV_CANDLE_MODES = ("same", "three_in_a_row", "mixed")
SL_MODES = ("atr", "candle")


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else default


def _dec(name: str, default: str) -> Decimal:
    raw = _str(name, default)
    try:
        return Decimal(raw)
    except Exception:
        raise RuntimeError(f"{name} must be a number, got {raw!r}")


def _int(name: str, default: int) -> int:
    raw = _str(name, str(default))
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be a whole number, got {raw!r}")


@dataclass
class SEConfig:
    enabled: bool = True
    mode: str = "paper"                      # paper | live (the starting mode; /se_mode changes it)
    symbols: list = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    budget_usdt: Decimal = Decimal("100")
    risk_pct: Decimal = Decimal("3.0")
    compound: bool = True
    prev_candle_mode: str = "same"           # same | three_in_a_row | mixed
    ema_filter: bool = True
    ema_length: int = 200
    sl_mode: str = "atr"                     # atr | candle
    atr_length: int = 14
    atr_mult: Decimal = Decimal("1.0")
    rr: Decimal = Decimal("4.0")
    direction: str = "long"                  # long only
    daily_loss_limit: int = 2                # losses in a row; 0 = off
    min_order_usdt: Decimal = Decimal("10")
    late_entry_minutes: int = 15
    # Fee per side used by paper mode and the replay (Bitunix taker is 0.06%).
    # Live trades use the fees Bitunix actually charged.
    fee_pct: Decimal = Decimal("0.06")
    # Bug-catcher caps (approved in Step 0): no single position, and no total
    # of open positions, may be worth more than this multiple of strategy equity.
    max_position_equity_mult: Decimal = Decimal("1.05")
    # Live mode needs this to equal I_ACCEPT_THE_RISK *and* a Telegram confirm.
    live_confirm: str = ""

    def validate(self):
        if self.mode not in ("paper", "live"):
            raise RuntimeError(f"SE_MODE must be paper or live, got {self.mode}")
        if self.prev_candle_mode not in PREV_CANDLE_MODES:
            raise RuntimeError(f"SE_PREV_CANDLE_MODE must be one of {PREV_CANDLE_MODES}, got {self.prev_candle_mode}")
        if self.sl_mode not in SL_MODES:
            raise RuntimeError(f"SE_SL_MODE must be one of {SL_MODES}, got {self.sl_mode}")
        if self.direction != "long":
            raise RuntimeError("SE_DIRECTION must be long: this worker runs long only")
        if not self.symbols:
            raise RuntimeError("SE_SYMBOLS is empty")
        if self.budget_usdt <= 0:
            raise RuntimeError(f"SE_BUDGET_USDT must be positive, got {self.budget_usdt}")
        if not Decimal(0) < self.risk_pct <= Decimal(100):
            raise RuntimeError(f"SE_RISK_PCT must be between 0 and 100, got {self.risk_pct}")
        if self.ema_length < 1 or self.atr_length < 1:
            raise RuntimeError("SE_EMA_LENGTH and SE_ATR_LENGTH must be at least 1")
        if self.atr_mult <= 0 or self.rr <= 0:
            raise RuntimeError("SE_ATR_MULT and SE_RR must be positive")
        if self.daily_loss_limit < 0:
            raise RuntimeError("SE_DAILY_LOSS_LIMIT must be 0 (off) or more")
        if self.late_entry_minutes < 1:
            raise RuntimeError("SE_LATE_ENTRY_MINUTES must be at least 1")
        if self.fee_pct < 0:
            raise RuntimeError("SE_FEE_PCT can't be negative")
        if self.max_position_equity_mult < 1:
            raise RuntimeError("SE_MAX_POSITION_EQUITY_MULT must be at least 1")
        return self

    @property
    def live_allowed(self) -> bool:
        return self.live_confirm == LIVE_CONFIRM_PHRASE

    def summary(self) -> str:
        return (f"symbols {','.join(self.symbols)} {TIMEFRAME} | budget ${self.budget_usdt} | "
                f"risk {self.risk_pct}% ({'compound' if self.compound else 'fixed'}) | "
                f"prev candle {self.prev_candle_mode} | EMA{self.ema_length} filter "
                f"{'on' if self.ema_filter else 'off'} | SL {self.sl_mode}"
                + (f" ATR{self.atr_length}x{self.atr_mult}" if self.sl_mode == "atr" else "")
                + f" | RR {self.rr} | daily loss limit "
                + (f"{self.daily_loss_limit}" if self.daily_loss_limit else "off")
                + f" | min order ${self.min_order_usdt} | late after {self.late_entry_minutes} min")


def load() -> SEConfig:
    symbols = [s.strip().upper() for s in _str("SE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
    return SEConfig(
        enabled=_bool("SE_ENABLED", True),
        mode=_str("SE_MODE", "paper").lower(),
        symbols=symbols,
        budget_usdt=_dec("SE_BUDGET_USDT", "100"),
        risk_pct=_dec("SE_RISK_PCT", "3.0"),
        compound=_bool("SE_COMPOUND", True),
        prev_candle_mode=_str("SE_PREV_CANDLE_MODE", "same").lower(),
        ema_filter=_bool("SE_EMA_FILTER", True),
        ema_length=_int("SE_EMA_LENGTH", 200),
        sl_mode=_str("SE_SL_MODE", "atr").lower(),
        atr_length=_int("SE_ATR_LENGTH", 14),
        atr_mult=_dec("SE_ATR_MULT", "1.0"),
        rr=_dec("SE_RR", "4.0"),
        direction=_str("SE_DIRECTION", "long").lower(),
        daily_loss_limit=_int("SE_DAILY_LOSS_LIMIT", 2),
        min_order_usdt=_dec("SE_MIN_ORDER_USDT", "10"),
        late_entry_minutes=_int("SE_LATE_ENTRY_MINUTES", 15),
        fee_pct=_dec("SE_FEE_PCT", "0.06"),
        max_position_equity_mult=_dec("SE_MAX_POSITION_EQUITY_MULT", "1.05"),
        live_confirm=_str("SE_LIVE_CONFIRM", ""),
    ).validate()
