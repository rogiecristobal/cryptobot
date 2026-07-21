"""
Central config. Loads everything from .env — never hardcode secrets here.
"""
import os
from dotenv import load_dotenv

load_dotenv()

def _required(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}. Check your .env file.")
    return val

BYBIT_API_KEY = _required("BYBIT_API_KEY")
BYBIT_API_SECRET = _required("BYBIT_API_SECRET")
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

TELEGRAM_BOT_TOKEN = _required("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = int(_required("TELEGRAM_CHAT_ID"))

RISK_PERCENT = float(os.getenv("RISK_PERCENT", "3.0"))
if not 0 < RISK_PERCENT <= 100:
    raise RuntimeError(f"RISK_PERCENT must be between 0 and 100, got {RISK_PERCENT}")

DCA_SPLIT_RATIO = float(os.getenv("DCA_SPLIT_RATIO", "0.5"))
if not 0 <= DCA_SPLIT_RATIO <= 1:
    raise RuntimeError(f"DCA_SPLIT_RATIO must be between 0 and 1, got {DCA_SPLIT_RATIO}")

DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "10"))
if not 1 <= DEFAULT_LEVERAGE <= 100:
    raise RuntimeError(f"DEFAULT_LEVERAGE must be 1-100, got {DEFAULT_LEVERAGE}")

DEFAULT_MARGIN_MODE = os.getenv("DEFAULT_MARGIN_MODE", "ISOLATED").upper()
if DEFAULT_MARGIN_MODE not in ("ISOLATED", "CROSS"):
    raise RuntimeError(f"DEFAULT_MARGIN_MODE must be ISOLATED or CROSS, got {DEFAULT_MARGIN_MODE}")

CONFIRM_TIMEOUT_SECONDS = int(os.getenv("CONFIRM_TIMEOUT_SECONDS", "120"))
if CONFIRM_TIMEOUT_SECONDS <= 0:
    raise RuntimeError(f"CONFIRM_TIMEOUT_SECONDS must be positive, got {CONFIRM_TIMEOUT_SECONDS}")

BREAKEVEN_TIMEOUT_SECONDS = int(os.getenv("BREAKEVEN_TIMEOUT_SECONDS", "60"))
if BREAKEVEN_TIMEOUT_SECONDS <= 0:
    raise RuntimeError(f"BREAKEVEN_TIMEOUT_SECONDS must be positive, got {BREAKEVEN_TIMEOUT_SECONDS}")

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "trades.db")
