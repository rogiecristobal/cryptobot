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
DCA_SPLIT_RATIO = float(os.getenv("DCA_SPLIT_RATIO", "0.5"))  # fraction of total qty on the initial entry order when DCA is present
DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "10"))
DEFAULT_MARGIN_MODE = os.getenv("DEFAULT_MARGIN_MODE", "ISOLATED").upper()

CONFIRM_TIMEOUT_SECONDS = int(os.getenv("CONFIRM_TIMEOUT_SECONDS", "120"))
BREAKEVEN_TIMEOUT_SECONDS = int(os.getenv("BREAKEVEN_TIMEOUT_SECONDS", "60"))

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "trades.db")
