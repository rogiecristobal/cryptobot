"""
Public Bitunix futures market data: 4h klines and trading-pair precision.
No API keys needed; used by the worker and by the replay command.

Docs (checked): GET /api/v1/futures/market/kline
  symbol, interval (4h), endTime (exclusive, ms), limit (default 100, max 200),
  type (LAST_PRICE | MARK_PRICE). Returns only closed candles; time = open time.
GET /api/v1/futures/market/trading_pairs?symbols=...
  basePrecision (qty decimals), quotePrecision (price decimals), minTradeVolume.
"""
import logging
import os
import time
from decimal import Decimal
from typing import List, Optional

import certifi
import requests

from se.strategy import Candle, CANDLE_MS

log = logging.getLogger("se.market")

BASE_URL = "https://fapi.bitunix.com"
KLINE_MAX = 200


class MarketError(Exception):
    pass


def new_session() -> requests.Session:
    s = requests.Session()
    # Same CA handling as the bot's config.py (Termux can lose SSL_CERT_FILE).
    ca = os.environ.get("SSL_CERT_FILE")
    s.verify = ca if ca and os.path.isfile(ca) else certifi.where()
    return s


def _get(session: requests.Session, path: str, params: dict, attempts: int = 4):
    """Public GET with exponential backoff on network errors and rate limits."""
    delay = 1.0
    for attempt in range(attempts):
        try:
            resp = session.get(BASE_URL + path, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            code = payload.get("code")
            if code in (0, "0"):
                return payload.get("data")
            if str(code) in ("10005", "10006") and attempt < attempts - 1:  # rate limited
                raise requests.ConnectionError(f"rate limited ({code})")
            raise MarketError(f"Bitunix {path} error {code}: {payload.get('msg')}")
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == attempts - 1:
                raise
            log.warning("GET %s failed (%s) — retrying in %.0fs", path, e, delay)
            time.sleep(delay)
            delay *= 2


def _candle(d: dict) -> Candle:
    return Candle(t=int(d["time"]), o=Decimal(str(d["open"])), h=Decimal(str(d["high"])),
                  l=Decimal(str(d["low"])), c=Decimal(str(d["close"])))


def fetch_klines(session, symbol: str, end_ms: Optional[int] = None, limit: int = KLINE_MAX,
                 interval: str = "4h") -> List[Candle]:
    """Up to `limit` closed candles before end_ms (exclusive), oldest first."""
    params = {"symbol": symbol, "interval": interval, "limit": min(limit, KLINE_MAX), "type": "LAST_PRICE"}
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    data = _get(session, "/api/v1/futures/market/kline", params) or []
    candles = sorted((_candle(d) for d in data), key=lambda c: c.t)
    return candles


def fetch_history(session, symbol: str, count: Optional[int] = None, since_ms: Optional[int] = None,
                  now_ms: Optional[int] = None) -> List[Candle]:
    """
    Closed 4h candles, oldest first, paging backward 200 at a time: either the
    latest `count`, or everything from since_ms on. Only fully closed candles
    (open + 4h <= now) are kept.
    """
    now_ms = now_ms or int(time.time() * 1000)
    by_time = {}
    end = None
    while True:
        page = fetch_klines(session, symbol, end_ms=end)
        if not page:
            break
        for c in page:
            by_time[c.t] = c
        oldest = page[0].t
        if count is not None and len(by_time) >= count:
            break
        if since_ms is not None and oldest <= since_ms:
            break
        if end is not None and oldest >= end:
            break  # no progress — avoid looping forever
        end = oldest
        if len(page) < 2:
            break
    candles = sorted((c for c in by_time.values() if c.t + CANDLE_MS <= now_ms), key=lambda c: c.t)
    if since_ms is not None:
        candles = [c for c in candles if c.t >= since_ms]
    if count is not None:
        candles = candles[-count:]
    return candles


def fetch_instrument(session, symbol: str) -> dict:
    """{'price_step', 'qty_step', 'min_qty'} as Decimals, from Get trading pairs."""
    data = _get(session, "/api/v1/futures/market/trading_pairs", {"symbols": symbol})
    rows = data if isinstance(data, list) else [data] if data else []
    info = next((d for d in rows if d.get("symbol") == symbol), None)
    if not info:
        raise MarketError(f"{symbol} is not listed on Bitunix futures")
    price_dec = int(info.get("quotePrecision") or 0)
    qty_dec = int(info.get("basePrecision") or 0)
    return {
        "price_step": Decimal(1).scaleb(-price_dec),
        "qty_step": Decimal(1).scaleb(-qty_dec),
        "min_qty": Decimal(str(info.get("minTradeVolume") or "0")),
        "status": str(info.get("symbolStatus") or "OPEN").upper(),
    }


def fetch_last_price(session, symbol: str) -> Decimal:
    data = _get(session, "/api/v1/futures/market/tickers", {"symbols": symbol})
    rows = data if isinstance(data, list) else [data] if data else []
    t = next((d for d in rows if d.get("symbol") == symbol), None) or {}
    last = t.get("lastPrice") or t.get("last")
    if not last or Decimal(str(last)) <= 0:
        raise MarketError(f"No last price for {symbol} from Bitunix")
    return Decimal(str(last))
