"""
Parses raw pasted signal text into a structured dict.
This mirrors the exact detection logic from the web formatter app,
so a signal that renders correctly there will parse identically here.
"""
import re
from dataclasses import dataclass, field
from typing import Optional, List
 
 
@dataclass
class ParsedSignal:
    asset: Optional[str] = None
    position: Optional[str] = None          # "LONG" or "SHORT"
    entry: Optional[float] = None
    entry_is_market: bool = False
    entry_range: Optional[List[float]] = None
    dca: Optional[float] = None
    leverage: int = 10
    leverage_mode: Optional[str] = None      # "Cross" / "Isolated" / None
    sl: Optional[float] = None
    margin_percent: Optional[float] = None   # informational only; risk sizing uses config.RISK_PERCENT
    tps: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
 
 
def _first_num(s: str) -> Optional[str]:
    m = re.search(r"\d+(?:\.\d+)?", s)
    return m.group(0) if m else None
 
 
def _all_nums(s: str) -> List[str]:
    return re.findall(r"\d+(?:\.\d+)?", s)
 
 
def extract_tps(text: str) -> List[float]:
    re1 = re.compile(r"TP\s*\d+\s*[:.\)]?\s*\$?(\d+(?:\.\d+)?)", re.IGNORECASE)
    matches = re1.findall(text)
    if matches:
        return [float(m) for m in matches]
 
    label = re.search(r"take\s*profits?\s*:?|targets?\s*:|tp\s*:", text, re.IGNORECASE)
    if not label:
        return []
 
    block = text[label.end():]
    stop = re.search(r"stop\s*loss|\bsl\b|leverage|margin|\brisk\b", block, re.IGNORECASE)
    if stop:
        block = block[:stop.start()]
 
    tps: List[float] = []
    for line in [l.strip() for l in block.split("\n") if l.strip()]:
        line = re.sub(r"^\d+[.\)]+\s+", "", line)   # strip "1. " / "1.) " list markers only (space required, so decimals like 0.0875 aren't touched)
        parts = re.split(r",|(?:\s-\s)|–", line)
        for part in parts:
            part = part.strip()
            n = _first_num(part)
            if n:
                tps.append(float(n))
    return tps
 
 
def parse_signal(text: str) -> ParsedSignal:
    data = ParsedSignal()
 
    # OCR frequently swaps ':' for '.', drops it, or adds stray punctuation
    # (e.g. "Entry. 69000" instead of "Entry: 69000") — SEP tolerates that.
    SEP = r"[:.\s]+"
 
    m = re.search(r"asset" + SEP + r"([A-Za-z0-9]{2,10}\s*/\s*[A-Za-z0-9]{2,10})", text, re.IGNORECASE)
    if not m:
        m = re.search(r"\b([A-Z0-9]{2,10}\s*/\s*[A-Z0-9]{2,10})\b", text)
    if m:
        # Bybit's API expects "BTCUSDT", not "BTC/USDT" — strip the slash here so
        # every downstream API call (positions, orders, tickers) uses the right format.
        data.asset = re.sub(r"[\s/]+", "", m.group(1)).upper()
 
    m = re.search(r"position" + SEP + r"(\w+)", text, re.IGNORECASE)
    if m and m.group(1).upper() in ("LONG", "SHORT"):
        data.position = m.group(1).upper()
    elif re.search(r"\bshort\b", text, re.IGNORECASE):
        data.position = "SHORT"
    elif re.search(r"\blong\b", text, re.IGNORECASE):
        data.position = "LONG"
 
    m = re.search(r"entr(?:y|ies)" + SEP + r"([^\n\r]+)", text, re.IGNORECASE)
    if m:
        val = m.group(1).strip()
        if re.search(r"market|now|current", val, re.IGNORECASE):
            data.entry_is_market = True
        else:
            nums = _all_nums(val)
            if nums:
                data.entry = float(nums[0])
                if len(nums) > 1:
                    data.entry_range = [float(n) for n in nums]
 
    m = re.search(r"dca" + SEP + r"([^\n\r]+)", text, re.IGNORECASE)
    if m:
        n = _first_num(m.group(1))
        if n:
            data.dca = float(n)
 
    m = re.search(r"leverage" + SEP + r"([^\n\r]+)", text, re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        if re.search(r"cross", raw, re.IGNORECASE):
            data.leverage_mode = "Cross"
        elif re.search(r"isolated", raw, re.IGNORECASE):
            data.leverage_mode = "Isolated"
        nums = _all_nums(raw)
        if nums:
            data.leverage = int(float(nums[0]))  # always take the FIRST number in a range
    else:
        m = re.search(r"(\d+(?:\.\d+)?)\s*[x×]\b", text, re.IGNORECASE)
        if m:
            data.leverage = int(float(m.group(1)))
    # default (10) already set on dataclass if nothing found
 
    m = re.search(r"(?:stop\s*loss|sl)" + SEP + r"\$?(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    if m:
        data.sl = float(m.group(1))
 
    m = re.search(r"(?:margin|risk)(?:\s*(?:percentage|%)?)?" + SEP + r"(\d+(?:\.\d+)?)\s*%?", text, re.IGNORECASE)
    if m:
        data.margin_percent = float(m.group(1))
 
    data.tps = extract_tps(text)
 
    # --- Validation ---
    if not data.asset:
        data.errors.append("No asset detected (expected something like BTC/USDT).")
    if not data.position:
        data.errors.append("No position (LONG/SHORT) detected.")
    if not data.entry and not data.entry_is_market:
        data.errors.append("No entry price detected.")
    if data.sl is None:
        data.errors.append("No stop loss detected — trades without an SL are rejected, no exceptions.")
    if not data.tps:
        data.errors.append("No take-profit targets detected.")
 
    return data