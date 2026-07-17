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
    margin_percent: Optional[float] = None
    tps: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
