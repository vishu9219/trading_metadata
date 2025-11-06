"""Domain models representing investor data."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(slots=True)
class Holding:
    """Represents a single stock holding."""

    investor: str
    ticker: str
    source_url: str
    percent_holding: Optional[float] = None
    shares: Optional[int] = None
    reported_date: Optional[date] = None


@dataclass(slots=True)
class Deal:
    """Represents a bulk or block deal."""

    investor: str
    ticker: str
    source_url: str
    deal_date: date
    quantity: Optional[int]
    price: Optional[float]
    deal_type: str  # "bulk" or "block"
    side: str  # "buy" or "sell"


__all__ = ["Holding", "Deal"]
