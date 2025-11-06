"""Source factory for investor scrapers."""
from __future__ import annotations

from typing import Iterable

from .base import InvestorSource
from .screener import ScreenerSource
from .trendlyne import TrendlyneSource


def create_source(investor: str, url: str) -> InvestorSource:
    """Instantiate the correct source implementation based on the URL."""

    if "screener.in" in url:
        return ScreenerSource(investor, url)
    if "trendlyne.com" in url:
        return TrendlyneSource(investor, url)
    raise ValueError(f"Unsupported source URL: {url}")


__all__ = ["create_source", "InvestorSource", "ScreenerSource", "TrendlyneSource"]
