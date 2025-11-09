"""Source factory for investor scrapers."""
from __future__ import annotations

import logging

from .base import InvestorSource
from .screener import ScreenerSource
from .trendlyne import TrendlyneSource

LOGGER = logging.getLogger(__name__)


def create_source(investor: str, url: str) -> InvestorSource:
    """Instantiate the correct source implementation based on the URL."""

    if "screener.in" in url:
        LOGGER.debug("Selected ScreenerSource for %s", investor)
        return ScreenerSource(investor, url)
    if "trendlyne.com" in url:
        LOGGER.debug("Selected TrendlyneSource for %s", investor)
        return TrendlyneSource(investor, url)
    raise ValueError(f"Unsupported source URL: {url}")


__all__ = ["create_source", "InvestorSource", "ScreenerSource", "TrendlyneSource"]
