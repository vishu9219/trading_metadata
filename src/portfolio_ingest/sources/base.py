"""Base classes for scraping investor data."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from ..models import Deal, Holding


class InvestorSource(ABC):
    """Abstract source that can load holdings and deals."""

    def __init__(self, investor: str, url: str) -> None:
        self.investor = investor
        self.url = url

    @abstractmethod
    def fetch_holdings(self) -> Iterable[Holding]:
        """Yield holdings for the investor."""

    @abstractmethod
    def fetch_deals(self) -> Iterable[Deal]:
        """Yield deals (bulk or block) for the investor."""


__all__ = ["InvestorSource"]
