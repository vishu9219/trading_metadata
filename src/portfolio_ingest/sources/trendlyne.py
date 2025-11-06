"""Trendlyne data source implementation."""
from __future__ import annotations

from typing import Iterable

import requests
from bs4 import BeautifulSoup

from ..models import Deal, Holding
from .base import InvestorSource
from .utils import parse_date, parse_float, parse_int


class TrendlyneSource(InvestorSource):
    """Scraper for Trendlyne superstar pages."""

    def __init__(self, investor: str, url: str, session: requests.Session | None = None) -> None:
        super().__init__(investor, url)
        self.session = session or requests.Session()

    def _get_soup(self) -> BeautifulSoup:
        response = self.session.get(self.url, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def fetch_holdings(self) -> Iterable[Holding]:
        soup = self._get_soup()
        tables = soup.select("table")
        for table in tables:
            header = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not header:
                continue
            if "stock" in header[0] or "company" in header[0]:
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if not cells:
                        continue
                    anchor = cells[0].find("a", href=True)
                    ticker = (anchor.get_text(strip=True) if anchor else cells[0].get_text(strip=True)).upper()
                    percent = parse_float(cells[1].get_text(strip=True) if len(cells) > 1 else None)
                    shares = parse_int(cells[2].get_text(strip=True) if len(cells) > 2 else None)
                    reported = parse_date(cells[3].get_text(strip=True) if len(cells) > 3 else None)
                    yield Holding(
                        investor=self.investor,
                        ticker=ticker,
                        source_url=self.url,
                        percent_holding=percent,
                        shares=shares,
                        reported_date=reported,
                    )
                break

    def fetch_deals(self) -> Iterable[Deal]:
        soup = self._get_soup()
        for section in soup.find_all("section"):
            title = section.find(["h2", "h3"])
            if not title:
                continue
            heading = title.get_text(strip=True).lower()
            if "bulk" not in heading and "block" not in heading:
                continue
            table = section.find("table")
            if not table:
                continue
            deal_type = "bulk" if "bulk" in heading else "block"
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if not cells:
                    continue
                ticker = cells[0].get_text(strip=True).upper()
                deal_date = parse_date(cells[1].get_text(strip=True) if len(cells) > 1 else None)
                side_text = cells[2].get_text(strip=True).lower() if len(cells) > 2 else ""
                if side_text not in {"buy", "sell"} or deal_date is None:
                    continue
                quantity = parse_int(cells[3].get_text(strip=True) if len(cells) > 3 else None)
                price = parse_float(cells[4].get_text(strip=True) if len(cells) > 4 else None)
                yield Deal(
                    investor=self.investor,
                    ticker=ticker,
                    source_url=self.url,
                    deal_date=deal_date,
                    quantity=quantity,
                    price=price,
                    deal_type=deal_type,
                    side=side_text,
                )


__all__ = ["TrendlyneSource"]
