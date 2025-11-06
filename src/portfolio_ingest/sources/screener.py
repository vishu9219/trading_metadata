"""Screener.in data source implementation."""
from __future__ import annotations

from typing import Iterable

import requests
from bs4 import BeautifulSoup

from ..models import Deal, Holding
from .base import InvestorSource
from .utils import parse_date, parse_float, parse_int

class ScreenerSource(InvestorSource):
    """Scraper for screener.in investor pages."""

    BASE_URL = "https://www.screener.in"

    def __init__(self, investor: str, url: str, session: requests.Session | None = None) -> None:
        super().__init__(investor, url)
        self.session = session or requests.Session()

    def _get_soup(self) -> BeautifulSoup:
        response = self.session.get(self.url, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def fetch_holdings(self) -> Iterable[Holding]:
        soup = self._get_soup()
        tables = soup.find_all("table")
        for table in tables:
            header = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "company" in header and ("holding" in " ".join(header) or "shares" in header):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if not cells:
                        continue
                    company_cell = cells[0]
                    anchor = company_cell.find("a", href=True)
                    if not anchor or "/company/" not in anchor["href"]:
                        continue
                    ticker = anchor["href"].strip("/").split("/")[-1]
                    percent = parse_float(cells[1].get_text(strip=True) if len(cells) > 1 else None)
                    shares = parse_int(cells[2].get_text(strip=True) if len(cells) > 2 else None)
                    reported = None
                    if len(cells) > 3:
                        reported = parse_date(cells[3].get_text(strip=True))
                    yield Holding(
                        investor=self.investor,
                        ticker=ticker.upper(),
                        source_url=self.url,
                        percent_holding=percent,
                        shares=shares,
                        reported_date=reported,
                    )
                break

    def fetch_deals(self) -> Iterable[Deal]:
        soup = self._get_soup()
        for title in soup.find_all("h2"):
            heading = title.get_text(strip=True).lower()
            if "bulk deals" in heading or "block deals" in heading:
                table = title.find_next("table")
                if not table:
                    continue
                deal_type = "bulk" if "bulk" in heading else "block"
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if not cells:
                        continue
                    anchor = cells[0].find("a", href=True)
                    if not anchor:
                        continue
                    ticker = anchor.get_text(strip=True).upper()
                    deal_date = parse_date(cells[1].get_text(strip=True) if len(cells) > 1 else None)
                    side_text = cells[2].get_text(strip=True).lower() if len(cells) > 2 else ""
                    if side_text not in {"buy", "sell"}:
                        continue
                    if deal_date is None:
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


__all__ = ["ScreenerSource"]
