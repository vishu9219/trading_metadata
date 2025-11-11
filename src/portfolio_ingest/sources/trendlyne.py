"""Trendlyne data source implementation."""
from __future__ import annotations

import logging
from typing import Iterable

import requests
from bs4 import BeautifulSoup

from ..models import Deal, Holding
from .base import InvestorSource
from .utils import parse_date, parse_float, parse_int

LOGGER = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9,hi;q=0.8",
    "cache-control": "max-age=0",
    "priority": "u=0, i",
    "referer": "https://trendlyne.com/equity/insider-trading-sast/custom/?query=Massachusetts%20Institute%20of%20Technology%20&%20PACs",
    "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
}

DEFAULT_COOKIES = {
    "_ga": "GA1.1.573022403.1733077069",
    "_clck": "19s8hfw|2|frc|0|1796",
    "_ga_8MLP1KVCSX": "GS1.1.1733077528.1.1.1733077621.0.0.0",
    "_ga_J2YW7VJGYP": "GS1.1.1733250768.3.0.1733250768.0.0.0",
    "TL_USER_COUNTRY": "IND",
    "_gcl_au": "1.1.788577265.1755862336",
    "csrftoken": "3j7IVBDJ7QeJXgPRrmbCSo1mWgw9elb3jxlB86NyrlNbwxa2nuMsPw9hoRefnCE9",
    "g_state": '{"i_l":0,"i_ll":1762888622928,"i_b":"E0GqlfJd6pc+RyFEWJlulPTFnjLfEhGqK5cQ50TuXkU"}',
    "_ga_7F29Q8ZGH0": "GS2.1.s1762888100$o35$g1$t1762888622$j60$l0$h0",
}


class TrendlyneSource(InvestorSource):
    """Scraper for Trendlyne superstar pages."""

    def __init__(self, investor: str, url: str, session: requests.Session | None = None) -> None:
        super().__init__(investor, url)
        self.session = session or requests.Session()
        # Update the session with headers/cookies required to avoid bot detection.
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.cookies.update(DEFAULT_COOKIES)

    def _get_soup(self) -> BeautifulSoup:
        LOGGER.debug("Requesting Trendlyne page for %s", self.investor)
        response = self.session.get(self.url, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def fetch_holdings(self) -> Iterable[Holding]:
        soup = self._get_soup()
        LOGGER.debug("Parsing holdings table for %s", self.investor)
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
        LOGGER.debug("Parsing deals sections for %s", self.investor)
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
