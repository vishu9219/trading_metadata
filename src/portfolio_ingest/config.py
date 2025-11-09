"""Application configuration helpers."""
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping


DEFAULT_INVESTOR_SOURCES: Mapping[str, str] = {
    "Parag Parikh Flexi Cap Fund": "https://www.screener.in/people/97814/parag-parikh-flexi-cap-fund/",
    "Mirae Asset Emerging Bluechip Fund": "https://www.screener.in/people/1604/mirae-asset-emerging-bluechip-fund/",
    "Mirae Asset Large & Midcap Fund": "https://www.screener.in/people/144848/mirae-asset-large-midcap-fund/",
    "3Pindia Equity Fund 1": "https://www.screener.in/people/130713/3pindia-equity-fund-1/",
    "Quant Mutual Fund - Quant Small Cap Fund": "https://www.screener.in/people/145014/quant-mutual-fund-quant-small-cap-fund/",
    "Ashish Kacholia": "https://www.screener.in/people/127736/ashish-kacholia/",
    "Mukul Mahavir Agrawal": "https://www.screener.in/people/127675/mukul-mahavir-agrawal/",
    "Akash Bhanushali": "https://www.screener.in/people/4101/akash-bhanushali/",
    "Sunil Singhania": "https://trendlyne.com/portfolio/superstar-shareholders/182955/latest/sunil-singhania-portfolio/",
    "Vijay Kedia": "https://www.screener.in/people/7377/vijay-krishanlal-kedia/",
    "Madhuri Kela": "https://www.screener.in/people/30960/madhuri-madhusudan-kela/",
    "Massachusetts Institute of Technology": "https://trendlyne.com/portfolio/superstar-shareholders/1537932/latest/massachusetts-institute-of-technology/",
    "Goldman Sachs India Equity Portfolio": "https://www.screener.in/people/19335/goldman-sachs-funds-goldman-sachs-india-equity-p/",
    "Small Cap World Fund Inc": "https://www.screener.in/people/436/small-cap-world-fund-inc/",
    "Nalanda India Equity Fund": "https://www.screener.in/people/73618/nalanda-india-equity-fund-limited/",
    "Jupiter India Fund": "https://www.screener.in/people/1555/jupiter-india-fund/",
}


@dataclass(frozen=True)
class Settings:
    """Runtime configuration."""

    database_url: str
    investor_sources: Mapping[str, str]

    @staticmethod
    def load(env: Mapping[str, str] | None = None) -> "Settings":
        """Load settings from environment variables."""

        env = env or os.environ
        database_url = env.get("PORTFOLIO_INGEST_DATABASE_URL")
        if not database_url:
            raise RuntimeError(
                "PORTFOLIO_INGEST_DATABASE_URL environment variable must be set"
            )

        investors_env = env.get("PORTFOLIO_INGEST_INVESTORS")
        if investors_env:
            investor_sources = {}
            for chunk in investors_env.split("\n"):
                if not chunk.strip():
                    continue
                try:
                    name, url = chunk.split("|", 1)
                except ValueError as exc:  # pragma: no cover - defensive
                    raise RuntimeError(
                        "Each investor definition must be of the form 'Name|URL'"
                    ) from exc
                investor_sources[name.strip()] = url.strip()
        else:
            investor_sources = dict(DEFAULT_INVESTOR_SOURCES)

        return Settings(database_url=database_url, investor_sources=investor_sources)


__all__ = ["Settings", "DEFAULT_INVESTOR_SOURCES"]
