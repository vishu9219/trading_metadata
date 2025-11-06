"""Command line entry point for the portfolio ingestion job."""
from __future__ import annotations

import argparse
import logging
from typing import Iterable, List

from .config import Settings
from .db import (
    create_db_engine,
    ensure_schema,
    sync_block_deals,
    sync_bulk_deals,
    sync_holdings,
)
from .models import Deal, Holding
from .sources import create_source

LOGGER = logging.getLogger(__name__)


def gather_data(settings: Settings) -> tuple[List[Holding], List[Deal]]:
    """Collect holdings and deal data from all configured sources."""

    holdings: List[Holding] = []
    deals: List[Deal] = []
    for investor, url in settings.investor_sources.items():
        source = create_source(investor, url)
        try:
            holdings.extend(source.fetch_holdings())
            deals.extend(source.fetch_deals())
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to gather data for %s (%s): %s", investor, url, exc)
    return holdings, deals


def run_ingestion(settings: Settings) -> None:
    """Run the ingestion process."""

    engine = create_db_engine(settings.database_url)
    ensure_schema(engine)

    holdings, deals = gather_data(settings)
    sync_holdings(engine, holdings)
    sync_bulk_deals(engine, deals)
    sync_block_deals(engine, deals)


def parse_args(args: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging output",
    )
    return parser.parse_args(args=args)


def main(argv: Iterable[str] | None = None) -> None:
    options = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if options.verbose else logging.INFO)
    settings = Settings.load()
    run_ingestion(settings)


if __name__ == "__main__":  # pragma: no cover
    main()
