"""Database integration utilities."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime
from typing import Iterable, Iterator

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    delete,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from .models import Deal, Holding


metadata = MetaData()

LOGGER = logging.getLogger(__name__)

investors = Table(
    "investors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(255), nullable=False, unique=True),
    Column("source_url", String(1024), nullable=False),
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
    Column(
        "updated_at", DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    ),
)

stocks = Table(
    "stocks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(64), nullable=False, unique=True),
)

holdings = Table(
    "holdings",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("investor_id", ForeignKey("investors.id", ondelete="CASCADE"), nullable=False),
    Column("stock_id", ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False),
    Column("percent_holding", Float, nullable=True),
    Column("shares", Integer, nullable=True),
    Column("reported_date", Date, nullable=True),
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
    Column(
        "updated_at", DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    ),
    UniqueConstraint("investor_id", "stock_id", name="uq_holdings_investor_stock"),
)

bulk_deals = Table(
    "bulk_deals",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("investor_id", ForeignKey("investors.id", ondelete="CASCADE"), nullable=False),
    Column("stock_id", ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False),
    Column("deal_date", Date, nullable=False),
    Column("quantity", Integer, nullable=True),
    Column("price", Float, nullable=True),
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
    Column(
        "updated_at", DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    ),
    UniqueConstraint("investor_id", "stock_id", "deal_date", name="uq_bulk_deal"),
)

block_deals = Table(
    "block_deals",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("investor_id", ForeignKey("investors.id", ondelete="CASCADE"), nullable=False),
    Column("stock_id", ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False),
    Column("deal_date", Date, nullable=False),
    Column("quantity", Integer, nullable=True),
    Column("price", Float, nullable=True),
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
    Column(
        "updated_at", DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    ),
    UniqueConstraint("investor_id", "stock_id", "deal_date", name="uq_block_deal"),
)


ingest_schedule = Table(
    "ingest_schedule",
    metadata,
    Column("id", Integer, primary_key=True, default=1),
    Column("hour", Integer, nullable=False),
    Column("minute", Integer, nullable=False),
    Column("timezone", String(64), nullable=False, default="UTC"),
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
    Column(
        "updated_at", DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    ),
)


DEFAULT_SCHEDULE = {"hour": 2, "minute": 0, "timezone": "UTC"}


UNIQUE_CONSTRAINTS: dict[str, str] = {
    "bulk_deals": "uq_bulk_deal",
    "block_deals": "uq_block_deal",
}


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine."""

    LOGGER.debug("Creating database engine")
    return create_engine(database_url, future=True, pool_pre_ping=True)


@contextmanager
def session(engine: Engine) -> Iterator[Engine]:
    """Provide a transactional scope around a series of operations."""

    with engine.begin() as conn:
        yield conn


def ensure_schema(engine: Engine) -> None:
    """Create tables if they do not exist."""

    LOGGER.debug("Ensuring database schema is present")
    metadata.create_all(engine)


def _upsert_investor(conn, name: str, source_url: str) -> int:
    stmt = pg_insert(investors).values(name=name, source_url=source_url)
    stmt = stmt.on_conflict_do_update(
        index_elements=[investors.c.name],
        set_={"source_url": stmt.excluded.source_url, "updated_at": datetime.utcnow()},
    ).returning(investors.c.id)
    return conn.execute(stmt).scalar_one()


def _upsert_stock(conn, ticker: str) -> int:
    stmt = pg_insert(stocks).values(ticker=ticker)
    stmt = stmt.on_conflict_do_nothing().returning(stocks.c.id)
    result = conn.execute(stmt).scalar_one_or_none()
    if result is not None:
        return result
    stmt = select(stocks.c.id).where(stocks.c.ticker == ticker)
    return conn.execute(stmt).scalar_one()


def sync_holdings(engine: Engine, holdings_data: Iterable[Holding]) -> None:
    """Synchronize holdings table with the scraped data."""

    grouped: dict[tuple[int, int], Holding] = {}
    with session(engine) as conn:
        for holding in holdings_data:
            investor_id = _upsert_investor(conn, holding.investor, holding.source_url)
            stock_id = _upsert_stock(conn, holding.ticker)
            key = (investor_id, stock_id)
            grouped[key] = holding

        for (investor_id, stock_id), holding in grouped.items():
            stmt = pg_insert(holdings).values(
                investor_id=investor_id,
                stock_id=stock_id,
                percent_holding=holding.percent_holding,
                shares=holding.shares,
                reported_date=holding.reported_date,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_holdings_investor_stock",
                set_={
                    "percent_holding": stmt.excluded.percent_holding,
                    "shares": stmt.excluded.shares,
                    "reported_date": stmt.excluded.reported_date,
                    "updated_at": datetime.utcnow(),
                },
            )
            conn.execute(stmt)

        existing_stmt = select(holdings.c.id, holdings.c.investor_id, holdings.c.stock_id)
        existing = conn.execute(existing_stmt).all()
        to_keep = set(grouped.keys())
        to_remove = [row.id for row in existing if (row.investor_id, row.stock_id) not in to_keep]
        if to_remove:
            conn.execute(delete(holdings).where(holdings.c.id.in_(to_remove)))
    LOGGER.info(
        "Synchronized %d holdings rows (%d removed)",
        len(grouped),
        len(to_remove),
    )


def _sync_deals(engine: Engine, deals_data: Iterable[Deal], table: Table, constraint: str) -> None:
    grouped: dict[tuple[int, int, date], Deal] = {}
    with session(engine) as conn:
        for deal in deals_data:
            investor_id = _upsert_investor(conn, deal.investor, deal.source_url)
            stock_id = _upsert_stock(conn, deal.ticker)
            key = (investor_id, stock_id, deal.deal_date)
            grouped[key] = deal

        for (investor_id, stock_id, deal_date), deal in grouped.items():
            stmt = pg_insert(table).values(
                investor_id=investor_id,
                stock_id=stock_id,
                deal_date=deal_date,
                quantity=deal.quantity,
                price=deal.price,
            )
            stmt = stmt.on_conflict_do_update(
                constraint=constraint,
                set_={
                    "quantity": stmt.excluded.quantity,
                    "price": stmt.excluded.price,
                    "updated_at": datetime.utcnow(),
                },
            )
            conn.execute(stmt)

        existing_stmt = select(table.c.id, table.c.investor_id, table.c.stock_id, table.c.deal_date)
        existing = conn.execute(existing_stmt).all()
        to_keep = set(grouped.keys())
        to_remove = [
            row.id
            for row in existing
            if (row.investor_id, row.stock_id, row.deal_date) not in to_keep
        ]
        if to_remove:
            conn.execute(delete(table).where(table.c.id.in_(to_remove)))
    LOGGER.info(
        "Synchronized %d %s rows (%d removed)",
        len(grouped),
        table.name,
        len(to_remove),
    )


def sync_bulk_deals(engine: Engine, deals_data: Iterable[Deal]) -> None:
    """Synchronize bulk deals, keeping only buy transactions."""

    filtered = [deal for deal in deals_data if deal.deal_type.lower() == "bulk" and deal.side.lower() == "buy"]
    _sync_deals(engine, filtered, bulk_deals, UNIQUE_CONSTRAINTS["bulk_deals"])


def sync_block_deals(engine: Engine, deals_data: Iterable[Deal]) -> None:
    """Synchronize block deals, keeping only buy transactions."""

    filtered = [deal for deal in deals_data if deal.deal_type.lower() == "block" and deal.side.lower() == "buy"]
    _sync_deals(engine, filtered, block_deals, UNIQUE_CONSTRAINTS["block_deals"])


def fetch_holdings_view(engine: Engine) -> list[dict[str, object]]:
    """Return holdings joined with investor and stock metadata for presentation."""

    LOGGER.debug("Loading holdings view data")
    with engine.connect() as conn:
        stmt = (
            select(
                stocks.c.ticker,
                investors.c.name.label("investor"),
                holdings.c.percent_holding,
                holdings.c.shares,
                holdings.c.reported_date,
            )
            .select_from(
                holdings.join(investors, holdings.c.investor_id == investors.c.id).join(
                    stocks, holdings.c.stock_id == stocks.c.id
                )
            )
            .order_by(stocks.c.ticker, investors.c.name)
        )
        rows = conn.execute(stmt).all()
    return [dict(row._mapping) for row in rows]


def fetch_deals_view(engine: Engine, table: Table) -> list[dict[str, object]]:
    """Return deal records joined with investor and stock metadata for presentation."""

    LOGGER.debug("Loading %s view data", table.name)
    with engine.connect() as conn:
        stmt = (
            select(
                stocks.c.ticker,
                investors.c.name.label("investor"),
                table.c.deal_date,
                table.c.quantity,
                table.c.price,
            )
            .select_from(
                table.join(investors, table.c.investor_id == investors.c.id).join(
                    stocks, table.c.stock_id == stocks.c.id
                )
            )
            .order_by(table.c.deal_date.desc(), stocks.c.ticker, investors.c.name)
        )
        rows = conn.execute(stmt).all()
    return [dict(row._mapping) for row in rows]


def get_or_create_schedule(engine: Engine) -> dict[str, int | str]:
    """Fetch the current ingestion schedule, seeding defaults when missing."""

    LOGGER.debug("Fetching ingestion schedule")
    with session(engine) as conn:
        row = conn.execute(select(ingest_schedule)).first()
        if row is not None:
            data = row._mapping
            return {
                "hour": data["hour"],
                "minute": data["minute"],
                "timezone": data["timezone"],
            }

        stmt = pg_insert(ingest_schedule).values(
            id=1,
            hour=DEFAULT_SCHEDULE["hour"],
            minute=DEFAULT_SCHEDULE["minute"],
            timezone=DEFAULT_SCHEDULE["timezone"],
        )
        stmt = stmt.on_conflict_do_nothing()
        conn.execute(stmt)
        LOGGER.info(
            "Seeded default schedule %02d:%02d %s",
            DEFAULT_SCHEDULE["hour"],
            DEFAULT_SCHEDULE["minute"],
            DEFAULT_SCHEDULE["timezone"],
        )
        return dict(DEFAULT_SCHEDULE)


def update_schedule(engine: Engine, hour: int, minute: int, timezone: str = "UTC") -> dict[str, int | str]:
    """Persist a new ingestion schedule."""

    LOGGER.debug(
        "Persisting schedule change to %02d:%02d %s",
        hour,
        minute,
        timezone,
    )
    with session(engine) as conn:
        stmt = pg_insert(ingest_schedule).values(
            id=1,
            hour=hour,
            minute=minute,
            timezone=timezone,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[ingest_schedule.c.id],
            set_={
                "hour": stmt.excluded.hour,
                "minute": stmt.excluded.minute,
                "timezone": stmt.excluded.timezone,
                "updated_at": datetime.utcnow(),
            },
        )
        conn.execute(stmt)

    return {"hour": hour, "minute": minute, "timezone": timezone}


__all__ = [
    "create_db_engine",
    "ensure_schema",
    "sync_holdings",
    "sync_bulk_deals",
    "sync_block_deals",
    "metadata",
    "investors",
    "stocks",
    "holdings",
    "bulk_deals",
    "block_deals",
    "ingest_schedule",
    "DEFAULT_SCHEDULE",
    "fetch_holdings_view",
    "fetch_deals_view",
    "get_or_create_schedule",
    "update_schedule",
]
