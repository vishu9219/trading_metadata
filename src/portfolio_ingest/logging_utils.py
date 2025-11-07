"""Logging configuration helpers for the portfolio ingestion project."""
from __future__ import annotations

import logging
import os


def _coerce_level(level: str | int) -> int:
    """Translate a user provided level into a numeric log level."""

    if isinstance(level, int):
        return level
    name = level.upper()
    if name not in logging._nameToLevel:  # type: ignore[attr-defined]
        raise ValueError(f"Unknown log level: {level}")
    return logging._nameToLevel[name]  # type: ignore[attr-defined]


def configure_logging(level: str | int | None = None, *, force: bool = False) -> None:
    """Configure the root logger for console output.

    The log level defaults to the ``PORTFOLIO_INGEST_LOG_LEVEL`` environment variable
    when ``level`` is not provided. A sensible INFO level is used when no override is
    supplied. ``force`` mirrors :func:`logging.basicConfig`'s ``force`` parameter and
    allows callers to reconfigure logging when required.
    """

    resolved_level: int
    if level is None:
        env_level = os.getenv("PORTFOLIO_INGEST_LOG_LEVEL", "INFO")
        try:
            resolved_level = _coerce_level(env_level)
        except ValueError:
            resolved_level = logging.INFO
    else:
        try:
            resolved_level = _coerce_level(level)
        except ValueError:
            resolved_level = logging.INFO

    root_logger = logging.getLogger()
    if root_logger.handlers and not force:
        root_logger.setLevel(resolved_level)
        return

    logging.basicConfig(
        level=resolved_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=force,
    )


__all__ = ["configure_logging"]
