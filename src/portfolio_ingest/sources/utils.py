"""Utility helpers for scraping."""
from __future__ import annotations

import re
from datetime import date
from typing import Optional

from dateutil import parser


NON_DIGIT = re.compile(r"[^0-9.]")


def parse_float(value: str | None) -> Optional[float]:
    """Parse a human readable percentage/float value."""

    if not value:
        return None
    cleaned = value.strip().replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        cleaned = NON_DIGIT.sub("", cleaned)
        return float(cleaned) if cleaned else None


def parse_int(value: str | None) -> Optional[int]:
    """Parse a human readable integer value."""

    if not value:
        return None
    cleaned = re.sub(r"[^0-9]", "", value)
    return int(cleaned) if cleaned else None


def parse_date(value: str | None) -> Optional[date]:
    """Parse a date string using dateutil."""

    if not value:
        return None
    return parser.parse(value, dayfirst=True).date()


__all__ = ["parse_float", "parse_int", "parse_date"]
