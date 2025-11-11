"""Application configuration helpers."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping
from urllib.parse import quote_plus


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


def _resolve_env_file(candidate: str) -> Path | None:
    """Return the first matching environment file path if it exists."""

    path = Path(candidate)
    if path.is_absolute() and path.exists():
        return path

    search_roots = [Path.cwd(), Path(__file__).resolve().parent]
    search_roots.extend(Path(__file__).resolve().parents)

    seen: set[Path] = set()
    for root in search_roots:
        root = root.resolve()
        if root in seen:
            continue
        seen.add(root)
        potential = root / candidate
        if potential.exists():
            return potential
    return None


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse a dotenv-style file into a mapping."""

    variables: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        variables[key] = value
    return variables


def _load_profile_env(env: Mapping[str, str]) -> dict[str, str]:
    """Load environment variables from the selected profile file."""

    explicit_file = env.get("PORTFOLIO_INGEST_ENV_FILE")
    profile = env.get("PORTFOLIO_INGEST_ENV", "local")
    candidates = [explicit_file] if explicit_file else [f".env.{profile}"]

    for candidate in candidates:
        if not candidate:
            continue
        path = _resolve_env_file(candidate)
        if path is not None:
            return _parse_env_file(path)
    return {}


def _build_database_url(env: Mapping[str, str]) -> str | None:
    """Construct a SQLAlchemy URL from discrete environment variables."""

    host = env.get("PORTFOLIO_INGEST_DB_HOST")
    if not host:
        return None

    username = env.get("PORTFOLIO_INGEST_DB_USERNAME")
    if not username:
        raise RuntimeError(
            "PORTFOLIO_INGEST_DB_USERNAME must be set when using discrete database settings"
        )

    if "PORTFOLIO_INGEST_DB_PASSWORD" not in env:
        raise RuntimeError(
            "PORTFOLIO_INGEST_DB_PASSWORD must be set when using discrete database settings"
        )

    password = env.get("PORTFOLIO_INGEST_DB_PASSWORD", "")
    port = env.get("PORTFOLIO_INGEST_DB_PORT", "5432")
    database = env.get("PORTFOLIO_INGEST_DB_NAME", "portfolio")
    driver = env.get("PORTFOLIO_INGEST_DB_DRIVER", "postgresql+psycopg")

    auth = f"{quote_plus(username)}:{quote_plus(password)}"
    port_part = f":{port}" if port else ""
    return f"{driver}://{auth}@{host}{port_part}/{database}"


@dataclass(frozen=True)
class Settings:
    """Runtime configuration."""

    database_url: str
    investor_sources: Mapping[str, str]

    @staticmethod
    def load(env: Mapping[str, str] | None = None) -> "Settings":
        """Load settings from environment variables."""

        base_env = dict(env or os.environ)
        file_env = _load_profile_env(base_env)
        # Environment variables set in the shell take precedence over the file.
        merged_env = {**file_env, **base_env}

        database_url = merged_env.get("PORTFOLIO_INGEST_DATABASE_URL")
        if not database_url:
            database_url = _build_database_url(merged_env)
        if not database_url:
            raise RuntimeError(
                "PORTFOLIO_INGEST_DATABASE_URL must be set or provide discrete database settings via the env file"
            )

        investors_env = merged_env.get("PORTFOLIO_INGEST_INVESTORS")
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
