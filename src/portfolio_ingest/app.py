"""FastAPI application exposing holdings dashboards and schedule controls."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Tuple
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .config import Settings
from .db import (
    block_deals,
    bulk_deals,
    create_db_engine,
    ensure_schema,
    fetch_deals_view,
    fetch_holdings_view,
    get_or_create_schedule,
    update_schedule,
)
from .runner import run_ingestion

LOGGER = logging.getLogger(__name__)

settings = Settings.load()
engine = create_db_engine(settings.database_url)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

scheduler = AsyncIOScheduler()
JOB_ID = "daily-ingestion"

def _ingestion_job() -> None:
    """Wrapper for running the ingestion pipeline within the scheduler."""

    try:
        run_ingestion(settings)
    except Exception:  # pragma: no cover - defensive logging
        LOGGER.exception("Scheduled ingestion run failed")


def _configure_job(schedule: dict[str, Any]) -> None:
    """Ensure the APScheduler job reflects the configured schedule."""

    trigger = CronTrigger(
        hour=schedule["hour"],
        minute=schedule["minute"],
        timezone=ZoneInfo(schedule["timezone"]),
    )
    if scheduler.get_job(JOB_ID):
        scheduler.reschedule_job(JOB_ID, trigger=trigger)
    else:
        scheduler.add_job(_ingestion_job, trigger=trigger, id=JOB_ID, replace_existing=True)


def _format_schedule(schedule: dict[str, Any]) -> str:
    return f"{int(schedule['hour']):02d}:{int(schedule['minute']):02d}"


def _parse_time(value: str) -> Tuple[int, int]:
    value = value.strip()
    if not value or ":" not in value:
        raise ValueError("Time must be in HH:MM format")
    hour_str, minute_str = value.split(":", 1)
    hour = int(hour_str)
    minute = int(minute_str)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Hours must be 0-23 and minutes 0-59")
    return hour, minute


app = FastAPI(title="Investor Holdings", default_response_class=HTMLResponse)


@app.on_event("startup")
async def startup_event() -> None:
    ensure_schema(engine)
    schedule = get_or_create_schedule(engine)
    _configure_job(schedule)
    if not scheduler.running:
        scheduler.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if scheduler.running:
        scheduler.shutdown()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    holdings = fetch_holdings_view(engine)
    bulk = fetch_deals_view(engine, bulk_deals)
    block = fetch_deals_view(engine, block_deals)
    schedule = get_or_create_schedule(engine)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "holdings": holdings,
            "bulk_deals": bulk,
            "block_deals": block,
            "schedule_time": _format_schedule(schedule),
            "schedule_timezone": schedule["timezone"],
        },
    )


@app.get("/schedule", response_class=HTMLResponse)
async def show_schedule(request: Request) -> HTMLResponse:
    schedule = get_or_create_schedule(engine)
    updated = request.query_params.get("updated")
    return templates.TemplateResponse(
        "schedule.html",
        {
            "request": request,
            "schedule_time": _format_schedule(schedule),
            "schedule_timezone": schedule["timezone"],
            "updated": bool(updated),
            "error": None,
        },
    )


@app.post("/schedule", response_class=HTMLResponse)
async def update_schedule_view(request: Request, time: str = Form(...)) -> HTMLResponse:
    try:
        hour, minute = _parse_time(time)
    except ValueError as exc:
        schedule = get_or_create_schedule(engine)
        return templates.TemplateResponse(
            "schedule.html",
            {
                "request": request,
                "schedule_time": _format_schedule(schedule),
                "schedule_timezone": schedule["timezone"],
                "updated": False,
                "error": str(exc),
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    schedule = update_schedule(engine, hour, minute)
    _configure_job(schedule)
    return RedirectResponse(url="/schedule?updated=1", status_code=status.HTTP_303_SEE_OTHER)


__all__ = ["app"]
