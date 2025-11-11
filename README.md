# trading_metadata

This mono repo will consists of apps which will be used to pull stock chart using yahoo finance, paper trading setup, dhan API integration for live order placement. Profit and loss screen to show how much profit and loss has been. XIRR of overall return.

## Portfolio ingestion service

The `portfolio-ingest` Python package pulls investor holding information along with bulk and block deals from Screener and Trendlyne and stores the results in PostgreSQL. A FastAPI web experience is bundled to review the latest data and to manage the daily ingestion schedule that is executed by an in-app cron scheduler.

Each daily run performs the following steps:

1. Scrape the configured investor pages for the latest holdings and deals.
2. Upsert investors, stocks, current holdings, and buy-side bulk/block deals.
3. Remove holdings (or deals) that are no longer reported, ensuring the database reflects investors exiting a position.

### Project layout

```
pyproject.toml
src/
  portfolio_ingest/
    app.py
    config.py
    db.py
    models.py
    runner.py
    sources/
      __init__.py
      base.py
      screener.py
      trendlyne.py
      utils.py
    templates/
      base.html
      dashboard.html
      schedule.html
```

### Database schema

The ingestion job creates and maintains the following tables:

- `investors`: investor metadata and source URLs.
- `stocks`: unique stock tickers referenced by investors.
- `holdings`: current investor holdings. Records are removed when an investor exits a stock.
- `bulk_deals` / `block_deals`: buy-side bulk and block deals. Records are removed if the deal is no longer published on the source page.
- `ingest_schedule`: stores the hour/minute/timezone for the in-app cron job.

### Configuration

Set the following environment variables before running the job:

- `PORTFOLIO_INGEST_DATABASE_URL`: SQLAlchemy connection URL for PostgreSQL (e.g. `postgresql+psycopg://user:password@localhost:5432/portfolio`).
- `PORTFOLIO_INGEST_INVESTORS` (optional): newline separated list of `Name|URL` pairs overriding the default investor list bundled with the project.

Alternatively, you can rely on the bundled environment files to populate these values. The loader inspects the
`PORTFOLIO_INGEST_ENV` variable (defaulting to `local`) and reads matching `.env.<environment>` files when present:

- `.env.local` &mdash; pre-populated with a localhost PostgreSQL configuration (`localhost:5432`, `vishal/vishal`).
- `.env.prod` &mdash; template to record production credentials.

Each file exposes discrete database settings (`PORTFOLIO_INGEST_DB_*`) and the loader assembles a SQLAlchemy URL automatically.

### Running the web app & scheduler

Install dependencies and start the FastAPI application with Uvicorn:

```bash
pip install -e .
export PORTFOLIO_INGEST_DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/portfolio
uvicorn portfolio_ingest.app:app --reload
```

The server exposes two pages:

- `/` &mdash; dashboard summarising holdings, bulk deals, and block deals.
- `/schedule` &mdash; form to adjust the time (HH:MM, UTC) that the ingestion cron job executes each day.

The initial schedule defaults to `02:00 UTC`. Updating the time via the schedule page immediately persists the configuration to PostgreSQL and reschedules the background job without restarting the server. You can also trigger an ad-hoc run from the command line if needed:

```bash
PORTFOLIO_INGEST_DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/portfolio \
    portfolio-ingest --verbose
```
