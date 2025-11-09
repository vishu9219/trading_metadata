FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PORTFOLIO_INGEST_DATABASE_URL="postgresql+psycopg://postgres:postgres@db:5432/postgres"

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "portfolio_ingest.app:app", "--host", "0.0.0.0", "--port", "8000"]
