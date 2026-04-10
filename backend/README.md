# Contango Backend

FastAPI backend for the CommodityWatch InventoryWatch MVP.

## Stack

- FastAPI
- PostgreSQL 16
- SQLAlchemy 2
- Alembic
- APScheduler

## Setup

1. Create a PostgreSQL database.
2. Copy `.env.example` to `.env` and fill in the required values.
3. Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

4. Run migrations:

```bash
alembic upgrade head
```

5. Seed reference data:

```bash
python scripts/seed_reference_data.py
```

## Run

API:

```bash
uvicorn app.main:app --reload
```

Worker:

```bash
python -m app.worker
```

Manual job execution:

```bash
python -m app.worker run-once --job eia_wpsr
python -m app.worker run-once --job eia_wngs
python -m app.worker run-once --job agsi_daily
```

## Backfill

Run a historical backfill from a chosen start date:

```bash
python scripts/run_backfill.py --source eia_wpsr --from-date 2018-01-01
python scripts/run_backfill.py --source eia_wngs --from-date 2018-01-01
python scripts/run_backfill.py --source agsi --from-date 2018-01-01
```

After a backfill, recompute seasonal ranges:

```bash
python -m app.processing.seasonal --indicator-scope inventorywatch
```

## Environment Variables

Core:

- `CW_ENV`
- `CW_BASE_URL`
- `CW_LOG_LEVEL`
- `CW_SECRET_KEY`

Database:

- `CW_DATABASE_URL`

Auth:

- `CW_SESSION_COOKIE_NAME`
- `CW_SESSION_COOKIE_DOMAIN`
- `CW_SESSION_MAX_AGE_SECONDS`

Source keys:

- `CW_EIA_API_KEY`
- `CW_AGSI_API_KEY`
- `CW_USDA_NASS_API_KEY`
- `CW_FRED_API_KEY`
- `CW_EMBER_API_KEY`
- `CW_CDS_API_KEY`
- `CW_CDS_UID`

Artifacts / monitoring:

- `CW_ARTIFACT_ROOT`
- `CW_ALERT_WEBHOOK_URL`

Optional billing config:

- `CW_STRIPE_SECRET_KEY`
- `CW_STRIPE_WEBHOOK_SECRET`
- `CW_STRIPE_PRICE_PRO_MONTHLY`
- `CW_STRIPE_PORTAL_CONFIGURATION_ID`
