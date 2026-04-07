# CommodityWatch

CommodityWatch ships as a five-module product in the existing production shell:

- `HeadlineWatch` for the existing live headline workflow backed by a local `data/feed.local.json` override when present, otherwise the tracked `data/feed.json` sample snapshot
- `PriceWatch` for the commodity visual page backed by published commodity database views, with optional local syncing into `data/commodities.db`
- `CalendarWatch` for publishable commodity-market calendar events backed by the local calendar ingestion pipeline and `/api/calendar`
- `InventoryWatch` for inventory snapshots and indicator detail views backed by a published local SQLite read model in `data/inventorywatch.db` when available, otherwise the local backend artifact archive, with optional proxying to the separate InventoryWatch backend
- `DemandWatch` for demand-side indicator panels backed by curated static data in `demand-watch/data.js` (live API backend in development via `scripts/publish_demandwatch_store.py`)

The root `/` serves a multi-module dashboard homepage. Two additional modules — `SupplyWatch` and `WeatherWatch` — appear as disabled placeholders in the navigation and are not yet implemented.

The separate `frontend/` Next.js app remains in the repo as an implementation reference, not as the primary production runtime.

## Product structure

- `index.html` + `dashboard/`
  - Root dashboard homepage. Renders a multi-module overview: Benchmark Prices, Demand Pulse, Inventory Snapshot, Latest Headlines, Upcoming Releases.
- `headline-watch/`
  - Route module for the headline product view
- `price-watch/`
  - Route module for the commodity price product view
- `calendar-watch/`
  - Route module for the calendar product view
- `inventory-watch/`
  - Route module for the integrated InventoryWatch product view
- `demand-watch/`
  - Route module for the demand-side indicator view. Data is currently curated static JS (`demand-watch/data.js`); a live backend publication workflow is in development under `scripts/publish_demandwatch_store.py`.
- `shared/`
  - Shared product shell assets, currently the cross-page tab navigation styling
- `sandbox/commodity-visual-prototype/`
  - Archived PriceWatch prototype reference. Production code lives under `price-watch/` and root `server.py`; keep this folder quarantined unless you are working on the sandbox directly.
- `server.py`
  - Main product server for the UI plus `/api/commodities/*`, `/api/calendar`, and InventoryWatch API routes served from the local published archive by default
- `app.py`
  - Existing control API for scrape and enrichment jobs
- `calendar_pipeline/`
  - CalendarWatch ingestion adapters, storage schema, CLI runner, and failure digest support
- `calendar_watch_pipeline.py`
  - CLI entrypoint for CalendarWatch ingestion

## Requirements

Install Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

`PriceWatch` also needs access to the commodity backend database views. Copy `.env.example` to `.env` if you need to point the app at non-default locations.
`InventoryWatch` browse-time support now also relies on `PyYAML`, which is included in the same root `requirements.txt` install.

If you want PriceWatch to browse entirely from a local published copy inside this repo, sync the sibling commodity pipeline database into `data/commodities.db`:

```bash
./scripts/sync_pricewatch_published_db.sh
```

When that local copy is valid, `server.py` prefers it automatically over the sibling repo.

## InventoryWatch Reference App

The separate reference frontend lives in [frontend/](/Users/jiryes/Desktop/Projects/CommodityWatch/frontend).

One-command local dev for InventoryWatch:

```bash
./scripts/run_inventorywatch_dev.sh
```

One-command InventoryWatch data refresh:

```bash
./scripts/update_inventorywatch_data.sh
```

The dev script:

- starts the FastAPI InventoryWatch backend on `127.0.0.1:8000` if it is not already running
- starts the Next.js frontend on `127.0.0.1:3000`
- points the frontend at `http://127.0.0.1:8000/api`

The refresh script now also publishes the browse-time InventoryWatch SQLite read model to `data/inventorywatch.db`.

First-time frontend install:

```bash
cd frontend
npm install
```

First-time backend setup is still required in `backend/`:

```bash
cd backend
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
alembic upgrade head
python scripts/seed_reference_data.py
```

If PostgreSQL is not installed yet on macOS, the shortest path is:

```bash
brew install postgresql@16
brew services start postgresql@16
$(brew --prefix postgresql@16)/bin/createdb commoditywatch
```

Then update `backend/.env` so `CW_DATABASE_URL` matches your local user instead of the placeholder default. Example:

```env
CW_DATABASE_URL=postgresql+asyncpg://your-mac-username@localhost:5432/commoditywatch
```

The refresh script runs `eia_wpsr`, `eia_wngs`, optional `agsi_daily`, and `seasonal_ranges` in sequence. You can also run specific jobs:

```bash
./scripts/update_inventorywatch_data.sh eia_wpsr
./scripts/update_inventorywatch_data.sh eia_wngs seasonal_ranges
```

`eia_wpsr` and `eia_wngs` require `CW_EIA_API_KEY` in `backend/.env`. `agsi_daily` requires `CW_AGSI_API_KEY`.

Frontend environment variable:

- `NEXT_PUBLIC_API_BASE_URL`
  - Optional
  - Defaults to `/api`
  - Set this when the Next.js app is not served on the same origin as the backend API

## Running the product

1. Generate or refresh the headline feed:

```bash
python3 rss_scraper.py
```

This writes to `data/feed.local.json` by default. That file is ignored by Git so your local scraping does not fight with code commits on `main`.

2. Start the product server:

```bash
python3 server.py
```

`server.py` is the intended browse-time runtime for the shipped shell. By default it serves InventoryWatch from the persisted archive under `backend/`, so the separate FastAPI/PostgreSQL stack is not required just to view the app.
When `data/inventorywatch.db` exists, `server.py` serves InventoryWatch from that published local store first.

3. Open:

```text
http://127.0.0.1:8080/
```

The root route opens the dashboard home. Use the top navigation tabs to switch to `HeadlineWatch`, `PriceWatch`, `DemandWatch`, `InventoryWatch`, or `CalendarWatch`.

## Optional control API

If you still want the existing job orchestration API, run it separately:

```bash
python3 app.py --host 127.0.0.1 --port 8081
```

## Commodity backend configuration

Environment variables supported by `server.py`:

- `COMMODITY_BACKEND_ROOT`
  - Absolute path to the commodity backend repo if it is not discoverable as a sibling `Commodity Prices` folder
- `DATABASE_URL`
  - Commodity backend database URL. Relative sqlite paths resolve from `COMMODITY_BACKEND_ROOT`
- `CALENDAR_DATABASE_URL`
  - CalendarWatch database URL. Defaults to `sqlite:///data/calendarwatch.db` relative to the repository root
- `HOST`
  - Product server bind host. Defaults to `127.0.0.1`
- `PORT`
  - Product server bind port. Defaults to `8080`
- `INVENTORYWATCH_API_BASE_URL`
  - Optional InventoryWatch backend API base for the production shell
  - Defaults to an absolute `NEXT_PUBLIC_API_BASE_URL` when that is set, otherwise `http://127.0.0.1:8000/api`
- `INVENTORYWATCH_PUBLISHED_DB_PATH`
  - Optional path to the published local InventoryWatch SQLite store
  - Defaults to `data/inventorywatch.db`
- `INVENTORYWATCH_BROWSE_MODE`
  - Optional InventoryWatch browse-time source selector
  - `auto` (default) serves from the published local store when available, otherwise the local artifact archive, otherwise proxies the separate backend
  - `local` requires a local InventoryWatch store and never calls the separate backend
  - `remote` requires the separate backend and never falls back to local archive data

If the commodity backend is unavailable, `HeadlineWatch` still loads and `PriceWatch` shows a targeted configuration error state instead of crashing the product server.
If the InventoryWatch local store is available, the shipped shell still loads and InventoryWatch works without the separate backend.

## CalendarWatch pipeline

Initialize the schema:

```bash
python3 calendar_watch_pipeline.py init-db
```

Run the first-milestone adapters:

```bash
python3 calendar_watch_pipeline.py run
```

Run a subset of adapters:

```bash
python3 calendar_watch_pipeline.py run --source eia --source fed_fomc --source ons_rss
```

Send the daily adapter-failure digest to a monitoring endpoint:

```bash
python3 calendar_watch_pipeline.py send-failure-digest --endpoint-url https://monitoring.example/calendarwatch
```

The product server exposes the CalendarWatch API at:

```text
GET /api/calendar?from=2026-03-01&to=2026-03-31&sectors=energy,macro
```

Only confirmed, publishable events are returned. Entries with unresolved redistribution checks, PDF-derived dates, press-release-derived dates, or flagged date changes stay in the review queue view until manually approved.

## Headline feed workflow

- Local development: run `python3 rss_scraper.py`
  This updates `data/feed.local.json` only.
- GitHub Actions writes the remote feed to the `feed-data` branch, not to `main`.
  That keeps `main` focused on code so your normal `git pull` and `git push` workflow is much safer.
- Each scheduled run starts from the existing `feed-data` snapshot so the branch keeps its accumulated history instead of resetting to a fresh scrape.
- GitHub Actions also runs `classifier.py`, `sentiment_finbert.py`, and `ner_spacy.py` against the `feed-data` snapshot before publishing it.
  When you sync from `feed-data`, your local `data/feed.local.json` includes those enrichments already.
- The tracked `data/feed.json` on `main` is now just a sample fallback when you do not have a local feed yet.
- If you want the latest remote overnight feed in your local ignored file, run:

```bash
./scripts/sync_remote_feed.sh
```

- If you need to update the sample fallback manually on `main`, run:

```bash
python3 rss_scraper.py --output data/feed.json
```

- If you want the server to use a specific feed file, set `COMMODITYWATCH_HEADLINE_FEED_PATH`.
- If you want the scraper default output somewhere else, set `COMMODITYWATCH_FEED_OUTPUT`.

## Git workflow

- `main`
  Code branch. This is where you build the product and push normal app changes.
- `feed-data`
  Automation branch. GitHub Actions updates this with the latest scraped feed snapshot.
- `data/feed.local.json`
  Your local ignored feed file. The app prefers this file when it exists.

Safe routine for code changes:

```bash
git status
git add -A
git restore --staged data/feed.json
git commit -m "Describe the code change"
git pull --rebase origin main
git push origin main
```

If you want the latest automated feed locally before you run the app:

```bash
./scripts/sync_remote_feed.sh
```

## Verification

Python tests:

```bash
python3 -m pytest tests/test_server.py tests/test_timestamps.py tests/test_deduplication.py -q
```

PriceWatch presentation mapping test:

```bash
node --test price-watch/tests/presentation.test.mjs
```

Syntax checks:

```bash
python3 -m py_compile server.py app.py rss_scraper.py argus_scraper.py
node --check price-watch/app.js
node --check price-watch/commodities-client.js
node --check price-watch/commodity-presentation.js
```
