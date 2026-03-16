# CommodityWatch

CommodityWatch now ships as a three-view product:

- `HeadlineWatch` for the existing live headline workflow backed by a local `data/feed.local.json` override when present, otherwise the tracked `data/feed.json` sample snapshot
- `PriceWatch` for the commodity visual page backed by published commodity database views
- `CalendarWatch` for publishable commodity-market calendar events backed by the local calendar ingestion pipeline and `/api/calendar`

## Product structure

- `headline-watch/`
  - Route module for the headline product view
- `price-watch/`
  - Route module for the commodity price product view
- `calendar-watch/`
  - Route module for the calendar product view
- `shared/`
  - Shared product shell assets, currently the cross-page tab navigation styling
- `sandbox/commodity-visual-prototype/`
  - Experimental/reference PriceWatch prototype; production code lives under `price-watch/` and root `server.py`
- `server.py`
  - Main product server for the UI plus `/api/commodities/*` and `/api/calendar`
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

3. Open:

```text
http://127.0.0.1:8080/
```

The root route opens `Home`. Use the top navigation tabs to switch to `HeadlineWatch`, `PriceWatch`, or `CalendarWatch`.

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

If the commodity backend is unavailable, `HeadlineWatch` still loads and `PriceWatch` shows a targeted configuration error state instead of crashing the product server.

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
