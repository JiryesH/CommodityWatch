# Contango

Contango now ships as a two-view product:

- `HeadlineWatch` for the existing live headline workflow backed by a local `data/feed.local.json` override when present, otherwise the tracked `data/feed.json` sample snapshot
- `PriceWatch` for the commodity visual page backed by published commodity database views

## Product structure

- `headline-watch/`
  - Route module for the headline product view
- `price-watch/`
  - Route module for the commodity price product view
- `shared/`
  - Shared product shell assets, currently the cross-page tab navigation styling
- `sandbox/commodity-visual-prototype/`
  - Experimental/reference PriceWatch prototype; production code lives under `price-watch/` and root `server.py`
- `server.py`
  - Main product server for the UI plus `/api/commodities/*`
- `app.py`
  - Existing control API for scrape and enrichment jobs

## Requirements

Install Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

`PriceWatch` also needs access to the commodity backend database views. Copy `.env.example` to `.env` if you need to point the app at a non-default backend location.

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

The root route opens `HeadlineWatch`. Use the top navigation tabs to switch to `PriceWatch`.

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
- `HOST`
  - Product server bind host. Defaults to `127.0.0.1`
- `PORT`
  - Product server bind port. Defaults to `8080`

If the commodity backend is unavailable, `HeadlineWatch` still loads and `PriceWatch` shows a targeted configuration error state instead of crashing the product server.

## Headline feed workflow

- Local development: run `python3 rss_scraper.py`
  This updates `data/feed.local.json` only.
- GitHub Actions writes the remote feed to the `feed-data` branch, not to `main`.
  That keeps `main` focused on code so your normal `git pull` and `git push` workflow is much safer.
- Each scheduled run starts from the existing `feed-data` snapshot so the branch keeps its accumulated history instead of resetting to a fresh scrape.
- The tracked `data/feed.json` on `main` is now just a sample fallback when you do not have a local feed yet.
- If you want the latest remote overnight feed in your local ignored file, run:

```bash
./scripts/sync_remote_feed.sh
```

- If you need to update the sample fallback manually on `main`, run:

```bash
python3 rss_scraper.py --output data/feed.json
```

- If you want the server to use a specific feed file, set `CONTANGO_HEADLINE_FEED_PATH`.
- If you want the scraper default output somewhere else, set `CONTANGO_FEED_OUTPUT`.

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
