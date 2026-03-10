# Contango

Contango now ships as a two-view product:

- `HeadlineWatch` for the existing live headline workflow backed by `data/feed.json`
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
