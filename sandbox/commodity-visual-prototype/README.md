# Commodity Price Watch

This directory is sandbox-only reference code. Production PriceWatch lives in `/Users/jiryes/Desktop/Projects/Contango/price-watch/` and is served by `/Users/jiryes/Desktop/Projects/Contango/server.py`.

Static Contango-style frontend wired to the commodity ingestion backend in `/Users/jiryes/Desktop/Projects/Commodity Prices`.

## Integration architecture
- The frontend stays browser-first and static.
- A tiny same-origin Python server in [`server.py`](/Users/jiryes/Desktop/Projects/Contango/sandbox/commodity-visual-prototype/server.py) serves the static assets plus `/api/commodities/*`.
- The API layer reads only the backend's published views:
  - `published_series`
  - `published_latest_observations`
  - `published_observations`
- The UI uses `actual_series_name` as the primary label, keeps `target_concept` as secondary context, and surfaces `match_type` honestly.
- Missing concepts never appear because the published views already exclude `match_type='missing'`.

## Run against live backend data
1. Copy `.env.example` to `.env` if you need to override the backend location or database URL.
2. Ensure the backend database exists and is populated. The normal backend refresh commands are:

```bash
commodity-pipeline bootstrap-all
commodity-pipeline update-due
commodity-pipeline show-freshness
```

3. Start the frontend/API server from this folder:

```bash
python3 server.py
```

4. Visit `http://127.0.0.1:8080`.

## Environment
- `COMMODITY_BACKEND_ROOT`
  - Optional absolute path to the backend repo. If omitted, the server looks for a sibling folder named `Commodity Prices`.
- `DATABASE_URL`
  - Uses the same setting as the backend pipeline. Defaults to `sqlite:///data/commodities.db`, resolved relative to `COMMODITY_BACKEND_ROOT`.
- `PORT`
  - Local server port. Defaults to `8080`.

## API contract
- `GET /api/commodities/series`
  - Published catalog rows from `published_series`, ordered by `actual_series_name`.
- `GET /api/commodities/latest`
  - Latest published rows from `published_latest_observations` with previous-value/delta enrichment derived from `published_observations`.
- `GET /api/commodities/:seriesKey/history?start=YYYY-MM-DD&end=YYYY-MM-DD`
  - Historical published observations for one `series_key`, filtered only through `published_observations`.

## Verification
- API route coverage:

```bash
python3 -m pytest tests/test_server.py -q
```

- Frontend data-mapping check:

```bash
node --test tests/presentation.test.mjs
```

- Syntax checks:

```bash
python3 -m py_compile server.py
node --check app.js
node --check commodities-client.js
node --check commodity-presentation.js
```

## Notes
- This integration is read-only. Secrets such as `FRED_API_KEY` stay backend-only.
- The frontend still uses the existing visual language; only the data layer was swapped from mock state to published backend data.
- See `ARCHITECTURE.md` for the original prototype notes.
