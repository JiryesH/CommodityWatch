# PriceWatch Integration Report

## Objective

Integrate the sandboxed `PriceWatch` prototype into the main Contango product without turning the app into a larger monolith, while preserving the existing headline workflow as `HeadlineWatch`.

## Architecture analysis

### Main product before integration

- The existing product lived in one large root `index.html`
- Styling, markup, and behavior were all inline in that file
- Headline data came from `data/feed.json`
- Operational control for scrapes and enrichment lived separately in `app.py`

### Sandbox before integration

- The `PriceWatch` prototype already had cleaner boundaries:
  - `server.py` for the commodity API and static serving
  - `commodities-client.js` for API access
  - `commodity-presentation.js` for turning published series into UI definitions
  - `app.js` for the page engine and interactions
- The prototype assumed a published commodity backend database and same-origin API routes

### Integration constraints

- `HeadlineWatch` needed to remain stable because it is the current workflow
- `PriceWatch` needed same-origin API serving to avoid turning the production app into a multi-process front-end setup
- Page removal and future edits needed to happen at the page boundary, not by touching a shared monolithic file

## Implementation summary

### Page modularization

- Moved the existing headline experience into `/headline-watch/`
- Moved the productionized commodity page into `/price-watch/`
- Added `/shared/site-shell.css` for shared product-level tab navigation styling
- Replaced the old root entrypoint with a lightweight redirect to `/headline-watch/`

### PriceWatch productization

- Copied the reusable sandbox modules into the main product under `/price-watch/`
- Kept the sandbox itself untouched as a reference implementation
- Added shared tab navigation so `PriceWatch` reads as part of the same product
- Preserved the configuration-first commodity presentation pipeline from the sandbox

### Unified serving model

- Added root `server.py` as the product server
- `server.py` serves:
  - `/` -> `HeadlineWatch`
  - `/headline-watch/`
  - `/price-watch/`
  - `/api/health`
  - `/api/commodities/*`
- Left `app.py` in place as the separate control API for headline scraping/enrichment jobs

## Weaknesses found and fixed

### Weakness 1: Product availability coupled to commodity database availability

Initial risk:
- The first server draft would fail to start if the commodity database was missing, which would take down `HeadlineWatch` even though it does not depend on commodity data

Fix:
- Introduced lazy commodity repository initialization in `server.py`
- The product server now starts even when the commodity backend is unavailable
- `PriceWatch` fails locally with a clear UI message instead of collapsing the entire app

### Weakness 2: Empty and misconfigured PriceWatch states were ambiguous

Initial risk:
- A missing database or empty published series set would have looked like a broken or blank page

Fix:
- Added explicit global error and empty-state rendering in `/price-watch/app.js`
- Added configuration guidance for `COMMODITY_BACKEND_ROOT` and `DATABASE_URL`

### Weakness 3: No production route verification

Initial risk:
- The integrated route structure and commodity API wiring were not covered by the main repo test suite

Fix:
- Added `/tests/test_server.py` to verify:
  - root product route
  - `PriceWatch` route
  - health route degradation behavior
  - commodity series/latest/history/detail endpoints

## Residual risks

- `HeadlineWatch` is now isolated as its own route module, but it still carries its legacy inline CSS and JS internally. The page boundary is clean, but an internal extraction to dedicated assets would be the next refactor if you want smaller review diffs inside that page.
- `PriceWatch` still depends on an external commodity backend contract. The product now degrades safely, but the quality of that view is only as good as the published views it receives.
- The current shared shell is intentionally thin. If more pages are added, promoting the shared nav/footer into a small template layer would reduce duplication further.

## Validation performed

- `python3 -m pytest tests/test_server.py tests/test_timestamps.py tests/test_deduplication.py -q`
- `node --test price-watch/tests/presentation.test.mjs`
- `python3 -m py_compile server.py app.py rss_scraper.py argus_scraper.py`
- `node --check price-watch/app.js`
- `node --check price-watch/commodities-client.js`
- `node --check price-watch/commodity-presentation.js`

All passed in the final verification run.
