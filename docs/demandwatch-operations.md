# DemandWatch Operator Runbook

DemandWatch MVP only refreshes legally cleared sources already seeded in the backend:

- `demand_eia_wpsr`
- `demand_eia_grid_monitor`
- `demand_fred_g17`
- `demand_fred_new_residential_construction`
- `demand_fred_motor_vehicle_sales`
- `demand_fred_traffic_volume_trends`
- `demand_oecd_cli`
- `demand_usda_wasde`
- `demand_usda_export_sales`
- `demand_ember_monthly_electricity`

Deferred indicators remain placeholders by design. Do not add direct China customs/NBS series, Japan LNG imports, South Korea LNG imports, India Department of Fertilizers monthly summaries, worldsteel monthly crude steel tables, or DemandWatch-owned Weather/HDD/CDD storage in these runs unless their source-specific blockers have been cleared and implemented.

## Prerequisites

- Run database migrations before any ingest command.
- Seed the DemandWatch registry before any ingest command.
- Set the backend database and source credentials in the `CW_*` environment variables used by the backend.
- Required for the currently wired DemandWatch feeds:
  - `CW_DATABASE_URL`
  - `CW_EIA_API_KEY` for `demand_eia_wpsr` and `demand_eia_grid_monitor`
  - `CW_FRED_API_KEY` for `demand_fred_g17`, `demand_fred_new_residential_construction`, `demand_fred_motor_vehicle_sales`, and `demand_fred_traffic_volume_trends`
  - `CW_EMBER_API_KEY` for `demand_ember_monthly_electricity`
  - `demand_oecd_cli`, USDA WASDE/PSD, and USDA export sales use public endpoints in the MVP and do not require API keys
- If `CW_EMBER_API_KEY` is missing, the Ember refresh records `partial` by design and the electricity/global coverage audit remains degraded.
- `demand_oecd_cli` stores latest published snapshot vintages by fetch time. The OECD public SDMX pull is treated as an attributed published release feed, not as a historical-vintage archive like FRED.
- Run commands from `/Users/jiryes/Desktop/Projects/CommodityWatch/backend`.

## Commands

Refresh the live-safe feeds:

```bash
python scripts/demandwatch.py refresh
```

Refresh only one feed and keep going if another feed fails:

```bash
python scripts/demandwatch.py refresh --source demand_fred_g17 --continue-on-error
```

Backfill the standard three-year window for one or more feeds:

```bash
python scripts/demandwatch.py backfill --source demand_eia_wpsr --source demand_usda_wasde
```

Backfill a fixed window:

```bash
python scripts/demandwatch.py backfill --source demand_ember_monthly_electricity --from 2023-01-01 --to 2026-04-08
```

Backfill the OECD CLI context series:

```bash
python scripts/demandwatch.py backfill --source demand_oecd_cli --from 2023-01-01 --to 2026-04-08
```

Publish the SQLite store used by downstream readers:

```bash
python scripts/demandwatch.py publish
```

Run the operational audit and fail if any feed is degraded or worse:

```bash
python scripts/demandwatch.py audit --fail-on degraded
```

## Outputs

Paths below assume the default `CW_ARTIFACT_ROOT=./artifacts`.

- Published store default: `backend/artifacts/demandwatch/published.sqlite`
- Audit JSON default: `backend/artifacts/demandwatch/audit.json`
- Audit Markdown default: `backend/artifacts/demandwatch/audit.md`

## What The Audit Checks

- Feed health using recent ingest runs plus stale-series detection
- Parse-failure history over the last 30 days
- Canonical-unit policy drift against the seeded DemandWatch series registry
- Coverage status by vertical (`live`, `partial`, `deferred`, `blocked`)

## Failure Handling

- Retry/backoff is applied at the DemandWatch operations layer for feeds that do not already manage retries internally.
- Parse failures are classified and persisted as failed ingest runs with stage metadata.
- Duplicate parsed rows are dropped before observation upserts.
- Observation upserts remain idempotent, so rerunning the same window is safe.

## Interpreting Feed Health

- `healthy`: latest run succeeded and active series are fresh
- `degraded`: partial coverage, stale series, or unit-policy issues exist
- `failing`: no successful recent run, repeated failures, or all active series are stale
- `deferred`: no live-safe indicators are assigned to that feed in the MVP
