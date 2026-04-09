# DemandWatch Operator Runbook

DemandWatch MVP only refreshes legally cleared sources already seeded in the backend:

- `demand_eia_wpsr`
- `demand_eia_grid_monitor`
- `demand_fred_g17`
- `demand_fred_new_residential_construction`
- `demand_usda_wasde`
- `demand_usda_export_sales`
- `demand_ember_monthly_electricity`

Blocked or deferred indicators remain placeholders by design. Do not add raw PMI values, direct China customs/NBS series, OPEC/IEA restricted tables, or DemandWatch-owned Weather/HDD/CDD storage in these runs.

## Prerequisites

- Run database migrations before any ingest command.
- Set the backend database and source credentials in the `CW_*` environment variables used by the backend.
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

Publish the SQLite store used by downstream readers:

```bash
python scripts/demandwatch.py publish
```

Run the operational audit and fail if any feed is degraded or worse:

```bash
python scripts/demandwatch.py audit --fail-on degraded
```

## Outputs

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
