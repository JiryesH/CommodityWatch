# Commodity Price Watch Sandbox Architecture

Archived sandbox notes. Production PriceWatch lives under `/Users/jiryes/Desktop/Projects/CommodityWatch/price-watch/` and should be treated as the source of truth for current product behavior.

This sandbox now uses a configuration-first visualization engine so new commodities can be added with minimal code changes.

## Core design

1. Registry-driven definitions
- All commodity metadata lives in `COMMODITY_REGISTRY` in `app.js`.
- Each entry declares:
  - id, labels, category/group tags
  - price bounds and display precision
  - mock behavior (`initial`, `mockDrift`)
  - visualizer type and options

2. Visualization modules
- Each visual metaphor is encapsulated in a visualizer class with a shared interface:
  - `createScene(bounds)`
  - `update({ value, previous, normalized, replay, bounds })`
  - `onResize()`
- Current modules:
  - `EnergyTileVisualizer` (oil, natural gas, lng, gasoline, thermal coal, diesel, rubber)
  - `PeriodicTileVisualizer` (metals periodic-tile system)
  - `AgriTileVisualizer` (wheat/corn/soybeans/soybean oil/palm oil/rice/lumber/coffee/sugar/cotton/cocoa)

3. Rendering pipeline
- `CommoditySandboxEngine` owns:
  - state store (`current`, `previous`)
  - card generation
  - top-level category filtering
  - detail spotlight rendering (flip card + chart)
  - per-commodity rendering
  - mock feed ticking
  - resize behavior

4. UI generation
- Cards are generated from registry definitions.
- This avoids hand-written HTML per commodity and keeps growth manageable.

## Data pipeline integration point (future)

This architecture is prepared for real-price ingestion later.

Recommended next integration shape:

1. Add a `PriceAdapter` layer
- Replace `tickMockFeed()` with `adapter.fetchLatest()`.
- Adapter returns normalized payload:
  - `{ id, value, source, observedAt }[]`

2. Validation stage
- Clamp out-of-range values by registry bounds.
- Flag stale or null values before rendering.

3. Attribution stage
- Store source metadata per commodity for citation in UI footer/tooltips.

4. Rendering stage
- Write validated values into engine state and call `renderAll()` (or a targeted commodity render).

## Adding a new commodity

1. Add one object to `COMMODITY_REGISTRY`.
2. Reuse an existing visualizer type or add a new visualizer class.
3. Add CSS for the new visualizer scene only.
4. No manual HTML card or slider wiring required.
5. Metals are routed to a dedicated periodic row (`metals-grid`) for a homogeneous table-like layout.
6. Agriculture/soft commodities are routed to a dedicated row (`agri-grid`) using code-first market tiles.
7. Category routing is deterministic: energy cards render in `featured-grid`, metals in `metals-grid`, and agri/softs in `agri-grid`.
