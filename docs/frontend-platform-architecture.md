# CommodityWatch Frontend Architecture

This document is the frontend counterpart to [backend-platform-architecture.md](/Users/jiryes/Desktop/Projects/CommodityWatch/docs/backend-platform-architecture.md). It translates the module planning documents into a buildable web architecture for the current CommodityWatch product.

The current repo ships a vanilla HTML/CSS/JS dashboard shell with live `HeadlineWatch`, `PriceWatch`, and `CalendarWatch` routes. The recommendation here is to migrate the platform to one typed frontend application incrementally, not to rewrite everything in one step.

## 1. Final Stack Decision

Use this stack:

| Layer | Choice | Why |
|---|---|---|
| Framework | `Next.js 15` + `React 19` + `TypeScript` | Best fit for public SEO pages, app-style routing, shared layouts, and a large React ecosystem. The builder gets one framework that handles routing, metadata, code splitting, and deployment cleanly. |
| Charting | `Plotly` wrapped behind shared chart components | Best fit for the hardest problem in the product: seasonal range charts, dual-axis overlays, annotations, zoom/pan, and heat-map-capable visuals. It is also the most Python-friendly mental model. |
| Styling | `Tailwind CSS` + CSS variables for design tokens | Fastest path for a solo builder learning frontend. Tailwind handles layout density well, while CSS variables keep theming, dark mode, and chart colors centralized. |
| Server-state fetching | `TanStack Query` | Correct tool for API-heavy dashboards, caching, stale windows, retries, pagination, and prefetching. |
| App/UI state | URL search params first, `Zustand` only for ephemeral UI state | Filters must be shareable and bookmarkable. Zustand stays limited to UI-only state such as drawer open state, chart sync, and density toggles. |
| Component primitives | `shadcn/ui` on top of `Radix UI` | Best balance of speed and control for a dense professional UI. It avoids the default look of big UI kits and keeps components readable in the codebase. |
| Tables | `@tanstack/react-table` | CommodityWatch needs dense, sortable, exportable tables. This is the right table engine. |
| Validation | `zod` | Validate backend responses at the edge of the app and avoid silent shape drift. |

### Why this is the right path

- `Next.js` is more complex than a plain SPA, but it removes more problems than it adds for this product: SEO, metadata, layouts, static marketing pages, route structure, and incremental migration.
- `Plotly` is heavier than `Recharts`, but it materially de-risks the signature visualizations. For CommodityWatch, chart correctness matters more than a smaller library that forces custom SVG work on the hardest chart.
- `TypeScript` should be used from day one. This platform has many modules, many shared data contracts, and many similar-looking indicator shapes. Type errors are cheaper than runtime debugging.

## 2. Architecture Principles

1. Build one frontend app, not seven mini-apps.
2. Keep route identity in the URL and keep secondary filters in query params.
3. Fetch data at the page or feature-container level; presentational components should mostly receive props.
4. Wrap Plotly once inside shared chart primitives. No module page should talk to Plotly directly.
5. Preserve the current CommodityWatch shell pattern: strong top navigation, dense content area, restrained typography, absolute timestamps.
6. Treat data density as a feature. Use tighter spacing, compact cards, sticky sub-navs, and right rails instead of oversized marketing layouts.
7. Support dark mode from the start with tokenized colors, not ad hoc overrides.
8. Migrate module by module. Keep current routes alive with redirects while new routes land.

## 3. Frontend Application Shape

### Application model

- Public marketing and overview pages are server-rendered with Next metadata.
- Data-heavy module pages are hybrid:
  - server `page.tsx` resolves route params, metadata, and any public initial payload
  - client feature containers handle filters, pagination, charts, and refetching via TanStack Query
- The browser talks to the backend on the same origin path: `/api/...`
- Do not create a frontend BFF layer initially. The frontend should consume the backend REST API directly.

### Migration rule

Adopt these clean routes:

```text
/headlines
/prices
/calendar
/inventory
/supply
/demand
/weather
```

Keep legacy redirects:

```text
/headline-watch  -> /headlines
/price-watch     -> /prices
/calendar-watch  -> /calendar
```

## 4. Design System Specification

### 4.1 Theme tokens

Use `src/styles/tokens.css` with class-based theme switching on `html[data-theme="light"]` and `html[data-theme="dark"]`.

```css
:root,
html[data-theme="light"] {
  --color-bg-app: #f2f2f2;
  --color-bg-canvas: #e8e8ea;
  --color-bg-surface: #ffffff;
  --color-bg-surface-alt: #f7f7f8;
  --color-bg-elevated: #fcfcfc;
  --color-bg-muted: #e0e0e0;
  --color-bg-overlay: rgba(26, 26, 46, 0.66);

  --color-text-primary: #1a1a2e;
  --color-text-secondary: #3d3d4f;
  --color-text-muted: #555555;
  --color-text-soft: #71717f;
  --color-text-inverse: #f4f4f7;

  --color-border-subtle: #d7d7dd;
  --color-border-default: #c7c7ce;
  --color-border-strong: #9f9faa;

  --color-accent: #d4a017;
  --color-accent-hover: #b88912;
  --color-accent-soft: rgba(212, 160, 23, 0.12);
  --color-accent-strong: #8f6c0c;

  --color-positive: #2d7d46;
  --color-positive-soft: rgba(45, 125, 70, 0.12);
  --color-negative: #b82020;
  --color-negative-soft: rgba(184, 32, 32, 0.12);
  --color-caution: #b36b00;
  --color-caution-soft: rgba(179, 107, 0, 0.12);
  --color-neutral: #555555;
  --color-neutral-soft: rgba(85, 85, 85, 0.1);
  --color-info: #2e5e8e;
  --color-info-soft: rgba(46, 94, 142, 0.12);

  --color-sector-energy: #d4a017;
  --color-sector-metals: #2e5e8e;
  --color-sector-agriculture: #5e7f34;

  --chart-grid: rgba(26, 26, 46, 0.1);
  --chart-axis: rgba(26, 26, 46, 0.45);
  --chart-band-10-90: rgba(26, 26, 46, 0.06);
  --chart-band-25-75: rgba(26, 26, 46, 0.12);
  --chart-median: rgba(85, 85, 85, 0.9);
  --chart-current-year: #d4a017;
  --chart-prior-year: rgba(85, 85, 85, 0.82);
  --chart-positive: #2d7d46;
  --chart-negative: #b82020;
  --chart-info: #2e5e8e;
  --chart-annotation: #2e5e8e;
  --chart-tooltip-bg: #141423;
  --chart-tooltip-border: rgba(255, 255, 255, 0.12);
  --chart-tooltip-text: #f4f4f7;

  --shadow-card: 0 1px 2px rgba(26, 26, 46, 0.04), 0 8px 24px rgba(26, 26, 46, 0.06);
}

html[data-theme="dark"] {
  --color-bg-app: #10121b;
  --color-bg-canvas: #151826;
  --color-bg-surface: #1a1d2b;
  --color-bg-surface-alt: #202434;
  --color-bg-elevated: #262b3d;
  --color-bg-muted: #2d3347;
  --color-bg-overlay: rgba(6, 8, 14, 0.72);

  --color-text-primary: #f2f3f7;
  --color-text-secondary: #d6d8df;
  --color-text-muted: #a7acb8;
  --color-text-soft: #8a90a0;
  --color-text-inverse: #0f1118;

  --color-border-subtle: #2e3447;
  --color-border-default: #3b4157;
  --color-border-strong: #555d78;

  --color-accent: #d4a017;
  --color-accent-hover: #e1b43a;
  --color-accent-soft: rgba(212, 160, 23, 0.16);
  --color-accent-strong: #f0c14b;

  --color-positive: #49a264;
  --color-positive-soft: rgba(73, 162, 100, 0.18);
  --color-negative: #d14b4b;
  --color-negative-soft: rgba(209, 75, 75, 0.18);
  --color-caution: #d08b1f;
  --color-caution-soft: rgba(208, 139, 31, 0.18);
  --color-neutral: #9a9fae;
  --color-neutral-soft: rgba(154, 159, 174, 0.16);
  --color-info: #5f8fbe;
  --color-info-soft: rgba(95, 143, 190, 0.18);

  --color-sector-energy: #d4a017;
  --color-sector-metals: #5f8fbe;
  --color-sector-agriculture: #7fa04d;

  --chart-grid: rgba(242, 243, 247, 0.1);
  --chart-axis: rgba(242, 243, 247, 0.55);
  --chart-band-10-90: rgba(242, 243, 247, 0.08);
  --chart-band-25-75: rgba(242, 243, 247, 0.16);
  --chart-median: rgba(215, 218, 223, 0.92);
  --chart-current-year: #d4a017;
  --chart-prior-year: rgba(167, 172, 184, 0.92);
  --chart-positive: #49a264;
  --chart-negative: #d14b4b;
  --chart-info: #5f8fbe;
  --chart-annotation: #5f8fbe;
  --chart-tooltip-bg: #0e111a;
  --chart-tooltip-border: rgba(255, 255, 255, 0.12);
  --chart-tooltip-text: #f2f3f7;

  --shadow-card: 0 1px 2px rgba(0, 0, 0, 0.32), 0 10px 30px rgba(0, 0, 0, 0.24);
}
```

### 4.2 Typography scale

Use `IBM Plex Sans` for UI and `IBM Plex Mono` for numbers, labels, timestamps, and table values.

| Token | Size / line-height | Weight | Use |
|---|---|---|---|
| `--text-display` | `32px / 38px` | 600 | Homepage headline, major module titles |
| `--text-h1` | `26px / 32px` | 600 | Module page title |
| `--text-h2` | `20px / 26px` | 600 | Section headers |
| `--text-h3` | `16px / 22px` | 600 | Card headers |
| `--text-body` | `14px / 20px` | 400 | Default body |
| `--text-body-strong` | `14px / 20px` | 500 | Table labels, active filters |
| `--text-caption` | `12px / 16px` | 400 | Source lines, helper text |
| `--text-micro` | `11px / 14px` | 500 | Badges, ticker labels, uppercase chips |
| `--text-data-xl` | `28px / 30px` | 500 mono | Main card value |
| `--text-data-lg` | `20px / 24px` | 500 mono | Secondary values |
| `--text-data-md` | `14px / 18px` | 400 mono | Table values |
| `--text-data-sm` | `12px / 16px` | 400 mono | Timestamps, source metadata |

Rules:

- Never use serif text in data views.
- Keep card labels and timestamps in mono uppercase or small caps style.
- Use tabular numerals everywhere values align in rows or tables.

### 4.3 Spacing system

Use a 4px base scale.

```css
--space-0: 0;
--space-1: 4px;
--space-2: 8px;
--space-3: 12px;
--space-4: 16px;
--space-5: 20px;
--space-6: 24px;
--space-8: 32px;
--space-10: 40px;
--space-12: 48px;
--space-16: 64px;
```

Defaults:

- Card padding: `16px`
- Dense data card padding: `12px`
- Section gap: `24px`
- Grid gap: `16px`
- Table cell padding: `10px 12px`
- Filter bar height: `44px`
- Top app bar height: `52px`

### 4.4 Reusable component patterns

#### Data card

- Surface: `var(--color-bg-surface)`
- Border: `1px solid var(--color-border-subtle)`
- Radius: `10px`
- Padding: `12px` on dense grids, `16px` on detail pages
- Structure:
  - header row: label + freshness badge
  - value row: main metric in mono
  - context row: change, deviation, and optional percentile badge
  - footer row: source/timestamp left, sparkline right
- Signal styling:
  - tightening / positive: left rule `var(--color-positive)`
  - loosening / negative: left rule `var(--color-negative)`
  - caution / stale: left rule `var(--color-caution)`

#### Indicator row

- Use inside dense tables and scorecards
- Fixed columns on desktop:
  - name
  - value
  - change
  - deviation
  - freshness
  - source
- Collapse on mobile to:
  - name + badges
  - value
  - stacked meta row

#### Chart container

- Title bar with:
  - title
  - subtitle
  - source attribution
  - range selector on the right
- Body min-heights:
  - desktop `420px`
  - tablet `340px`
  - mobile `280px`
- Footer:
  - notes
  - annotations legend
  - export CSV button if applicable

#### Sparkline

- Render as inline SVG, not Plotly
- Width `88px`, height `24px`
- Stroke width `2`
- No fill, no axes, no markers
- Positive trend stroke `var(--color-positive)`
- Negative trend stroke `var(--color-negative)`
- Neutral trend stroke `var(--color-neutral)`

#### Directional indicator

- Up: `↑` in green
- Down: `↓` in red
- Flat: `→` in neutral
- Render with mono text, not icon font

#### Alert badge

- Pill shape, `11px` mono
- Variants:
  - `extreme-low`
  - `extreme-high`
  - `fresh`
  - `aged`
  - `awaiting-release`
  - `estimate`
  - `proxy`
  - `observed`
  - `disruption`

#### Source attribution

- Format: `SOURCE · 2026-03-27 10:30 UTC`
- Always mono, always visible, never hidden behind a tooltip

#### Freshness indicator

- `Live`: updated < 24h
- `Current`: within expected release window
- `Lagged`: release not yet stale but not current cadence
- `Structural`: quarterly or annual series still valid
- `Aged`: likely overdue or missing

#### Module navigation tabs

- Primary module nav in top shell
- Secondary local nav on each module page
- Active state:
  - 2px bottom border `var(--color-accent)`
  - text `var(--color-text-primary)`

#### Filters

- Use shadcn `Select`, `Popover`, `Command`, and `DatePicker`
- All filter controls map to URL params
- Default dense height `36px`
- Do not use oversized pill chips for every filter on desktop; use compact dropdowns with visible active values

## 5. Routing and Page Architecture

### Route map

| Route | Page title / meta description | API on initial load | Key components | Loading strategy | Mobile behavior |
|---|---|---|---|---|---|
| `/` | `CommodityWatch | Commodity intelligence across headlines, prices, calendar, inventories, supply, demand, and weather` / `Monitor commodity headlines, benchmark prices, release calendars, inventories, supply, demand, and weather in one platform.` | `GET /api/headlines?limit=8`; `GET /api/calendar?from=today&to=today+14d&limit=8`; `GET /api/snapshot/inventorywatch?limit=6` | `AppShell`, `PlatformHero`, `ModuleNav`, `HeadlineFeedPreview`, `CalendarPreview`, `SnapshotPreviewGrid` | Server-render the hero and preview shells; hydrate preview feeds with `initialData` | Single-column stack; previews become swipeable sections |
| `/headlines` | `HeadlineWatch | Commodity headlines` / `Filter commodity headlines by vertical, source, module origin, and date range.` | `GET /api/headlines` with URL filters | `AppShell`, `ModuleNav`, `HeadlineFilters`, `HeadlineFeed`, `HeadlineItem`, `SourceFilter` | Infinite-query skeleton list, preserve previous page while filter changes | Filters move into drawer; item rows stay dense with source and timestamp visible |
| `/prices` | `PriceWatch | Benchmark commodity prices` / `Track commodity benchmark prices and overlay cross-module context.` | `GET /api/indicators?module=pricewatch&measure_family=price`; selected benchmarks `GET /api/indicators/{id}/data` | `AppShell`, `ModuleNav`, `BenchmarkStrip`, `PriceDashboard`, `PriceChart`, `OverlayToggle` | Server render benchmark shell; selected charts hydrate client-side | Benchmark strip becomes horizontal scroll; chart range defaults to `1Y` |
| `/calendar` | `CalendarWatch | Commodity data release calendar` / `See upcoming commodity data releases across inventories, supply, demand, weather, and macro.` | `GET /api/calendar?from=...&to=...` | `AppShell`, `ModuleNav`, `CalendarToolbar`, `CalendarGrid`, `ReleaseEvent`, `ModuleFilter` | Skeleton calendar grid plus right-side release list | Month grid collapses to agenda list by default |
| `/inventory` | `InventoryWatch | Market snapshot` / `Track commodity inventories, builds, draws, and deviations from seasonal norms.` | `GET /api/snapshot/inventorywatch?include_sparklines=true`; `GET /api/indicators?module=inventorywatch` | `AppShell`, `ModuleNav`, `ReleaseStrip`, `InventorySnapshot`, `IndicatorCard`, `AlertBanner` | Snapshot cards load first; indicator catalog loads in parallel for filters | Cards stack one per row; sparkline stays visible on the right |
| `/inventory/[commodity]` | `InventoryWatch | {commodity}` / `Inventory snapshot and tracked indicators for {commodity}.` | `GET /api/snapshot/inventorywatch?commodity=...`; `GET /api/indicators?module=inventorywatch&commodity=...` | `AppShell`, `ModuleNav`, `CommodityHeader`, `InventorySnapshot`, `IndicatorList`, `UpcomingReleasesRail` | Snapshot visible immediately; detail list skeletons under header | Right rail moves below content |
| `/inventory/[commodity]/[indicatorId]` | `InventoryWatch | {indicator}` / `Seasonal inventory chart, recent changes, and release history for {indicator}.` | `GET /api/indicators/{id}/data?include_seasonal=true`; `GET /api/indicators/{id}/latest`; `GET /api/calendar?module=inventorywatch&commodity=...&from=now&to=now+30d&limit=5` | `AppShell`, `IndicatorHeader`, `SeasonalRangeChart`, `ChangeBarChart`, `DataTable`, `SourceAttribution`, `FreshnessBadge` | Chart skeleton first, then chart and table hydrate separately | Default tab is chart; table becomes secondary tab |
| `/supply` | `SupplyWatch | Supply snapshot` / `Track observed output, utilisation, and supply proxies across commodity markets.` | `GET /api/snapshot/supplywatch?include_sparklines=true`; `GET /api/calendar?module=supplywatch&from=now&to=now+14d&limit=6` | `AppShell`, `ModuleNav`, `ReleaseStrip`, `SupplySnapshot`, `SupplyMoversFeed`, `DisruptionWatch` | Snapshot grid first, movers feed second | Snapshot cards stack; disruption panel turns into accordion |
| `/supply/[commodity]` | `SupplyWatch | {commodity}` / `Observed output, proxies, capacity, and regional breakdown for {commodity}.` | `GET /api/indicators?module=supplywatch&commodity=...`; selected indicator `GET /api/indicators/{id}/data`; `GET /api/calendar?...` | `AppShell`, `CommodityHeader`, `ProductionChart`, `CapacityGauge`, `RegionalComparison`, `RevisionLedger` | Hero and tabs render first; each panel fetches independently | Regional bars become carousel cards; revision ledger collapses |
| `/demand` | `DemandWatch | Demand pulse` / `Track commodity demand scorecards, movers, and macro context.` | `GET /api/snapshot/demandwatch?include_sparklines=false`; `GET /api/indicators?module=demandwatch&measure_family=macro` then `GET /api/indicators/{id}/latest` for strip IDs | `AppShell`, `ModuleNav`, `MacroStrip`, `DemandScorecard`, `DemandMoverFeed` | Macro strip loads independently from scorecard; no blocking spinners | Macro strip becomes swipe row; scorecard becomes stacked rows |
| `/demand/[commodity]` | `DemandWatch | {commodity}` / `Demand trends, proxies, trade flows, and macro context for {commodity}.` | `GET /api/indicators?module=demandwatch&commodity=...`; selected indicator `GET /api/indicators/{id}/data`; optional `GET /api/weather/current?geography=...`; `GET /api/calendar?...` | `AppShell`, `CommodityHeader`, `DemandSummary`, `TimeSeriesChart`, `DemandMoverFeed`, `DataTable`, `MacroStrip` | Primary demand chart renders first, supporting panels stream in below | Tabs separate `Direct`, `Proxy`, `Trade`, `Macro` |
| `/weather` | `WeatherWatch | Weather story for markets` / `See the current weather story for energy and agriculture markets.` | `GET /api/weather/current`; `GET /api/headlines?module_origin=weatherwatch&limit=5`; selected weather indicators via `GET /api/indicators?module=weatherwatch` | `AppShell`, `ModuleNav`, `WeatherBriefing`, `WeatherIndicatorTiles`, `EmbeddedMapProduct`, `WeatherHeadlineRail` | Briefing and tiles load before maps; maps lazy-load below fold | Briefing becomes top card; maps collapse into a swipe stack |
| `/weather/enso` | `WeatherWatch | ENSO` / `Track ENSO status, probability forecasts, Niño 3.4 history, and commodity impacts.` | `GET /api/indicators?module=weatherwatch&measure_family=weather`; selected ENSO indicators `GET /api/indicators/{id}/data` | `AppShell`, `ENSOModule`, `TimeSeriesChart`, `ProbabilityStackedBars`, `CommodityImpactMatrix` | Status and probability load first, history chart second | Matrix becomes stacked cards |
| `/weather/[region]` | `WeatherWatch | {region}` / `Regional weather context and commodity impact for {region}.` | `GET /api/weather/current?geography={region}`; `GET /api/indicators?module=weatherwatch&geography={region}` | `AppShell`, `RegionalView`, `EmbeddedMapProduct`, `TimeSeriesChart`, `ImpactNote` | Region summary loads first; maps and charts load after | Region summary stays fixed at top, charts become tabs |
| `/balance/[commodity]` | `Commodity balance | {commodity}` / `Compare supply, demand, inventory, and weather context on one balance view.` | `GET /api/commodities/{commodity}/balance`; optional `GET /api/indicators?commodity=...` for supporting context | `AppShell`, `BalanceHeader`, `DualAxisBalanceChart`, `InventoryModeToggle`, `BalanceNotes` | Paid-gated route; show paywall shell immediately if not entitled | Overlay chart becomes one column with preset toggles |

Notes:

- The `PriceWatch` page uses the shared indicator contract as a `pricewatch` module. This follows the backend document's shared observation model and phase-4 compatibility note.
- `HeadlineWatch`, `CalendarWatch`, and overview pages should stay public and indexable.
- Premium-only routes or premium-only panels should render a useful teaser state, not a blank lock screen.

## 6. Shared Component Hierarchy

### 6.1 Core shared types

```ts
export type ModuleCode =
  | "headlines"
  | "prices"
  | "calendar"
  | "inventory"
  | "supply"
  | "demand"
  | "weather";

export type FreshnessState = "live" | "current" | "lagged" | "structural" | "aged";

export interface SnapshotCardData {
  indicatorId: string;
  code: string;
  name: string;
  commodityCode: string;
  geographyCode: string;
  latestValue: number;
  unit: string;
  changeAbs: number | null;
  deviationAbs: number | null;
  signal: "tightening" | "loosening" | "expanding" | "contracting" | "neutral";
  sparkline?: number[];
  lastUpdatedAt: string;
  freshness: FreshnessState;
  quality?: "observed" | "estimate" | "proxy" | "disruption";
}

export interface SeriesPoint {
  date: string;
  value: number;
  unit: string;
  releaseDate?: string;
  observationKind?: "actual" | "estimate" | "forecast";
}

export interface SeasonalRangePoint {
  periodIndex: number;
  p10?: number;
  p25: number;
  p50: number;
  p75: number;
  p90?: number;
}

export interface ChartAnnotation {
  date: string;
  label: string;
  kind?: "event" | "release" | "policy" | "weather";
}
```

### 6.2 Shared layout components

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `AppShell` | `{ module: ModuleCode; title: string; children: ReactNode; filters?: ReactNode; rightRail?: ReactNode }` | No | Persistent top app bar, module nav, content region, optional right rail, footer |
| `TopNav` | `{ current: ModuleCode; entitlements: UserEntitlements | null }` | No | Sticky top bar, theme toggle, account menu, module links |
| `ModuleNav` | `{ current: ModuleCode; items: ModuleNavItem[] }` | No | Sticky secondary nav for module switching |
| `CommodityFilter` | `{ value?: string; options: CommodityOption[]; onChange: (value?: string) => void }` | No | Compact select or command-popover, writes to URL |
| `GeographyFilter` | `{ value?: string; options: GeographyOption[]; onChange: (...) => void }` | No | Same dense filter pattern |
| `DateRangeSelector` | `{ value: DateRangePreset; onChange: (...) => void }` | No | Preset chips plus custom range popover |
| `AccessGate` | `{ tier: "public" | "free" | "paid"; children: ReactNode; fallback: ReactNode }` | No | Freemium gate with teaser content |

### 6.3 Shared data and visual components

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `TimeSeriesChart` | `{ title: string; series: ChartSeries[]; rightAxisSeries?: string[]; annotations?: ChartAnnotation[]; height?: number }` | No | General Plotly wrapper for date x-axis charts, optional dual axis, drag zoom on desktop |
| `SeasonalRangeChart` | `{ title: string; seasonalRange: SeasonalRangePoint[]; currentYear: SeriesPoint[]; priorYear?: SeriesPoint[]; annotations?: ChartAnnotation[]; periodMode: "week" | "month"; excludeYears?: number[] }` | No | Signature inventory/supply/demand chart with percentile bands and seasonal overlays |
| `ChangeBarChart` | `{ series: SeriesPoint[]; seasonalAverage?: SeriesPoint[]; semanticMode: "inventory" | "generic" }` | No | Build/draw or change bars with optional overlay |
| `Sparkline` | `{ values: number[]; trend: "up" | "down" | "flat"; width?: number; height?: number }` | No | Inline SVG sparkline |
| `HeatMapGrid` | `{ rows: HeatMapRow[]; columns: HeatMapColumn[]; onCellClick?: (...) => void }` | No | Semantic table, not Plotly, keyboard accessible |
| `DataTable` | `{ columns: ColumnDef<T>[]; data: T[]; exportName?: string; compact?: boolean }` | No | TanStack Table wrapper with sorting, pagination, export |
| `IndicatorCard` | `{ card: SnapshotCardData; onClick?: () => void }` | No | Dense market snapshot card |
| `AlertBadge` | `{ kind: AlertKind; label?: string }` | No | Threshold and freshness badge system |
| `FreshnessBadge` | `{ state: FreshnessState; nextReleaseAt?: string }` | No | Compact freshness chip with optional countdown |
| `SourceAttribution` | `{ sourceLabel: string; timestamp: string; href?: string }` | No | Mono source line under charts and cards |
| `LoadingState` | `{ variant: "card" | "chart" | "table" | "feed"; rows?: number }` | No | Skeletons shaped like the final layout |
| `ErrorState` | `{ title?: string; message: string; onRetry?: () => void }` | No | Inline retry panel, never full-screen unless route-level failure |

## 7. Module-Specific Components

Route-only composites such as `ReleaseStrip`, `CommodityHeader`, `RevisionLedger`, `WeatherIndicatorTiles`, and `BalanceHeader` live inside each feature folder. The tables below cover the reusable module components.

### HeadlineWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `HeadlineFeed` | `{ filters: HeadlineFilters }` | Yes, `GET /api/headlines` via infinite query | Dense chronological feed with cursor pagination |
| `HeadlineItem` | `{ item: HeadlineItemData }` | No | Source, title, module-origin tag, timestamp, auto-trigger badge |
| `SourceFilter` | `{ value?: string; options: SourceOption[]; onChange: (...) => void }` | No | Compact dropdown with source counts |

### PriceWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `BenchmarkStrip` | `{ indicators: PriceBenchmarkSummary[] }` | No | Horizontal strip of latest benchmark cards |
| `PriceChart` | `{ primarySeries: ChartSeries; overlaySeries?: ChartSeries[]; range: DateRangePreset }` | No | Plotly line chart with optional module overlays |
| `OverlayToggle` | `{ options: OverlayOption[]; value: string[]; onChange: (...) => void }` | No | Multi-select overlay switcher |

### CalendarWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `CalendarGrid` | `{ range: CalendarRange; filters: CalendarFilters }` | Yes, `GET /api/calendar` | Month grid on desktop, agenda list on mobile |
| `ReleaseEvent` | `{ item: CalendarEventData }` | No | Event card with module chip, time, source, and deep link |
| `ModuleFilter` | `{ value?: string; options: ModuleOption[]; onChange: (...) => void }` | No | Dense module filter |

### InventoryWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `InventorySnapshot` | `{ cards: SnapshotCardData[]; onSelectIndicator: (...) => void }` | No | Grid of market snapshot cards |
| `InventoryDetail` | `{ indicator: IndicatorMeta; latest: LatestPoint; series: SeriesPoint[]; seasonalRange: SeasonalRangePoint[] }` | No | Header, seasonal chart, change chart, recent releases table |
| `SeasonalToggle` | `{ excludeYear2020: boolean; onChange: (value: boolean) => void }` | No | Toggle for anomalous years in seasonal overlays |

### SupplyWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `SupplySnapshot` | `{ cards: SnapshotCardData[] }` | No | Supply grid with quality badges |
| `ProductionChart` | `{ observed: ChartSeries; proxies?: ChartSeries[]; annotations?: ChartAnnotation[] }` | No | Primary observed-output chart with proxy overlay |
| `CapacityGauge` | `{ value: number; target?: number; label: string }` | No | Compact bullet-chart style capacity view |

### DemandWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `DemandScorecard` | `{ rows: DemandScorecardRow[]; onSelectCommodity: (...) => void }` | No | Heat-map scorecard, row click-through |
| `DemandMoverFeed` | `{ filters?: DemandMoverFilters }` | Yes, `GET /api/headlines?module_origin=demandwatch&auto_only=true` or module release feed config | Dense feed of demand-moving releases |
| `MacroStrip` | `{ items: MacroStripItem[] }` | No | Persistent macro banner with 4-5 key indicators |

### WeatherWatch

| Component | Props | Fetches its own data | Visual / behavior |
|---|---|---|---|
| `WeatherBriefing` | `{ summary: string; generatedAt: string; regionSignals: WeatherSignal[] }` | No | Editorial or template-generated briefing card |
| `RegionalView` | `{ region: RegionSummary; charts: ChartPanelConfig[] }` | No | Region summary, charts, maps, commodity impact note |
| `ENSOModule` | `{ status: ENSOStatus; probabilities: ENSOProbabilityRow[]; history: SeriesPoint[] }` | No | Phase card, probability bars, Niño 3.4 chart, impact matrix |
| `EmbeddedMapProduct` | `{ imageUrl: string; issuedAt: string; sourceLabel: string; alt: string; href?: string }` | No | Responsive map tile with attribution and issue time |

## 8. Data Fetching and Caching Strategy

### 8.1 API client

Implement one typed client in `src/lib/api/client.ts`:

- `getJson<T>(path: string, schema: ZodSchema<T>, init?: RequestInit)`
- same-origin calls to `/api/...`
- attach credentials for session-cookie auth
- normalize backend error envelopes into one frontend error shape

### 8.2 TanStack Query defaults

```ts
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      refetchOnReconnect: true,
      gcTime: 30 * 60 * 1000,
    },
  },
});
```

### 8.3 Stale windows by endpoint

| Endpoint | Stale time | Notes |
|---|---|---|
| `/api/headlines` | 60s | Fast-moving public feed |
| `/api/calendar` | 15m | Release schedule changes slowly |
| `/api/indicators` | 60m | Catalog metadata is stable |
| `/api/indicators/{id}/latest` | 5m | Good match for release cadence |
| `/api/indicators/{id}/data` | 5m | Most core series are weekly or monthly |
| `/api/snapshot/{module}` | 5m | Backend already precomputes and expires snapshots |
| `/api/weather/current` | 15m | Weather summary is more time-sensitive |
| `/api/commodities/{commodity}/balance` | 5m | Paid detail view |
| `/api/auth/me` | 5m | Entitlements should not thrash |

### 8.4 Filter state

Primary rules:

- Route segments identify the module and main subject:
  - `/inventory/crude_oil/EIA_CRUDE_US_COMMERCIAL_STOCKS`
- Search params hold secondary state:
  - `?geography=US&range=5y&frequency=weekly&view=seasonal&overlay=price`
- URL is the source of truth for:
  - commodity
  - geography
  - date range
  - frequency
  - selected overlay
  - view tab
- `Zustand` stores only ephemeral UI state:
  - mobile drawer open
  - chart crosshair sync
  - table density
  - theme if not persisted elsewhere

### 8.5 Loading strategy

- Never block an entire page on all queries finishing.
- Use progressive rendering:
  - shell
  - hero/header
  - primary chart or primary grid
  - secondary panels
  - tables and right rail
- Use skeletons, not centered spinners.
- Use `keepPreviousData` for filter changes so cards and charts do not flash empty.

### 8.6 Error handling

- Route-level failure:
  - show module shell and one `ErrorState` panel inside content
- Panel-level failure:
  - keep the rest of the page alive
  - show retry action inline
- If stale cached data exists:
  - render stale data
  - show `Aged` badge
  - display: `Latest cached data shown. Refresh failed.`

### 8.7 Prefetching

- On hover or visible card intersection:
  - prefetch indicator detail query for the top 3-5 visible cards on inventory and supply grids
- On calendar event hover:
  - prefetch linked module page if route params are known
- When entering `/inventory/[commodity]`:
  - prefetch first indicator detail for the default selected indicator

## 9. Chart Rendering Specification

Only shared chart wrappers may define Plotly traces. Module components pass typed data into those wrappers.

### 9.1 Seasonal range chart

This is the signature chart of the platform.

#### Data inputs

- `seasonalRange[]` from `GET /api/indicators/{id}/data`
- `series[]` from the same payload
- optional annotations from release metadata or static event config

#### Visual rules

- Height:
  - desktop `420px`
  - tablet `340px`
  - mobile `280px`
- X-axis:
  - `week 1-52` for weekly data
  - `Jan-Dec` for monthly data
- Y-axis:
  - native unit only
- Outer band `10th-90th percentile`:
  - fill `var(--chart-band-10-90)`
  - no border line
- Inner band `25th-75th percentile`:
  - fill `var(--chart-band-25-75)`
  - no border line
- Median line:
  - color `var(--chart-median)`
  - width `1.25`
  - dash `4,4`
- Current year line:
  - color `var(--chart-current-year)`
  - width `3`
  - solid
- Prior year line:
  - color `var(--chart-prior-year)`
  - width `1.5`
  - dash `6,4`
- Grid:
  - horizontal only
  - color `var(--chart-grid)`
- Annotations:
  - vertical marker color `var(--chart-annotation)`
  - width `1`
  - dash `3,3`

#### Plotly implementation rule

Render traces in this order:

1. `p90` invisible line
2. `p10` with `fill: "tonexty"` and outer-band fill color
3. `p75` invisible line
4. `p25` with `fill: "tonexty"` and inner-band fill color
5. `median`
6. `prior year`
7. `current year`
8. annotation shapes

This ordering guarantees the current year line stays visually dominant.

#### Tooltip

Tooltip contents:

- Period label: `Week 12` or `Mar`
- Current value
- Prior year value, if present
- Median
- `vs median`
- Percentile bracket:
  - `Below 10th`
  - `10th-25th`
  - `25th-75th`
  - `75th-90th`
  - `Above 90th`
- Release date

Tooltip style:

- background `var(--chart-tooltip-bg)`
- border `1px solid var(--chart-tooltip-border)`
- text `var(--chart-tooltip-text)`
- mono values, sans labels

### 9.2 General time-series chart

- Use Plotly scatter traces with `mode: "lines"`
- X-axis uses actual date values, never index positions
- Allow up to two y-axes:
  - left axis for primary indicator
  - right axis for price overlay or secondary series
- Use drag zoom on desktop
- Disable scroll-wheel zoom by default
- Provide range presets above the chart:
  - `3M`, `6M`, `1Y`, `3Y`, `5Y`, `All`
- On mobile:
  - no drag zoom
  - presets only

Default series styling:

- primary series width `2.5`
- secondary series width `1.75`
- comparative YoY line dash `6,4`
- forecast series opacity `0.8`

### 9.3 Change bar chart

- Bar semantic mode is module-aware:
  - inventory energy convention: builds red, draws green
  - generic mode: positive green, negative red
- Bar gap `0.18`
- Optional seasonal-average overlay:
  - thin dashed neutral line
- Zero line:
  - solid `var(--chart-axis)`
- Default height `220px`

### 9.4 Demand heat map grid

Use semantic HTML table, not Plotly.

- Rows: commodity verticals
- Columns: key demand indicators
- Cell state colors:
  - above trend: `var(--color-positive-soft)` background, `var(--color-positive)` text
  - at trend: `var(--color-neutral-soft)` background, `var(--color-text-primary)` text
  - below trend: `var(--color-negative-soft)` background, `var(--color-negative)` text
  - stale or no data: `var(--color-bg-muted)` background, `var(--color-text-soft)` text
- Cell height `44px`
- Each cell opens the commodity drill-down

### 9.5 Sparkline

- Input should be pre-aggregated on the backend to 26-52 points
- Render as inline SVG
- No tooltip on desktop grid view
- Optional value tooltip on mobile tap

## 10. Responsive Design Strategy

### Breakpoints

```text
sm  = 640px
md  = 768px
lg  = 1024px
xl  = 1280px
2xl = 1536px
```

### Navigation

- Desktop `lg+`:
  - sticky top bar
  - sticky secondary module nav below it
  - no permanent left sidebar
- Tablet `md-lg`:
  - same top structure
  - filters collapse into one row with overflow scroll
- Mobile `<md`:
  - top bar with menu drawer
  - filters in bottom sheet or full-width drawer

### Layout behavior

- Snapshot grids:
  - desktop: `3-4` columns depending on width
  - tablet: `2` columns
  - mobile: `1` column
- Detail pages:
  - desktop: main chart left, right rail for source/release/calendar context
  - tablet: right rail drops below main chart
  - mobile: chart, then tabs, then supporting data
- Tables:
  - desktop: full dense table
  - mobile: convert to stacked row cards or horizontal-scroll table for numeric comparison views

### Chart behavior

- Full-size charts resize with container width
- Do not introduce horizontal page scrolling for charts
- On mobile:
  - keep chart height lower
  - use range presets and tabs instead of heavy interaction
  - hide secondary legend text if space is too tight

## 11. Performance Plan

### Charting

- Lazy-load Plotly charts with `next/dynamic` and `ssr: false`
- Keep Plotly usage inside `TimeSeriesChart`, `SeasonalRangeChart`, `ChangeBarChart`, and `PriceChart`
- Build a reduced Plotly bundle containing only the traces CommodityWatch needs: scatter, bar, heatmap, and annotations
- Use inline SVG sparklines so snapshot grids do not instantiate dozens of Plotly charts

### Data shape

- Pre-aggregate sparkline data in the backend to `26-52` points
- Use the backend's precomputed `/api/snapshot/{module}` responses for landing pages
- Use the backend's `seasonal_range` payload directly; do not compute percentile bands in the browser

### Media and weather maps

- NOAA/CPC and similar raster map products load below the fold with native lazy loading
- Reserve aspect-ratio boxes before image load to prevent layout shift
- Only render interactive weather maps in V2+, not MVP

### Bundle discipline

- Do not import large libraries into route roots
- Keep chart code in feature-level client components
- Use server components for page shells, metadata, and non-interactive sections

## 12. SEO and Metadata

### Rendering policy

- `Static with revalidation`:
  - `/`
  - `/headlines`
  - `/calendar`
  - `/inventory`
  - `/supply`
  - `/demand`
  - `/weather`
- `Dynamic server-rendered with revalidation`:
  - `/inventory/[commodity]`
  - `/inventory/[commodity]/[indicatorId]`
  - `/supply/[commodity]`
  - `/demand/[commodity]`
  - `/weather/[region]`
  - `/weather/enso`
  - `/prices`

Recommended revalidate windows:

- overview and snapshot pages: `300s`
- calendar: `900s`
- headlines: `60s`

### Metadata rules

- Use Next Metadata API in each route
- Generate canonical URLs
- Generate Open Graph and Twitter cards
- Use an OG image route with module name, commodity name, and latest signal

### Structured data

- `/`:
  - `WebSite`
- module landing pages:
  - `CollectionPage`
  - `BreadcrumbList`
- indicator detail pages:
  - `Dataset`
  - `BreadcrumbList`

### Indexing rules

- Public pages: index
- Paid-only balance page: `noindex`
- Thin gated pages: index only if the public teaser has meaningful content

### Sitemap

Implement `src/app/sitemap.ts`:

- static entries for module routes
- commodity routes from local taxonomy config
- public indicator detail pages from `/api/indicators?visibility=public`

## 13. Recommended Project Structure

Because the backend already lives at the repo root, keep the frontend isolated in one folder:

```text
frontend/
├── package.json
├── next.config.ts
├── tsconfig.json
├── public/
├── src/
│   ├── app/
│   │   ├── (marketing)/
│   │   │   └── page.tsx
│   │   ├── (platform)/
│   │   │   ├── headlines/page.tsx
│   │   │   ├── prices/page.tsx
│   │   │   ├── calendar/page.tsx
│   │   │   ├── inventory/page.tsx
│   │   │   ├── inventory/[commodity]/page.tsx
│   │   │   ├── inventory/[commodity]/[indicatorId]/page.tsx
│   │   │   ├── supply/page.tsx
│   │   │   ├── supply/[commodity]/page.tsx
│   │   │   ├── demand/page.tsx
│   │   │   ├── demand/[commodity]/page.tsx
│   │   │   ├── weather/page.tsx
│   │   │   ├── weather/enso/page.tsx
│   │   │   ├── weather/[region]/page.tsx
│   │   │   └── balance/[commodity]/page.tsx
│   │   ├── sitemap.ts
│   │   ├── layout.tsx
│   │   └── globals.css
│   ├── components/
│   │   ├── layout/
│   │   ├── shared/
│   │   └── ui/
│   ├── features/
│   │   ├── headlines/
│   │   ├── prices/
│   │   ├── calendar/
│   │   ├── inventory/
│   │   ├── supply/
│   │   ├── demand/
│   │   └── weather/
│   ├── hooks/
│   ├── lib/
│   │   ├── api/
│   │   ├── charts/
│   │   ├── format/
│   │   ├── query/
│   │   └── utils/
│   ├── stores/
│   ├── styles/
│   │   ├── tokens.css
│   │   └── utilities.css
│   ├── types/
│   │   ├── api.ts
│   │   ├── chart.ts
│   │   └── filters.ts
│   └── config/
│       ├── commodities.ts
│       ├── modules.ts
│       └── navigation.ts
└── tests/
```

### Structure rules

- `features/` owns module-specific containers, queries, and feature components
- `components/shared/` owns reusable data UI
- `components/ui/` stores shadcn primitives
- `lib/api/` stores the client, schemas, and endpoint wrappers
- `types/` only stores shared TS types, not business logic
- `config/` stores stable frontend config such as commodity taxonomy, module labels, and region mappings

## 14. MVP Build Plan

### Phase 1: InventoryWatch MVP

Build:

- `AppShell`, `TopNav`, `ModuleNav`
- theme tokens, light/dark mode, Tailwind setup
- typed API client + TanStack Query setup
- `IndicatorCard`, `AlertBadge`, `FreshnessBadge`, `SourceAttribution`
- `InventorySnapshot`
- `SeasonalRangeChart`
- `ChangeBarChart`
- `DataTable`
- routes:
  - `/`
  - `/inventory`
  - `/inventory/[commodity]`
  - `/inventory/[commodity]/[indicatorId]`

### Phase 2: DemandWatch + HeadlineWatch

Build:

- `HeadlineFeed`, `HeadlineItem`, `SourceFilter`
- `DemandScorecard`
- `DemandMoverFeed`
- `MacroStrip`
- routes:
  - `/headlines`
  - `/demand`
  - `/demand/[commodity]`
- cross-module nav and headline deep links

### Phase 3: SupplyWatch + CalendarWatch

Build:

- `CalendarGrid`, `ReleaseEvent`, `ModuleFilter`
- `SupplySnapshot`
- `ProductionChart`
- `CapacityGauge`
- `RevisionLedger`
- routes:
  - `/calendar`
  - `/supply`
  - `/supply/[commodity]`
- calendar-to-module deep links

### Phase 4: WeatherWatch + PriceWatch + Integration

Build:

- `WeatherBriefing`
- `EmbeddedMapProduct`
- `RegionalView`
- `ENSOModule`
- `BenchmarkStrip`
- `PriceChart`
- `BalanceHeader`
- `DualAxisBalanceChart`
- routes:
  - `/weather`
  - `/weather/enso`
  - `/weather/[region]`
  - `/prices`
  - `/balance/[commodity]`
- cross-module overlays:
  - inventory on price
  - demand on price
  - weather context on demand and supply

## 15. Recommended First Milestone

The first coded milestone should be:

1. Create `frontend/` with Next.js, TypeScript, Tailwind, shadcn/ui, TanStack Query, and the token system.
2. Ship `/inventory` and one fully working indicator detail page.
3. Make `SeasonalRangeChart` production-grade before building other modules.
4. Only after the inventory chart, snapshot grid, and detail table feel correct should the rest of the platform be added.

That keeps the hardest frontend problem on the table early and gives every later module a stable foundation.
