export type ModuleCode =
  | "headlines"
  | "prices"
  | "calendar"
  | "inventory"
  | "supply"
  | "demand"
  | "weather";

export type FreshnessState = "live" | "current" | "lagged" | "structural" | "aged";

export type InventorySignal = "tightening" | "loosening" | "expanding" | "contracting" | "neutral";

export type AlertKind =
  | "extreme-low"
  | "extreme-high"
  | "fresh"
  | "aged"
  | "awaiting-release"
  | "estimate"
  | "proxy"
  | "observed"
  | "disruption";

export interface ModuleNavItem {
  code: ModuleCode;
  label: string;
  href: string;
  enabled: boolean;
  description: string;
}

export interface IndicatorListItem {
  id: string;
  code: string;
  name: string;
  description?: string | null;
  modules: string[];
  commodityCode: string | null;
  geographyCode: string | null;
  measureFamily: string;
  frequency: string;
  nativeUnit: string | null;
  canonicalUnit: string | null;
  isSeasonal: boolean;
  isDerived: boolean;
  visibilityTier: string;
  latestReleaseAt: string | null;
}

export interface IndicatorListResponse {
  items: IndicatorListItem[];
  nextCursor: string | null;
}

export interface IndicatorLatest {
  periodEndAt: string;
  releaseDate: string | null;
  value: number;
  unit: string;
  changeFromPriorAbs: number | null;
  changeFromPriorPct: number | null;
  deviationFromSeasonalAbs: number | null;
  deviationFromSeasonalZscore: number | null;
  revisionSequence: number;
}

export interface IndicatorLatestResponse {
  indicator: {
    id: string;
    code: string;
  };
  latest: IndicatorLatest;
}

export interface SeriesPoint {
  periodStartAt: string;
  periodEndAt: string;
  releaseDate: string | null;
  vintageAt: string;
  value: number;
  unit: string;
  observationKind: string;
  revisionSequence: number;
}

export interface SeasonalRangePoint {
  periodIndex: number;
  p10: number | null;
  p25: number | null;
  p50: number | null;
  p75: number | null;
  p90: number | null;
  mean: number | null;
  stddev: number | null;
}

export interface IndicatorDataResponse {
  indicator: {
    id: string;
    code: string;
    name: string;
    description?: string | null;
    modules: string[];
    commodityCode: string | null;
    geographyCode: string | null;
    frequency: string;
    measureFamily: string;
    unit: string | null;
    periodType?: string | null;
    marketingYearStartMonth?: number | null;
    isSeasonal?: boolean;
  };
  series: SeriesPoint[];
  seasonalRange: SeasonalRangePoint[];
  metadata: {
    latestReleaseId: string | null;
    latestReleaseAt: string | null;
    sourceUrl: string | null;
    sourceLabel?: string | null;
  };
}

export interface SnapshotCardData {
  indicatorId: string;
  code: string;
  name: string;
  commodityCode: string | null;
  geographyCode: string | null;
  frequency?: string | null;
  periodType?: string | null;
  marketingYearStartMonth?: number | null;
  isSeasonal?: boolean;
  latestValue: number;
  unit: string;
  changeAbs: number | null;
  deviationAbs: number | null;
  signal: InventorySignal;
  sparkline: number[];
  lastUpdatedAt: string;
  freshness: FreshnessState;
  stale: boolean;
  sourceLabel: string;
  sourceHref?: string;
  description?: string | null;
  semanticMode: "inventory" | "generic";
  snapshotGroup: string;
}

export interface SnapshotResponse {
  module: string;
  generatedAt: string;
  expiresAt: string;
  cards: SnapshotCardData[];
}
