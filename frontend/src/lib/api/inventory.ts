import type {
  IndicatorDataResponse,
  IndicatorLatestResponse,
  IndicatorListResponse,
  SnapshotResponse,
} from "@/types/api";
import type { CommodityGroupSlug } from "@/config/commodities";
import { getJson, getOptionalJson } from "@/lib/api/client";
import {
  indicatorDataResponseSchema,
  indicatorLatestResponseSchema,
  indicatorListResponseSchema,
  snapshotResponseSchema,
  type RawIndicatorDataResponse,
  type RawIndicatorLatestResponse,
  type RawIndicatorListResponse,
  type RawSnapshotResponse,
} from "@/lib/api/schemas";
import { convertUnitValue } from "@/lib/format/numbers";
import { commodityGroupForCode, freshnessFor, getIndicatorRegistryEntry, semanticModeForCommodity } from "@/features/inventory/indicator-registry";

export interface IndicatorFilters {
  module?: string;
  commodity?: string;
  geography?: string;
  frequency?: string;
  measureFamily?: string;
  visibility?: string;
  active?: boolean;
  limit?: number;
  cursor?: string | null;
}

export interface InventorySnapshotParams {
  commodity?: string;
  geography?: string;
  limit?: number;
  includeSparklines?: boolean;
}

export interface IndicatorDataParams {
  startDate?: string;
  endDate?: string;
  downsample?: "auto" | "raw" | "daily" | "weekly" | "monthly" | "quarterly";
  vintage?: "latest" | "first" | "as_of";
  asOf?: string;
  includeSeasonal?: boolean;
  seasonalProfile?: string;
  limitPoints?: number;
}

function queryString(params: Record<string, string | number | boolean | null | undefined>) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }

    search.set(key, String(value));
  });
  const result = search.toString();
  return result ? `?${result}` : "";
}

function mapIndicatorListResponse(payload: RawIndicatorListResponse): IndicatorListResponse {
  return {
    items: payload.items.map((item) => ({
      id: item.id,
      code: item.code,
      name: item.name,
      description: item.description ?? getIndicatorRegistryEntry(item.code).description,
      modules: item.modules,
      commodityCode: item.commodity_code,
      geographyCode: item.geography_code,
      measureFamily: item.measure_family,
      frequency: item.frequency,
      nativeUnit: item.native_unit,
      canonicalUnit: item.canonical_unit,
      isSeasonal: item.is_seasonal,
      isDerived: item.is_derived,
      visibilityTier: item.visibility_tier,
      latestReleaseAt: item.latest_release_at,
    })),
    nextCursor: payload.next_cursor,
  };
}

function mapIndicatorLatestResponse(payload: RawIndicatorLatestResponse): IndicatorLatestResponse {
  return {
    indicator: {
      id: payload.indicator.id,
      code: payload.indicator.code,
    },
    latest: {
      periodEndAt: payload.latest.period_end_at,
      releaseDate: payload.latest.release_date,
      value: convertUnitValue(payload.latest.value, payload.latest.unit) ?? payload.latest.value,
      unit: payload.latest.unit === "kb" ? "mb" : payload.latest.unit,
      changeFromPriorAbs: convertUnitValue(payload.latest.change_from_prior_abs, payload.latest.unit),
      changeFromPriorPct: payload.latest.change_from_prior_pct,
      deviationFromSeasonalAbs: convertUnitValue(payload.latest.deviation_from_seasonal_abs, payload.latest.unit),
      deviationFromSeasonalZscore: payload.latest.deviation_from_seasonal_zscore,
      revisionSequence: payload.latest.revision_sequence,
    },
  };
}

function mapIndicatorDataResponse(payload: RawIndicatorDataResponse): IndicatorDataResponse {
  const registry = getIndicatorRegistryEntry(payload.indicator.code);

  return {
    indicator: {
      id: payload.indicator.id,
      code: payload.indicator.code,
      name: payload.indicator.name,
      description: payload.indicator.description ?? registry.description,
      modules: payload.indicator.modules,
      commodityCode: payload.indicator.commodity_code,
      geographyCode: payload.indicator.geography_code,
      frequency: payload.indicator.frequency,
      measureFamily: payload.indicator.measure_family,
      unit: payload.indicator.unit === "kb" ? "mb" : payload.indicator.unit,
    },
    series: payload.series.map((point) => ({
      periodStartAt: point.period_start_at,
      periodEndAt: point.period_end_at,
      releaseDate: point.release_date,
      vintageAt: point.vintage_at,
      value: convertUnitValue(point.value, point.unit) ?? point.value,
      unit: point.unit === "kb" ? "mb" : point.unit,
      observationKind: point.observation_kind,
      revisionSequence: point.revision_sequence,
    })),
    seasonalRange: payload.seasonal_range.map((point) => ({
      periodIndex: point.period_index,
      p10: convertUnitValue(point.p10 ?? null, payload.indicator.unit),
      p25: convertUnitValue(point.p25 ?? null, payload.indicator.unit),
      p50: convertUnitValue(point.p50 ?? null, payload.indicator.unit),
      p75: convertUnitValue(point.p75 ?? null, payload.indicator.unit),
      p90: convertUnitValue(point.p90 ?? null, payload.indicator.unit),
      mean: convertUnitValue(point.mean ?? null, payload.indicator.unit),
      stddev: convertUnitValue(point.stddev ?? null, payload.indicator.unit),
    })),
    metadata: {
      latestReleaseId: payload.metadata.latest_release_id ?? null,
      latestReleaseAt: payload.metadata.latest_release_at ?? null,
      sourceUrl: payload.metadata.source_url ?? registry.sourceHref ?? null,
      sourceLabel: payload.metadata.source_label ?? registry.sourceLabel,
    },
  };
}

function mapSnapshotResponse(payload: RawSnapshotResponse): SnapshotResponse {
  return {
    module: payload.module,
    generatedAt: payload.generated_at,
    expiresAt: payload.expires_at,
    cards: payload.cards.map((card) => {
      const registry = getIndicatorRegistryEntry(card.code);
      const unit = card.unit === "kb" ? "mb" : card.unit;
      return {
        indicatorId: card.indicator_id,
        code: card.code,
        name: card.name,
        commodityCode: card.commodity_code,
        geographyCode: card.geography_code,
        latestValue: convertUnitValue(card.latest_value, card.unit) ?? card.latest_value,
        unit,
        changeAbs: convertUnitValue(card.change_abs, card.unit),
        deviationAbs: convertUnitValue(card.deviation_abs, card.unit),
        signal: card.signal,
        sparkline: card.sparkline.map((value) => convertUnitValue(value, card.unit) ?? value),
        lastUpdatedAt: card.last_updated_at,
        freshness: freshnessFor(undefined, card.last_updated_at, card.stale),
        stale: card.stale,
        sourceLabel: card.source_label ?? registry.sourceLabel,
        sourceHref: card.source_url ?? registry.sourceHref,
        description: card.description ?? registry.description,
        semanticMode: semanticModeForCommodity(card.commodity_code),
        snapshotGroup: registry.snapshotGroup,
      };
    }),
  };
}

export async function fetchInventorySnapshot(params: InventorySnapshotParams = {}) {
  const payload = await getJson(
    `/api/snapshot/inventorywatch${queryString({
      commodity: params.commodity,
      geography: params.geography,
      limit: params.limit,
      include_sparklines: params.includeSparklines ?? true,
    })}`,
    snapshotResponseSchema,
  ) as RawSnapshotResponse;

  return mapSnapshotResponse(payload);
}

export async function fetchIndicatorData(indicatorId: string, params: IndicatorDataParams = {}) {
  const payload = await getJson(
    `/api/indicators/${indicatorId}/data${queryString({
      start_date: params.startDate,
      end_date: params.endDate,
      downsample: params.downsample,
      vintage: params.vintage,
      as_of: params.asOf,
      include_seasonal: params.includeSeasonal ?? true,
      seasonal_profile: params.seasonalProfile,
      limit_points: params.limitPoints,
    })}`,
    indicatorDataResponseSchema,
  ) as RawIndicatorDataResponse;

  return mapIndicatorDataResponse(payload);
}

export async function fetchIndicatorLatest(indicatorId: string) {
  const payload = (await getOptionalJson(
    `/api/indicators/${indicatorId}/latest`,
    indicatorLatestResponseSchema,
  )) as RawIndicatorLatestResponse | null;
  return payload ? mapIndicatorLatestResponse(payload) : null;
}

export async function fetchIndicators(filters: IndicatorFilters = {}) {
  const payload = await getJson(
    `/api/indicators${queryString({
      module: filters.module,
      commodity: filters.commodity,
      geography: filters.geography,
      frequency: filters.frequency,
      measure_family: filters.measureFamily,
      visibility: filters.visibility ?? "public",
      active: filters.active ?? true,
      limit: filters.limit ?? 200,
      cursor: filters.cursor,
    })}`,
    indicatorListResponseSchema,
  ) as RawIndicatorListResponse;

  return mapIndicatorListResponse(payload);
}

export function filterCardsByCommodityGroup(cards: SnapshotResponse["cards"], slug: CommodityGroupSlug) {
  if (slug === "all") {
    return cards;
  }

  return cards.filter((card) => commodityGroupForCode(card.commodityCode) === slug);
}
