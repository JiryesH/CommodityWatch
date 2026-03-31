import {
  convertUnitValue,
  freshnessFor,
  getIndicatorRegistryEntry,
  semanticModeForCommodity,
} from "./catalog.js";

function buildQueryString(params) {
  const search = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }

    search.set(key, String(value));
  });

  const serialized = search.toString();
  return serialized ? `?${serialized}` : "";
}

function compactResponseBody(bodyText) {
  const normalized = String(bodyText || "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }

  if (normalized.length <= 180) {
    return normalized;
  }

  return `${normalized.slice(0, 177)}...`;
}

async function fetchJson(url) {
  const response = await fetch(url, {
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
    },
  });

  const bodyText = await response.text();
  let payload = null;

  if (bodyText) {
    try {
      payload = JSON.parse(bodyText);
    } catch {
      payload = null;
    }
  }

  if (!response.ok) {
    const detail =
      (payload && (payload.detail || payload.error || payload.message)) || compactResponseBody(bodyText) || "Request failed.";
    throw new Error(typeof detail === "string" ? detail : "Request failed.");
  }

  if (!payload || typeof payload !== "object") {
    throw new Error("InventoryWatch API returned an invalid payload.");
  }

  return payload;
}

function mapIndicatorLatestResponse(payload) {
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

function mapIndicatorDataResponse(payload) {
  const registry = getIndicatorRegistryEntry(payload.indicator.code, payload.indicator.commodity_code);

  return {
    indicator: {
      id: payload.indicator.id,
      code: payload.indicator.code,
      name: payload.indicator.name,
      description: payload.indicator.description || registry.description,
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
      p10: convertUnitValue(point.p10, payload.indicator.unit),
      p25: convertUnitValue(point.p25, payload.indicator.unit),
      p50: convertUnitValue(point.p50, payload.indicator.unit),
      p75: convertUnitValue(point.p75, payload.indicator.unit),
      p90: convertUnitValue(point.p90, payload.indicator.unit),
      mean: convertUnitValue(point.mean, payload.indicator.unit),
      stddev: convertUnitValue(point.stddev, payload.indicator.unit),
    })),
    metadata: {
      latestReleaseId: payload.metadata.latest_release_id || null,
      latestReleaseAt: payload.metadata.latest_release_at || null,
      sourceUrl: payload.metadata.source_url || registry.sourceHref || null,
      sourceLabel: payload.metadata.source_label || registry.sourceLabel,
    },
  };
}

function mapSnapshotResponse(payload) {
  return {
    module: payload.module,
    generatedAt: payload.generated_at,
    expiresAt: payload.expires_at,
    cards: payload.cards.map((card) => {
      const registry = getIndicatorRegistryEntry(card.code, card.commodity_code);
      return {
        indicatorId: card.indicator_id,
        code: card.code,
        name: card.name,
        commodityCode: card.commodity_code,
        geographyCode: card.geography_code,
        latestValue: convertUnitValue(card.latest_value, card.unit) ?? card.latest_value,
        unit: card.unit === "kb" ? "mb" : card.unit,
        frequency: card.frequency || null,
        changeAbs: convertUnitValue(card.change_abs, card.unit),
        deviationAbs: convertUnitValue(card.deviation_abs, card.unit),
        signal: card.signal,
        sparkline: Array.isArray(card.sparkline)
          ? card.sparkline.map((value) => convertUnitValue(value, card.unit) ?? value)
          : [],
        lastUpdatedAt: card.last_updated_at,
        freshness: freshnessFor(undefined, card.last_updated_at, Boolean(card.stale)),
        stale: Boolean(card.stale),
        sourceLabel: card.source_label || registry.sourceLabel,
        sourceHref: card.source_url || registry.sourceHref || null,
        description: card.description || registry.description,
        semanticMode: semanticModeForCommodity(card.commodity_code),
        snapshotGroup: registry.snapshotGroup,
        seasonalLow: convertUnitValue(card.seasonal_low, card.unit),
        seasonalHigh: convertUnitValue(card.seasonal_high, card.unit),
        seasonalMedian: convertUnitValue(card.seasonal_median, card.unit),
        seasonalP10: convertUnitValue(card.seasonal_p10, card.unit),
        seasonalP25: convertUnitValue(card.seasonal_p25, card.unit),
        seasonalP75: convertUnitValue(card.seasonal_p75, card.unit),
        seasonalP90: convertUnitValue(card.seasonal_p90, card.unit),
        seasonalSamples:
          typeof card.seasonal_samples === "number" && Number.isFinite(card.seasonal_samples)
            ? card.seasonal_samples
            : 0,
      };
    }),
  };
}

export async function fetchInventorySnapshot(params = {}) {
  const payload = await fetchJson(
    `/api/snapshot/inventorywatch${buildQueryString({
      commodity: params.commodity,
      geography: params.geography,
      limit: params.limit,
      include_sparklines: params.includeSparklines ?? true,
    })}`
  );

  return mapSnapshotResponse(payload);
}

export async function fetchIndicatorData(indicatorId, params = {}) {
  const payload = await fetchJson(
    `/api/indicators/${encodeURIComponent(indicatorId)}/data${buildQueryString({
      start_date: params.startDate,
      end_date: params.endDate,
      downsample: params.downsample,
      vintage: params.vintage,
      as_of: params.asOf,
      include_seasonal: params.includeSeasonal ?? true,
      seasonal_profile: params.seasonalProfile,
      limit_points: params.limitPoints,
    })}`
  );

  return mapIndicatorDataResponse(payload);
}

export async function fetchIndicatorLatest(indicatorId) {
  const payload = await fetchJson(`/api/indicators/${encodeURIComponent(indicatorId)}/latest`);
  return mapIndicatorLatestResponse(payload);
}
