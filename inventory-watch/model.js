import { commodityGroupForCode, getCommodityGroup } from "./catalog.js";

export const SNAPSHOT_SECTION_ORDER = [
  "Crude Oil",
  "Refined Products",
  "Natural Gas",
  "Base Metals",
  "Grains",
  "Softs",
  "Precious Metals",
  "Coal",
  "Fertilisers",
  "Inventory",
];

const SNAPSHOT_CARD_ORDER = {
  EIA_CRUDE_US_COMMERCIAL_STOCKS_EX_SPR: 10,
  EIA_CRUDE_US_CUSHING_STOCKS: 20,
  EIA_CRUDE_US_SPR_STOCKS: 30,
  EIA_GASOLINE_US_TOTAL_STOCKS: 10,
  EIA_DISTILLATE_US_TOTAL_STOCKS: 20,
  EIA_JET_FUEL_US_TOTAL_STOCKS: 30,
  EIA_PROPANE_US_TOTAL_STOCKS: 40,
  EIA_NATURAL_GAS_US_WORKING_STORAGE: 10,
  GIE_NATURAL_GAS_EU_TOTAL_STORAGE: 20,
  GIE_NATURAL_GAS_EU_DE_STORAGE: 30,
  GIE_NATURAL_GAS_EU_IT_STORAGE: 40,
  GIE_NATURAL_GAS_EU_FR_STORAGE: 50,
  GIE_NATURAL_GAS_EU_NL_STORAGE: 60,
  GIE_NATURAL_GAS_EU_AT_STORAGE: 70,
  LME_COPPER_WAREHOUSE_STOCKS: 10,
  LME_ALUMINIUM_WAREHOUSE_STOCKS: 20,
  LME_ZINC_WAREHOUSE_STOCKS: 30,
  LME_NICKEL_WAREHOUSE_STOCKS: 40,
  LME_TIN_WAREHOUSE_STOCKS: 50,
  LME_LEAD_WAREHOUSE_STOCKS: 60,
  USDA_US_CORN_ENDING_STOCKS: 10,
  USDA_US_SOYBEAN_ENDING_STOCKS: 20,
  USDA_US_WHEAT_ENDING_STOCKS: 30,
  USDA_US_RICE_ENDING_STOCKS: 40,
  USDA_WORLD_CORN_ENDING_STOCKS: 50,
  USDA_WORLD_SOYBEAN_ENDING_STOCKS: 60,
  USDA_WORLD_WHEAT_ENDING_STOCKS: 70,
  COMEX_GOLD_WAREHOUSE_STOCKS: 10,
  COMEX_SILVER_WAREHOUSE_STOCKS: 20,
  ETF_GLD_HOLDINGS: 30,
  ETF_IAU_HOLDINGS: 40,
  ETF_SLV_HOLDINGS: 50,
  ICE_ARABICA_COFFEE_CERTIFIED_STOCKS: 10,
  ICE_ROBUSTA_COFFEE_CERTIFIED_STOCKS: 20,
  ICE_RAW_SUGAR_CERTIFIED_STOCKS: 30,
  ICE_COTTON_CERTIFIED_STOCKS: 40,
  ICE_COCOA_CERTIFIED_STOCKS: 50,
};

export function groupCardsForSnapshot(cards) {
  return cards.reduce((groups, card) => {
    const key = card.snapshotGroup || "Inventory";
    if (!groups[key]) {
      groups[key] = [];
    }
    groups[key].push(card);
    return groups;
  }, {});
}

export function sortSnapshotCards(cards) {
  return [...cards].sort((left, right) => {
    const leftRank = SNAPSHOT_CARD_ORDER[left.code] ?? 999;
    const rightRank = SNAPSHOT_CARD_ORDER[right.code] ?? 999;
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    return String(left.name || "").localeCompare(String(right.name || ""));
  });
}

export function snapshotSectionEntries(groupedCards) {
  const ordered = SNAPSHOT_SECTION_ORDER.filter((sectionName) => groupedCards[sectionName]?.length).map((sectionName) => [
    sectionName,
    sortSnapshotCards(groupedCards[sectionName]),
  ]);
  const overflow = Object.keys(groupedCards)
    .filter((sectionName) => groupedCards[sectionName]?.length && !SNAPSHOT_SECTION_ORDER.includes(sectionName))
    .sort((left, right) => left.localeCompare(right))
    .map((sectionName) => [sectionName, sortSnapshotCards(groupedCards[sectionName])]);
  return [...ordered, ...overflow];
}

export function filterCardsByCommodityGroup(cards, slug) {
  if (slug === "all") {
    return cards;
  }
  return cards.filter((card) => commodityGroupForCode(card.commodityCode) === slug);
}

function weekOfYear(date) {
  const utcDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNumber = utcDate.getUTCDay() || 7;
  utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNumber);
  const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
  return Math.min(Math.ceil(((utcDate.getTime() - yearStart.getTime()) / 86400000 + 1) / 7), 52);
}

function dayOfYear(date) {
  const start = Date.UTC(date.getUTCFullYear(), 0, 0);
  return Math.floor((date.getTime() - start) / 86400000);
}

function quarterOfYear(date) {
  return Math.floor(date.getUTCMonth() / 3) + 1;
}

function periodTypeForIndicator(indicatorLike = {}) {
  return indicatorLike.periodType || indicatorLike.period_type || indicatorLike.frequency || "weekly";
}

function marketingYearStartMonth(indicatorLike = {}) {
  const month = Number(indicatorLike.marketingYearStartMonth || indicatorLike.marketing_year_start_month || 1);
  return month >= 1 && month <= 12 ? month : 1;
}

function yearBucketForPoint(date, indicatorLike = {}) {
  if (periodTypeForIndicator(indicatorLike) === "marketing_month") {
    const startMonth = marketingYearStartMonth(indicatorLike);
    return date.getUTCMonth() + 1 >= startMonth ? date.getUTCFullYear() : date.getUTCFullYear() - 1;
  }
  return date.getUTCFullYear();
}

export function periodModeForIndicator(indicatorLike = {}) {
  const periodType = periodTypeForIndicator(indicatorLike);
  if (periodType === "daily") {
    return "day";
  }
  if (periodType === "weekly") {
    return "week";
  }
  if (periodType === "quarterly") {
    return "quarter";
  }
  return "month";
}

export function periodIndexForDate(dateString, indicatorLike = {}) {
  const date = new Date(dateString);
  const periodType = periodTypeForIndicator(indicatorLike);
  if (periodType === "daily") {
    return dayOfYear(date);
  }
  if (periodType === "quarterly") {
    return quarterOfYear(date);
  }
  if (periodType === "marketing_month") {
    const startMonth = marketingYearStartMonth(indicatorLike);
    return ((date.getUTCMonth() + 1 - startMonth + 12) % 12) + 1;
  }
  if (periodType === "weekly") {
    return weekOfYear(date);
  }
  return date.getUTCMonth() + 1;
}

export function periodLabel(periodIndex, indicatorLike = {}) {
  const periodType = periodTypeForIndicator(indicatorLike);
  if (periodType === "daily") {
    return `Day ${periodIndex}`;
  }
  if (periodType === "weekly") {
    return `Week ${periodIndex}`;
  }
  if (periodType === "quarterly") {
    return `Q${periodIndex}`;
  }
  if (periodType === "marketing_month") {
    const baseMonth = ((marketingYearStartMonth(indicatorLike) + periodIndex - 2) % 12) + 1;
    return new Date(Date.UTC(2026, baseMonth - 1, 1)).toLocaleString("en-US", {
      month: "short",
      timeZone: "UTC",
    });
  }
  return new Date(Date.UTC(2026, periodIndex - 1, 1)).toLocaleString("en-US", {
    month: "short",
    timeZone: "UTC",
  });
}

export function buildSeasonalSeries(data) {
  const series = Array.isArray(data?.series) ? data.series : [];
  if (!series.length) {
    return { mode: periodModeForIndicator(data?.indicator), currentYear: [], priorYear: [] };
  }

  const buckets = [...new Set(series.map((point) => yearBucketForPoint(new Date(point.periodEndAt), data.indicator)))].sort(
    (left, right) => left - right
  );
  const currentBucket = buckets[buckets.length - 1];
  const priorBucket = buckets[buckets.length - 2];
  const mode = periodModeForIndicator(data.indicator);

  const mapPoint = (point) => {
    const periodIndex = periodIndexForDate(point.periodEndAt, data.indicator);
    return {
      periodIndex,
      label: periodLabel(periodIndex, data.indicator),
      value: point.value,
      releaseDate: point.releaseDate,
      revisionFlag: Boolean(point.revisionFlag),
    };
  };

  return {
    mode,
    currentYear: series
      .filter((point) => yearBucketForPoint(new Date(point.periodEndAt), data.indicator) === currentBucket)
      .map(mapPoint)
      .sort((left, right) => left.periodIndex - right.periodIndex),
    priorYear: series
      .filter((point) => yearBucketForPoint(new Date(point.periodEndAt), data.indicator) === priorBucket)
      .map(mapPoint)
      .sort((left, right) => left.periodIndex - right.periodIndex),
  };
}

function mean(values) {
  if (!values.length) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values) {
  if (!values.length) {
    return null;
  }
  const ordered = [...values].sort((left, right) => left - right);
  const middle = Math.floor(ordered.length / 2);
  if (ordered.length % 2 === 0) {
    return (ordered[middle - 1] + ordered[middle]) / 2;
  }
  return ordered[middle];
}

function changesWithHistory(data) {
  const series = [...(data?.series || [])].sort(
    (left, right) => new Date(left.periodEndAt).getTime() - new Date(right.periodEndAt).getTime()
  );
  return series
    .map((point, index) => {
      const prior = series[index - 1];
      if (!prior) {
        return null;
      }
      return {
        point,
        prior,
        date: point.periodEndAt,
        value: point.value - prior.value,
        periodIndex: periodIndexForDate(point.periodEndAt, data.indicator),
        yearBucket: yearBucketForPoint(new Date(point.periodEndAt), data.indicator),
      };
    })
    .filter(Boolean);
}

export function buildChangeBarSeries(data) {
  const changes = changesWithHistory(data);
  const seasonalAverageByPeriod = changes.reduce((groups, item) => {
    if (!groups[item.periodIndex]) {
      groups[item.periodIndex] = [];
    }
    groups[item.periodIndex].push(item.value);
    return groups;
  }, {});

  return changes.slice(-24).map((item) => ({
    label: new Date(item.date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      timeZone: "UTC",
    }),
    date: item.date,
    value: item.value,
    releaseDate: item.point.releaseDate,
    seasonalAverageChange: mean(seasonalAverageByPeriod[item.periodIndex] || []),
  }));
}

export function buildYtdChangeStats(data) {
  const series = [...(data?.series || [])].sort(
    (left, right) => new Date(left.periodEndAt).getTime() - new Date(right.periodEndAt).getTime()
  );
  if (series.length < 2) {
    return null;
  }

  const latestDate = new Date(series[series.length - 1].periodEndAt);
  const currentYear = latestDate.getUTCFullYear();
  const currentPeriodIndex = periodIndexForDate(series[series.length - 1].periodEndAt, data.indicator);
  const currentYearPoints = series.filter((point) => new Date(point.periodEndAt).getUTCFullYear() === currentYear);
  if (currentYearPoints.length < 2) {
    return null;
  }

  const ytdChange = currentYearPoints[currentYearPoints.length - 1].value - currentYearPoints[0].value;
  const history = [];

  for (let year = currentYear - 1; year >= currentYear - 5; year -= 1) {
    const points = series.filter((point) => {
      const pointDate = new Date(point.periodEndAt);
      return pointDate.getUTCFullYear() === year && periodIndexForDate(point.periodEndAt, data.indicator) <= currentPeriodIndex;
    });
    if (points.length >= 2) {
      history.push(points[points.length - 1].value - points[0].value);
    }
  }

  const medianYtdChange = median(history);
  return {
    ytdChange,
    medianYtdChange,
    deviationFromMedian: medianYtdChange == null ? null : ytdChange - medianYtdChange,
  };
}

export function seasonalPointForLatest(latestDate, seasonalRange, indicatorLike = {}) {
  const index = periodIndexForDate(latestDate, indicatorLike);
  return seasonalRange.find((point) => point.periodIndex === index) || null;
}

export function percentileBracketLabel(value, seasonalPoint) {
  if (!seasonalPoint) {
    return "Unavailable";
  }
  if (seasonalPoint.p10 != null && value < seasonalPoint.p10) {
    return "Below 10th";
  }
  if (seasonalPoint.p25 != null && value < seasonalPoint.p25) {
    return "10th-25th";
  }
  if (seasonalPoint.p75 != null && value <= seasonalPoint.p75) {
    return "25th-75th";
  }
  if (seasonalPoint.p90 != null && value <= seasonalPoint.p90) {
    return "75th-90th";
  }
  return "Above 90th";
}

export function snapshotSignalDescriptor(card) {
  if (!card?.isSeasonal) {
    return { state: "seasonal-range", label: "Seasonal unavailable" };
  }

  const value = card?.latestValue;
  const p10 = card?.seasonalP10;
  const p25 = card?.seasonalP25;
  const p75 = card?.seasonalP75;
  const p90 = card?.seasonalP90;

  if (typeof value !== "number" || !Number.isFinite(value)) {
    return { state: "seasonal-range", label: "Seasonal range" };
  }
  if (typeof p10 === "number" && Number.isFinite(p10) && value < p10) {
    return { state: "well-below-seasonal", label: "Well below seasonal" };
  }
  if (typeof p25 === "number" && Number.isFinite(p25) && value < p25) {
    return { state: "below-seasonal", label: "Below seasonal" };
  }
  if (typeof p90 === "number" && Number.isFinite(p90) && value > p90) {
    return { state: "well-above-seasonal", label: "Well above seasonal" };
  }
  if (typeof p75 === "number" && Number.isFinite(p75) && value > p75) {
    return { state: "above-seasonal", label: "Above seasonal" };
  }
  return { state: "seasonal-range", label: "Seasonal range" };
}

export function changeToneForValue(value, semanticMode = "generic") {
  if (value == null || value === 0) {
    return "flat";
  }
  const positive = semanticMode === "inventory" ? value < 0 : value > 0;
  return positive ? "positive" : "negative";
}

export function alertToneFromAlerts(alerts = []) {
  if (alerts.some((alert) => alert.tone === "tight")) {
    return "tight";
  }
  if (alerts.some((alert) => alert.tone === "ample")) {
    return "ample";
  }
  if (alerts.some((alert) => alert.tone === "watch")) {
    return "watch";
  }
  if (alerts.some((alert) => alert.tone === "cool")) {
    return "cool";
  }
  return "neutral";
}

export function buildRecentReleaseRows(data) {
  const recent = data.series.slice(-20).reverse();
  const sortedAscending = [...data.series].sort(
    (left, right) => new Date(left.periodEndAt).getTime() - new Date(right.periodEndAt).getTime()
  );

  return recent.map((point) => {
    const index = sortedAscending.findIndex((candidate) => candidate.periodEndAt === point.periodEndAt);
    const prior = index > 0 ? sortedAscending[index - 1] : null;
    const seasonalPoint = seasonalPointForLatest(point.periodEndAt, data.seasonalRange, data.indicator);
    const change = prior ? point.value - prior.value : null;
    const percentChange = prior && prior.value !== 0 ? (change / prior.value) * 100 : null;
    const vsMedian = seasonalPoint?.p50 != null ? point.value - seasonalPoint.p50 : null;

    return {
      date: point.periodEndAt,
      value: point.value,
      change,
      percentChange,
      vsMedian,
      percentileRankLabel: percentileBracketLabel(point.value, seasonalPoint),
      revisionFlag: Boolean(point.revisionFlag),
    };
  });
}

export function humanizeCommodityGroup(slug) {
  return getCommodityGroup(slug).label;
}
