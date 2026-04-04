import { getCommodityGroup, type CommodityGroupSlug } from "@/config/commodities";
import { commodityGroupForCode } from "@/features/inventory/indicator-registry";
import { formatSignedValue } from "@/lib/format/numbers";
import type { AlertKind, IndicatorDataResponse, SeasonalRangePoint, SeriesPoint, SnapshotCardData } from "@/types/api";

export interface SeasonalSeriesPoint {
  periodIndex: number;
  label: string;
  value: number;
  releaseDate: string | null;
}

export interface ChangeBarPoint {
  label: string;
  date: string;
  value: number;
  releaseDate: string | null;
}

export interface RecentReleaseRow {
  periodEndAt: string;
  releaseDate: string | null;
  value: number;
  change: number | null;
  percentChange: number | null;
  vsMedian: number | null;
  percentileRankLabel: string;
}

interface SeasonalBoundsLike {
  p10?: number | null;
  p25?: number | null;
  p50?: number | null;
  p75?: number | null;
  p90?: number | null;
}

export function groupCardsForSnapshot(cards: SnapshotCardData[]) {
  return cards.reduce<Record<string, SnapshotCardData[]>>((accumulator, card) => {
    const key = card.snapshotGroup;
    accumulator[key] ??= [];
    accumulator[key].push(card);
    return accumulator;
  }, {});
}

export function buildVisibleSnapshotSections(
  groupedCards: Record<string, SnapshotCardData[]>,
  sectionOrder: readonly string[],
) {
  return sectionOrder
    .map((name) => [name, groupedCards[name] ?? []] as const)
    .filter(([, sectionCards]) => sectionCards.length > 0);
}

export function filterCardsByCommodityGroup(cards: SnapshotCardData[], slug: CommodityGroupSlug) {
  if (slug === "all") {
    return cards;
  }

  return cards.filter((card) => commodityGroupForCode(card.commodityCode) === slug);
}

export function hasSeasonalCoverage(data: IndicatorDataResponse) {
  return Boolean(data.indicator.isSeasonal && data.seasonalRange.some((point) => point.p50 != null));
}

function periodTypeForIndicator(indicatorLike: Pick<IndicatorDataResponse["indicator"], "frequency" | "periodType">) {
  return indicatorLike.periodType ?? indicatorLike.frequency;
}

function marketingYearStartMonth(indicatorLike: Pick<IndicatorDataResponse["indicator"], "marketingYearStartMonth">) {
  const month = Number(indicatorLike.marketingYearStartMonth ?? 1);
  return month >= 1 && month <= 12 ? month : 1;
}

function yearBucketForPoint(point: SeriesPoint, indicatorLike: IndicatorDataResponse["indicator"]) {
  const date = new Date(point.periodEndAt);
  if (periodTypeForIndicator(indicatorLike) === "marketing_month") {
    const startMonth = marketingYearStartMonth(indicatorLike);
    return date.getUTCMonth() + 1 >= startMonth ? date.getUTCFullYear() : date.getUTCFullYear() - 1;
  }
  return date.getUTCFullYear();
}

function weekOfYear(date: Date) {
  const utcDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = utcDate.getUTCDay() || 7;
  utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
  return Math.min(Math.ceil((((utcDate.getTime() - yearStart.getTime()) / 86400000) + 1) / 7), 52);
}

function dayOfYear(date: Date) {
  const start = Date.UTC(date.getUTCFullYear(), 0, 0);
  return Math.floor((date.getTime() - start) / 86400000);
}

export function periodModeForFrequency(frequency: string): "week" | "month" | "day" {
  if (frequency === "monthly" || frequency === "quarterly" || frequency === "annual") {
    return "month";
  }

  if (frequency === "daily") {
    return "day";
  }

  return "week";
}

export function periodIndexForDate(
  dateString: string,
  indicatorLike: Pick<IndicatorDataResponse["indicator"], "frequency" | "periodType" | "marketingYearStartMonth">,
) {
  const date = new Date(dateString);
  const periodType = periodTypeForIndicator(indicatorLike);

  if (periodType === "marketing_month") {
    const startMonth = marketingYearStartMonth(indicatorLike);
    return ((date.getUTCMonth() + 1 - startMonth + 12) % 12) + 1;
  }

  if (periodType === "monthly" || periodType === "quarterly" || periodType === "annual") {
    return date.getUTCMonth() + 1;
  }

  if (periodType === "daily") {
    return dayOfYear(date);
  }

  return weekOfYear(date);
}

export function periodLabel(
  periodIndex: number,
  indicatorLike: Pick<IndicatorDataResponse["indicator"], "frequency" | "periodType" | "marketingYearStartMonth">,
) {
  const periodType = periodTypeForIndicator(indicatorLike);

  if (periodType === "marketing_month") {
    const baseMonth = ((marketingYearStartMonth(indicatorLike) + periodIndex - 2) % 12) + 1;
    return new Date(Date.UTC(2026, baseMonth - 1, 1)).toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  }

  if (periodType === "monthly" || periodType === "quarterly" || periodType === "annual") {
    return new Date(Date.UTC(2026, periodIndex - 1, 1)).toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  }

  if (periodType === "daily") {
    return `Day ${periodIndex}`;
  }

  return `Week ${periodIndex}`;
}

export function buildSeasonalSeries(data: IndicatorDataResponse) {
  const buckets = [...new Set(data.series.map((point) => yearBucketForPoint(point, data.indicator)))].sort((a, b) => a - b);
  const currentBucket = buckets[buckets.length - 1];
  const priorBucket = buckets[buckets.length - 2];
  const mode = periodModeForFrequency(data.indicator.frequency);

  const mapPoint = (point: SeriesPoint): SeasonalSeriesPoint => ({
    periodIndex: periodIndexForDate(point.periodEndAt, data.indicator),
    label: periodLabel(periodIndexForDate(point.periodEndAt, data.indicator), data.indicator),
    value: point.value,
    releaseDate: point.releaseDate,
  });

  return {
    mode,
    currentYear: data.series
      .filter((point) => yearBucketForPoint(point, data.indicator) === currentBucket)
      .map(mapPoint)
      .sort((a, b) => a.periodIndex - b.periodIndex),
    priorYear: data.series
      .filter((point) => yearBucketForPoint(point, data.indicator) === priorBucket)
      .map(mapPoint)
      .sort((a, b) => a.periodIndex - b.periodIndex),
  };
}

export function buildChangeBarSeries(series: SeriesPoint[]): ChangeBarPoint[] {
  return series
    .slice(-24)
    .map((point, index, points) => {
      const prior = points[index - 1];
      return {
        label: new Date(point.periodEndAt).toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
          timeZone: "UTC",
        }),
        date: point.periodEndAt,
        value: prior ? point.value - prior.value : 0,
        releaseDate: point.releaseDate,
      };
    })
    .slice(1);
}

export function seasonalPointForLatest(
  latestDate: string,
  seasonalRange: SeasonalRangePoint[],
  indicatorLike: Pick<IndicatorDataResponse["indicator"], "frequency" | "periodType" | "marketingYearStartMonth">,
) {
  const index = periodIndexForDate(latestDate, indicatorLike);
  return seasonalRange.find((point) => point.periodIndex === index) ?? null;
}

export function hasSeasonalBaseline(seasonalPoint: SeasonalBoundsLike | null | undefined) {
  return Boolean(seasonalPoint && seasonalPoint.p50 != null);
}

export function percentileBracketLabel(value: number, seasonalPoint: SeasonalBoundsLike | null | undefined) {
  if (!seasonalPoint || seasonalPoint.p50 == null) {
    return "Unavailable";
  }

  const bounds = seasonalPoint;
  if (bounds.p10 != null && value < bounds.p10) return "Below 10th";
  if (bounds.p25 != null && value < bounds.p25) return "10th-25th";
  if (bounds.p75 != null && value <= bounds.p75) return "25th-75th";
  if (bounds.p90 != null && value <= bounds.p90) return "75th-90th";
  return "Above 90th";
}

export function alertKindFromSeasonalPosition(value: number | null | undefined, seasonalPoint: SeasonalBoundsLike | null | undefined): AlertKind | null {
  if (value == null || !seasonalPoint || seasonalPoint.p50 == null) {
    return null;
  }

  const bounds = seasonalPoint;
  if (bounds.p10 != null && value < bounds.p10) return "extreme-low";
  if (bounds.p90 != null && value > bounds.p90) return "extreme-high";
  return null;
}

function seasonalPointFromSnapshotCard(card: Pick<SnapshotCardData, "seasonalP10" | "seasonalP25" | "seasonalMedian" | "seasonalP75" | "seasonalP90">) {
  return {
    p10: card.seasonalP10,
    p25: card.seasonalP25,
    p50: card.seasonalMedian,
    p75: card.seasonalP75,
    p90: card.seasonalP90,
  };
}

export function snapshotHasSeasonalBaseline(
  card: Pick<SnapshotCardData, "seasonalMedian" | "seasonalP10" | "seasonalP25" | "seasonalP75" | "seasonalP90">,
) {
  return hasSeasonalBaseline(seasonalPointFromSnapshotCard(card));
}

export function alertKindFromSnapshotCard(
  card: Pick<SnapshotCardData, "latestValue" | "seasonalMedian" | "seasonalP10" | "seasonalP25" | "seasonalP75" | "seasonalP90">,
) {
  return alertKindFromSeasonalPosition(card.latestValue, seasonalPointFromSnapshotCard(card));
}

export function snapshotSeasonalComparisonText(
  card: Pick<SnapshotCardData, "deviationAbs" | "unit" | "seasonalMedian" | "seasonalP10" | "seasonalP25" | "seasonalP75" | "seasonalP90">,
) {
  if (!snapshotHasSeasonalBaseline(card)) {
    return { title: "Seasonal baseline", value: "Current only" };
  }

  return {
    title: "vs 5Y median",
    value: describeMedianDeviation(card.deviationAbs, card.unit),
  };
}

export function describeMedianDeviation(value: number | null | undefined, unit: string | null | undefined) {
  if (value == null) {
    return "Median unavailable";
  }

  if (value === 0) {
    return "At median";
  }

  return `${formatSignedValue(value, unit)} ${value < 0 ? "below" : "above"} median`;
}

export function changeReferenceLabel(indicatorLike: { frequency?: string | null; periodType?: string | null }) {
  const cadence = indicatorLike.periodType ?? indicatorLike.frequency ?? null;

  switch (cadence) {
    case "daily":
      return "vs prior day";
    case "weekly":
      return "vs prior week";
    case "monthly":
    case "marketing_month":
      return "vs prior month";
    case "quarterly":
      return "vs prior quarter";
    case "annual":
      return "vs prior year";
    default:
      return "vs prior period";
  }
}

export function trendWindowLabel(observationCount: number) {
  if (observationCount <= 0) {
    return "Trend unavailable";
  }

  return `Trend · last ${observationCount} observation${observationCount === 1 ? "" : "s"}`;
}

export function buildRecentReleaseRows(data: IndicatorDataResponse): RecentReleaseRow[] {
  const recent = data.series.slice(-20).reverse();
  const sortedAsc = [...data.series].sort((a, b) => new Date(a.periodEndAt).getTime() - new Date(b.periodEndAt).getTime());
  const valueByDate = new Map(sortedAsc.map((point) => [point.periodEndAt, point]));

  return recent.map((point) => {
    const pointIndex = sortedAsc.findIndex((candidate) => candidate.periodEndAt === point.periodEndAt);
    const prior = pointIndex > 0 ? sortedAsc[pointIndex - 1] : null;
    const seasonalPoint = seasonalPointForLatest(point.periodEndAt, data.seasonalRange, data.indicator);
    const change = prior ? point.value - prior.value : null;
    const percentChange = prior && prior.value !== 0 ? (change! / prior.value) * 100 : null;
    const vsMedian = seasonalPoint?.p50 != null ? point.value - seasonalPoint.p50 : null;

    return {
      periodEndAt: point.periodEndAt,
      releaseDate: point.releaseDate,
      value: point.value,
      change,
      percentChange,
      vsMedian,
      percentileRankLabel: percentileBracketLabel(point.value, seasonalPoint),
    };
  });
}

export function humanizeCommodityGroup(slug: CommodityGroupSlug) {
  return getCommodityGroup(slug).label;
}
