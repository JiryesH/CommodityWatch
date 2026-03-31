import { getCommodityGroup, type CommodityGroupSlug } from "@/config/commodities";
import { commodityGroupForCode } from "@/features/inventory/indicator-registry";
import type { AlertKind, IndicatorDataResponse, IndicatorLatestResponse, SeasonalRangePoint, SeriesPoint, SnapshotCardData } from "@/types/api";

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
  date: string;
  value: number;
  change: number | null;
  percentChange: number | null;
  vsMedian: number | null;
  percentileRankLabel: string;
}

export function groupCardsForSnapshot(cards: SnapshotCardData[]) {
  return cards.reduce<Record<string, SnapshotCardData[]>>((accumulator, card) => {
    const key = card.snapshotGroup;
    accumulator[key] ??= [];
    accumulator[key].push(card);
    return accumulator;
  }, {});
}

export function filterCardsByCommodityGroup(cards: SnapshotCardData[], slug: CommodityGroupSlug) {
  if (slug === "all") {
    return cards;
  }

  return cards.filter((card) => commodityGroupForCode(card.commodityCode) === slug);
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

export function periodIndexForDate(dateString: string, frequency: string) {
  const date = new Date(dateString);
  if (frequency === "monthly" || frequency === "quarterly" || frequency === "annual") {
    return date.getUTCMonth() + 1;
  }

  if (frequency === "daily") {
    return dayOfYear(date);
  }

  return weekOfYear(date);
}

export function periodLabel(periodIndex: number, frequency: string) {
  if (frequency === "monthly" || frequency === "quarterly" || frequency === "annual") {
    return new Date(Date.UTC(2026, periodIndex - 1, 1)).toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  }

  if (frequency === "daily") {
    return `Day ${periodIndex}`;
  }

  return `Week ${periodIndex}`;
}

export function buildSeasonalSeries(data: IndicatorDataResponse) {
  const currentYear = new Date().getUTCFullYear();
  const priorYear = currentYear - 1;
  const mode = periodModeForFrequency(data.indicator.frequency);

  const mapPoint = (point: SeriesPoint): SeasonalSeriesPoint => ({
    periodIndex: periodIndexForDate(point.periodEndAt, data.indicator.frequency),
    label: periodLabel(periodIndexForDate(point.periodEndAt, data.indicator.frequency), data.indicator.frequency),
    value: point.value,
    releaseDate: point.releaseDate,
  });

  return {
    mode,
    currentYear: data.series
      .filter((point) => new Date(point.periodEndAt).getUTCFullYear() === currentYear)
      .map(mapPoint)
      .sort((a, b) => a.periodIndex - b.periodIndex),
    priorYear: data.series
      .filter((point) => new Date(point.periodEndAt).getUTCFullYear() === priorYear)
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

export function seasonalPointForLatest(latestDate: string, seasonalRange: SeasonalRangePoint[], frequency: string) {
  const index = periodIndexForDate(latestDate, frequency);
  return seasonalRange.find((point) => point.periodIndex === index) ?? null;
}

export function percentileBracketLabel(value: number, seasonalPoint: SeasonalRangePoint | null) {
  if (!seasonalPoint) {
    return "Unavailable";
  }

  if (seasonalPoint.p10 != null && value < seasonalPoint.p10) return "Below 10th";
  if (seasonalPoint.p25 != null && value < seasonalPoint.p25) return "10th-25th";
  if (seasonalPoint.p75 != null && value <= seasonalPoint.p75) return "25th-75th";
  if (seasonalPoint.p90 != null && value <= seasonalPoint.p90) return "75th-90th";
  return "Above 90th";
}

export function alertKindFromSeasonal(value: number, seasonalPoint: SeasonalRangePoint | null): AlertKind | null {
  if (!seasonalPoint) {
    return null;
  }

  if (seasonalPoint.p10 != null && value < seasonalPoint.p10) return "extreme-low";
  if (seasonalPoint.p90 != null && value > seasonalPoint.p90) return "extreme-high";
  return null;
}

export function alertKindFromLatest(latest: IndicatorLatestResponse | null | undefined): AlertKind | null {
  const zscore = latest?.latest.deviationFromSeasonalZscore;
  if (zscore == null) {
    return null;
  }

  if (zscore <= -1.28) {
    return "extreme-low";
  }

  if (zscore >= 1.28) {
    return "extreme-high";
  }

  return null;
}

export function buildRecentReleaseRows(data: IndicatorDataResponse): RecentReleaseRow[] {
  const recent = data.series.slice(-20).reverse();
  const sortedAsc = [...data.series].sort((a, b) => new Date(a.periodEndAt).getTime() - new Date(b.periodEndAt).getTime());
  const valueByDate = new Map(sortedAsc.map((point) => [point.periodEndAt, point]));

  return recent.map((point) => {
    const pointIndex = sortedAsc.findIndex((candidate) => candidate.periodEndAt === point.periodEndAt);
    const prior = pointIndex > 0 ? sortedAsc[pointIndex - 1] : null;
    const seasonalPoint = seasonalPointForLatest(point.periodEndAt, data.seasonalRange, data.indicator.frequency);
    const change = prior ? point.value - prior.value : null;
    const percentChange = prior && prior.value !== 0 ? (change! / prior.value) * 100 : null;
    const vsMedian = seasonalPoint?.p50 != null ? point.value - seasonalPoint.p50 : null;

    return {
      date: point.periodEndAt,
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
