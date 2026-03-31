import { commodityGroupForCode, getCommodityGroup } from "./catalog.js";

export const SNAPSHOT_SECTION_ORDER = [
  "Crude Oil",
  "Refined Products",
  "Natural Gas",
  "Base Metals",
  "Grains",
  "Softs",
  "Precious Metals",
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

export function periodModeForFrequency(frequency) {
  if (frequency === "monthly" || frequency === "quarterly" || frequency === "annual") {
    return "month";
  }

  if (frequency === "daily") {
    return "day";
  }

  return "week";
}

export function periodIndexForDate(dateString, frequency) {
  const date = new Date(dateString);
  if (frequency === "monthly" || frequency === "quarterly" || frequency === "annual") {
    return date.getUTCMonth() + 1;
  }

  if (frequency === "daily") {
    return dayOfYear(date);
  }

  return weekOfYear(date);
}

export function periodLabel(periodIndex, frequency) {
  if (frequency === "monthly" || frequency === "quarterly" || frequency === "annual") {
    return new Date(Date.UTC(2026, periodIndex - 1, 1)).toLocaleString("en-US", {
      month: "short",
      timeZone: "UTC",
    });
  }

  if (frequency === "daily") {
    return `Day ${periodIndex}`;
  }

  return `Week ${periodIndex}`;
}

export function buildSeasonalSeries(data) {
  const currentYear = new Date().getUTCFullYear();
  const priorYear = currentYear - 1;
  const mode = periodModeForFrequency(data.indicator.frequency);

  const mapPoint = (point) => {
    const periodIndex = periodIndexForDate(point.periodEndAt, data.indicator.frequency);
    return {
      periodIndex,
      label: periodLabel(periodIndex, data.indicator.frequency),
      value: point.value,
      releaseDate: point.releaseDate,
    };
  };

  return {
    mode,
    currentYear: data.series
      .filter((point) => new Date(point.periodEndAt).getUTCFullYear() === currentYear)
      .map(mapPoint)
      .sort((left, right) => left.periodIndex - right.periodIndex),
    priorYear: data.series
      .filter((point) => new Date(point.periodEndAt).getUTCFullYear() === priorYear)
      .map(mapPoint)
      .sort((left, right) => left.periodIndex - right.periodIndex),
  };
}

export function buildChangeBarSeries(series) {
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

export function seasonalPointForLatest(latestDate, seasonalRange, frequency) {
  const index = periodIndexForDate(latestDate, frequency);
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

export function alertKindFromSeasonal(value, seasonalPoint) {
  if (!seasonalPoint) {
    return null;
  }

  if (seasonalPoint.p10 != null && value < seasonalPoint.p10) {
    return "extreme-low";
  }
  if (seasonalPoint.p90 != null && value > seasonalPoint.p90) {
    return "extreme-high";
  }
  return null;
}

export function alertKindFromLatest(latest) {
  const zscore = latest?.latest?.deviationFromSeasonalZscore;
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

export function buildRecentReleaseRows(data) {
  const recent = data.series.slice(-20).reverse();
  const sortedAscending = [...data.series].sort(
    (left, right) => new Date(left.periodEndAt).getTime() - new Date(right.periodEndAt).getTime()
  );

  return recent.map((point) => {
    const index = sortedAscending.findIndex((candidate) => candidate.periodEndAt === point.periodEndAt);
    const prior = index > 0 ? sortedAscending[index - 1] : null;
    const seasonalPoint = seasonalPointForLatest(point.periodEndAt, data.seasonalRange, data.indicator.frequency);
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
    };
  });
}

export function humanizeCommodityGroup(slug) {
  return getCommodityGroup(slug).label;
}
