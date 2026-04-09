export const DEMAND_TAXONOMY = [
  {
    id: "T1",
    shortLabel: "T1 · Direct",
    label: "Direct Consumption",
    reliability: "Highest",
    description: "Measured volumes actually consumed or delivered.",
  },
  {
    id: "T2",
    shortLabel: "T2 · Throughput",
    label: "Throughput Proxy",
    reliability: "High",
    description: "Processing volumes that imply input demand.",
  },
  {
    id: "T3",
    shortLabel: "T3 · Trade Flow",
    label: "Trade Flow",
    reliability: "High",
    description: "Import and export volumes showing cross-border demand.",
  },
  {
    id: "T4",
    shortLabel: "T4 · End-Use",
    label: "End-Use Activity",
    reliability: "Medium-High",
    description: "Output from the consuming industries underneath the market.",
  },
  {
    id: "T5",
    shortLabel: "T5 · Leading",
    label: "Leading Indicator",
    reliability: "Medium",
    description: "Forward-looking signals of future demand.",
  },
  {
    id: "T6",
    shortLabel: "T6 · Macro",
    label: "Macro Context",
    reliability: "Medium",
    description: "The economic backdrop shaping the demand cycle.",
  },
  {
    id: "T7",
    shortLabel: "T7 · Weather",
    label: "Weather-Derived",
    reliability: "Conditional",
    description: "Temperature-driven signals, strongest for gas and power.",
  },
];

export const DEMAND_VERTICALS = [
  {
    id: "crude-products",
    code: "crude_products",
    navLabel: "Crude",
    label: "Crude Oil + Refined Products",
    shortLabel: "Crude + Products",
    sectorId: "energy",
    accent: "var(--color-energy)",
    supportedGroupIds: ["crude-oil"],
  },
  {
    id: "electricity",
    code: "electricity",
    navLabel: "Power",
    label: "Electricity / Power",
    shortLabel: "Electricity",
    sectorId: "energy",
    accent: "var(--color-natural-gas)",
    supportedGroupIds: ["power"],
  },
  {
    id: "grains",
    code: "grains_oilseeds",
    navLabel: "Grains",
    label: "Grains & Oilseeds",
    shortLabel: "Grains",
    sectorId: "agriculture",
    accent: "var(--color-agri)",
    supportedGroupIds: ["grains-oilseeds"],
  },
  {
    id: "base-metals",
    code: "base_metals",
    navLabel: "Metals",
    label: "Base Metals / Industrial Demand",
    shortLabel: "Base Metals",
    sectorId: "metals",
    accent: "var(--color-metals)",
    supportedGroupIds: ["base-metals", "battery-metals"],
  },
];

const DEMAND_VERTICAL_ORDER = new Map(DEMAND_VERTICALS.map((vertical, index) => [vertical.id, index]));
const DEMAND_VERTICALS_BY_ID = new Map(DEMAND_VERTICALS.map((vertical) => [vertical.id, vertical]));
const DEMAND_TIER_LABELS = new Map(
  DEMAND_TAXONOMY.map((tier) => [
    String(tier.id || "").toLowerCase(),
    tier.shortLabel,
  ])
);

const TIER_SORT_ORDER = {
  t1_direct: 1,
  t2_throughput: 2,
  t3_trade: 3,
  t4_end_use: 4,
  t5_leading: 5,
  t6_macro: 6,
  t7_weather: 7,
};

const RELEASE_DATE_FORMATTER = new Intl.DateTimeFormat("en-GB", {
  weekday: "short",
  day: "2-digit",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

function firstNonEmpty(...values) {
  return values.find((value) => value !== null && value !== undefined && String(value).trim() !== "") ?? null;
}

function titleCase(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function fallbackSparkline(values, fallbackValue = null) {
  const points = asArray(values).filter((value) => Number.isFinite(value));
  if (points.length > 1) {
    return points;
  }

  if (points.length === 1) {
    return [points[0], points[0]];
  }

  if (Number.isFinite(fallbackValue)) {
    return [fallbackValue, fallbackValue];
  }

  return [0, 0];
}

function tierOrder(value) {
  return TIER_SORT_ORDER[String(value || "").toLowerCase()] ?? 99;
}

function tierLabel(value) {
  const normalized = String(value || "").toLowerCase();
  const compact = normalized.startsWith("t") ? normalized.slice(0, 2) : normalized;
  return DEMAND_TIER_LABELS.get(compact) || titleCase(value);
}

function sortCoverageItems(items) {
  return [...asArray(items)].sort((left, right) => {
    const tierDelta = tierOrder(left.tier) - tierOrder(right.tier);
    if (tierDelta !== 0) {
      return tierDelta;
    }

    return String(left.name || left.code || "").localeCompare(String(right.name || right.code || ""));
  });
}

function formatReleaseStamp(value) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    return null;
  }

  return RELEASE_DATE_FORMATTER.format(parsed).replace(",", "");
}

function coverageLabel(status) {
  switch (String(status || "").toLowerCase()) {
    case "live":
      return "Live";
    case "partial":
      return "Partial";
    case "blocked":
      return "Blocked";
    case "needs_verification":
      return "Needs verification";
    case "deferred":
      return "Deferred";
    default:
      return "Coverage note";
  }
}

export function coverageToneForStatus(status) {
  switch (String(status || "").toLowerCase()) {
    case "live":
      return "live";
    case "partial":
      return "partial";
    case "blocked":
      return "blocked";
    case "needs_verification":
    case "deferred":
      return "deferred";
    default:
      return "neutral";
  }
}

function coverageDetail(item) {
  const reason = firstNonEmpty(...asArray(item?.reasons));
  const sourceName = firstNonEmpty(item?.source_name, item?.source_slug);
  if (reason && sourceName) {
    return `${sourceName} - ${reason}`;
  }
  return reason || sourceName || "Coverage remains explicit until republication terms are clear.";
}

function coverageSummaryLabel(coverageVertical) {
  const counts = coverageVertical?.counts || {};
  const parts = [];

  if (Number.isFinite(counts.live) && counts.live > 0) {
    parts.push(`${counts.live} live`);
  }
  if (Number.isFinite(counts.partial) && counts.partial > 0) {
    parts.push(`${counts.partial} partial`);
  }
  if (Number.isFinite(counts.deferred) && counts.deferred > 0) {
    parts.push(`${counts.deferred} deferred`);
  }
  if (Number.isFinite(counts.blocked) && counts.blocked > 0) {
    parts.push(`${counts.blocked} blocked`);
  }

  return parts.join(" / ") || "No coverage metadata";
}

function gapSectionDescription(coverageVertical) {
  const counts = coverageVertical?.counts || {};
  const deferredCount = Number.isFinite(counts.deferred) ? counts.deferred : 0;
  const blockedCount = Number.isFinite(counts.blocked) ? counts.blocked : 0;
  const parts = [];

  if (deferredCount > 0) {
    parts.push(`${deferredCount} deferred`);
  }
  if (blockedCount > 0) {
    parts.push(`${blockedCount} blocked`);
  }

  return parts.length
    ? `DemandWatch keeps ${parts.join(" / ")} indicators visible without fabricated values.`
    : "Coverage gaps remain explicit where the backend marks them as deferred or blocked.";
}

function mapDemandMacroStripItem(item) {
  return {
    id: item.id,
    code: item.code,
    label: item.label,
    descriptor: item.descriptor,
    value: firstNonEmpty(item.display_value, item.latest_period_label, "No live value"),
    change: firstNonEmpty(item.change_label, item.latest_period_label, "Awaiting next release"),
    trend: item.trend || "flat",
    freshness: firstNonEmpty(item.freshness, item.latest_period_label, "Unknown"),
    freshnessState: item.freshness_state || "unknown",
    sourceUrl: item.source_url || null,
  };
}

export function mapDemandScorecardItem(item) {
  const meta = getDemandVerticalById(item.id);

  return {
    id: item.id,
    code: item.code,
    navLabel: firstNonEmpty(item.nav_label, meta?.navLabel, item.label, item.id),
    label: firstNonEmpty(item.label, meta?.label, item.id),
    shortLabel: firstNonEmpty(item.short_label, meta?.shortLabel, item.nav_label, item.label, item.id),
    sectorId: firstNonEmpty(item.sector, meta?.sectorId, "energy"),
    accent: meta?.accent || "var(--color-amber)",
    supportedGroupIds: meta?.supportedGroupIds || [],
    scorecard: {
      label: firstNonEmpty(item.scorecard_label, "Demand signal"),
      value: firstNonEmpty(item.display_value, "No live value"),
      yoyLabel: firstNonEmpty(item.yoy_label, "YoY unavailable"),
      yoyValue: item.yoy_value ?? null,
      trend: item.trend || "flat",
      latestData: firstNonEmpty(item.latest_period_label, "Latest release"),
      freshness: firstNonEmpty(item.freshness, "Unknown"),
      freshnessState: item.freshness_state || "unknown",
      stale: Boolean(item.stale),
      sourceUrl: item.source_url || null,
      primarySeriesCode: item.primary_series_code,
    },
  };
}

export function mapDemandScorecardItems(items) {
  return sortDemandVerticalItems(asArray(items).map(mapDemandScorecardItem));
}

function mapDemandMoverItem(item) {
  const meta = getDemandVerticalById(item.vertical_id);

  return {
    id: `${item.vertical_id}:${item.code}`,
    verticalId: item.vertical_id,
    tier: firstNonEmpty(item.tier_label, item.tier, "Tier"),
    title: item.title,
    value: firstNonEmpty(item.display_value, "No live value"),
    change: firstNonEmpty(item.change_label, item.latest_period_label, "Awaiting next release"),
    surprise: firstNonEmpty(item.surprise_label, item.latest_period_label, "No active surprise flag"),
    freshness: firstNonEmpty(item.freshness, "Unknown"),
    freshnessState: item.freshness_state || "unknown",
    trend: item.trend || "flat",
    accent: meta?.accent || "var(--color-amber)",
    sourceUrl: item.source_url || null,
  };
}

function mapIndicatorCard(indicator) {
  return {
    id: indicator.series_id || indicator.code,
    title: indicator.title,
    tier: firstNonEmpty(indicator.tier_label, indicator.tier, "Tier"),
    value: firstNonEmpty(indicator.display_value, "No live value"),
    change: firstNonEmpty(indicator.change_label, indicator.latest_period_label, "Awaiting next release"),
    detail: firstNonEmpty(
      indicator.detail,
      indicator.latest_period_label ? `As of ${indicator.latest_period_label}` : null,
      indicator.freshness ? `Updated ${indicator.freshness}` : null,
      "Awaiting next release"
    ),
    trend: indicator.trend || "flat",
    sparkline: fallbackSparkline(indicator.sparkline, indicator.latest_value),
    freshness: firstNonEmpty(indicator.freshness, "Unknown"),
    freshnessState: indicator.freshness_state || "unknown",
    latestPeriodLabel: indicator.latest_period_label || null,
    sourceUrl: indicator.source_url || null,
    coverageStatus: indicator.coverage_status || "live",
    coverageLabel: coverageLabel(indicator.coverage_status || "live"),
    coverageTone: coverageToneForStatus(indicator.coverage_status || "live"),
    placeholder: false,
  };
}

function mapIndicatorTableRow(row) {
  return {
    id: row.series_id || row.code,
    label: row.label,
    latest: firstNonEmpty(row.latest_display, "No live value"),
    change: firstNonEmpty(row.change_display, "-"),
    yoy: firstNonEmpty(row.yoy_display, "-"),
    freshness: firstNonEmpty(row.freshness, "Unknown"),
    freshnessState: row.freshness_state || "unknown",
    trend: row.trend || "flat",
    sourceUrl: row.source_url || null,
    placeholder: false,
  };
}

function mapCoveragePlaceholderCard(item) {
  const status = item.coverage_status || "deferred";

  return {
    id: item.series_id || item.code,
    title: firstNonEmpty(item.name, item.code, "Coverage gap"),
    tier: tierLabel(item.tier || "coverage"),
    value: coverageLabel(status),
    change: coverageLabel(status),
    detail: coverageDetail(item),
    trend: "flat",
    sparkline: [0, 0],
    freshness: coverageLabel(status),
    freshnessState: item.freshness_state || "unknown",
    latestPeriodLabel: item.latest_period_label || null,
    sourceUrl: item.source_url || null,
    coverageStatus: status,
    coverageLabel: coverageLabel(status),
    coverageTone: coverageToneForStatus(status),
    placeholder: true,
  };
}

function mapCoveragePlaceholderRow(item) {
  const status = item.coverage_status || "deferred";

  return {
    id: item.series_id || item.code,
    label: firstNonEmpty(item.name, item.code, "Coverage gap"),
    latest: coverageLabel(status),
    change: "-",
    yoy: "-",
    freshness: coverageLabel(status),
    freshnessState: item.freshness_state || "unknown",
    trend: "flat",
    sourceUrl: item.source_url || null,
    placeholder: true,
  };
}

function mapReleaseItem(item) {
  const scheduleLabel = formatReleaseStamp(item.scheduled_for);

  return {
    label: item.release_name,
    value: scheduleLabel ? `${item.is_estimated ? "Estimated - " : ""}${scheduleLabel} UTC` : titleCase(item.cadence || "schedule pending"),
    note: firstNonEmpty(
      asArray(item.notes)[0],
      item.source_name ? `${item.source_name} - ${titleCase(item.cadence || "release")}` : null,
      "Release timing pending"
    ),
  };
}

function findCoverageVertical(coverageNotes, verticalId) {
  return asArray(coverageNotes?.verticals).find((vertical) => vertical.id === verticalId) || null;
}

function buildCoverageGapSection(coverageVertical) {
  const placeholderItems = sortCoverageItems([
    ...asArray(coverageVertical?.deferred),
    ...asArray(coverageVertical?.blocked),
  ]);

  if (!placeholderItems.length) {
    return null;
  }

  return {
    id: "coverage-gaps",
    title: "Coverage Gaps",
    description: gapSectionDescription(coverageVertical),
    indicators: placeholderItems.map(mapCoveragePlaceholderCard),
    tableRows: placeholderItems.map(mapCoveragePlaceholderRow),
    isCoverageGap: true,
  };
}

export function mapDemandVerticalDetail(detail, coverageNotes) {
  const meta = getDemandVerticalById(detail.id);
  const coverageVertical = findCoverageVertical(coverageNotes, detail.id);
  const gapSection = buildCoverageGapSection(coverageVertical);
  const sections = asArray(detail.sections).map((section) => ({
    id: section.id,
    title: section.title,
    description: section.description,
    indicators: asArray(section.indicators).map(mapIndicatorCard),
    tableRows: asArray(section.table_rows).map(mapIndicatorTableRow),
    isCoverageGap: false,
  }));

  if (gapSection) {
    sections.push(gapSection);
  }

  return {
    id: detail.id,
    code: detail.code,
    navLabel: firstNonEmpty(detail.nav_label, meta?.navLabel, detail.label, detail.id),
    label: firstNonEmpty(detail.label, meta?.label, detail.id),
    shortLabel: firstNonEmpty(detail.short_label, meta?.shortLabel, detail.nav_label, detail.label, detail.id),
    sectorId: firstNonEmpty(detail.sector, meta?.sectorId, "energy"),
    accent: meta?.accent || "var(--color-amber)",
    supportedGroupIds: meta?.supportedGroupIds || [],
    summary: firstNonEmpty(detail.summary, "Demand coverage is loading."),
    facts: asArray(detail.facts),
    scorecard: mapDemandScorecardItem(detail.scorecard).scorecard,
    sections,
    calendar: asArray(detail.calendar).map(mapReleaseItem),
    notes: asArray(detail.notes),
    coverageLabel: coverageSummaryLabel(coverageVertical),
    coverageVertical,
  };
}

function buildHeroSummary(scorecardItems, coverageSummary) {
  const counts = scorecardItems.reduce(
    (accumulator, item) => {
      const trend = item?.scorecard?.trend || "flat";
      accumulator[trend] = (accumulator[trend] || 0) + 1;
      return accumulator;
    },
    { up: 0, down: 0, flat: 0 }
  );

  let value = "Demand is mixed across the launch set.";
  if (counts.up > 0 && counts.down === 0) {
    value = counts.flat > 0 ? "Demand is firm where live coverage is deepest." : "Demand is improving across the launch set.";
  } else if (counts.down > 0 && counts.up === 0) {
    value = counts.flat > 0 ? "Demand is softening in part of the launch set." : "Demand is softening across the launch set.";
  } else if (counts.up === 0 && counts.down === 0) {
    value = "Demand is stable across the launch set.";
  }

  const statusCounts = coverageSummary?.status_counts || {};
  const liveCount = Number.isFinite(statusCounts.live) ? statusCounts.live : 0;
  const deferredCount = Number.isFinite(statusCounts.deferred) ? statusCounts.deferred : 0;
  const blockedCount = Number.isFinite(statusCounts.blocked) ? statusCounts.blocked : 0;
  const explicitGaps = deferredCount + blockedCount;

  return {
    label: "Current signal",
    value,
    copy:
      liveCount || explicitGaps
        ? `${liveCount} live series in the launch set, ${explicitGaps} explicit coverage gaps, freshness labelled on every release.`
        : "Freshness and coverage metadata stay attached to every release.",
  };
}

export function buildDemandWatchPageModel({
  macroStrip,
  scorecard,
  movers,
  coverageNotes,
  verticalDetails,
} = {}) {
  const mappedScorecard = mapDemandScorecardItems(scorecard?.items);
  const mappedVerticals = sortDemandVerticalItems(asArray(verticalDetails).map((detail) => mapDemandVerticalDetail(detail, coverageNotes)));

  return {
    generatedAt: firstNonEmpty(
      coverageNotes?.generated_at,
      scorecard?.generated_at,
      macroStrip?.generated_at,
      movers?.generated_at,
      null
    ),
    hero: {
      ...buildHeroSummary(mappedScorecard, coverageNotes?.summary),
      stats: [
        { label: "verticals", value: String(mappedScorecard.length || DEMAND_VERTICALS.length) },
        { label: "signal tiers", value: String(DEMAND_TAXONOMY.length) },
        {
          label: "tracked series",
          value: String(coverageNotes?.summary?.series_count || 0),
        },
      ],
    },
    macroStrip: asArray(macroStrip?.items).map(mapDemandMacroStripItem),
    scorecard: mappedScorecard,
    movers: asArray(movers?.items).map(mapDemandMoverItem),
    verticals: mappedVerticals,
    taxonomy: DEMAND_TAXONOMY,
    coverageSummary: coverageNotes?.summary || null,
    coverageNotes: coverageNotes || null,
  };
}

export function sortDemandVerticalItems(items) {
  return [...asArray(items)].sort((left, right) => {
    const leftIndex = DEMAND_VERTICAL_ORDER.get(left.id) ?? 999;
    const rightIndex = DEMAND_VERTICAL_ORDER.get(right.id) ?? 999;
    if (leftIndex !== rightIndex) {
      return leftIndex - rightIndex;
    }

    return String(left.label || left.id || "").localeCompare(String(right.label || right.id || ""));
  });
}

export function getDemandVerticalById(verticalId) {
  return DEMAND_VERTICALS_BY_ID.get(verticalId) || null;
}
