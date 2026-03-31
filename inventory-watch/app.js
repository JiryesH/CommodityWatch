import {
  COMMODITY_GROUPS,
  FRESHNESS_BADGES,
  commodityGroupForCode,
  freshnessFor,
  formatPercent,
  formatSignedValue,
  formatUtcTimestamp,
  formatValue,
  groupDescriptionFor,
  isoDate,
  semanticModeForCommodity,
} from "./catalog.js";
import { fetchIndicatorData, fetchIndicatorLatest, fetchInventorySnapshot } from "./api-client.js";
import {
  alertKindFromSeasonal,
  buildChangeBarSeries,
  buildRecentReleaseRows,
  buildSeasonalSeries,
  filterCardsByCommodityGroup,
  groupCardsForSnapshot,
  percentileBracketLabel,
  seasonalPointForLatest,
  snapshotSignalDescriptor,
  snapshotSectionEntries,
} from "./model.js";
import {
  buildInventoryDetailHref,
  buildInventorySnapshotHref,
  parseInventoryRoute,
} from "./router.js";

const filterRoot = document.getElementById("inventory-filter-root");
const appRoot = document.getElementById("inventory-root");
const toTopButton = document.getElementById("to-top-btn");

if (!filterRoot || !appRoot) {
  throw new Error("InventoryWatch shell roots are missing.");
}

const snapshotCache = {
  promise: null,
  value: null,
};
const detailDataCache = new Map();
const alternateDataCache = new Map();
const latestCache = new Map();

let renderRequestId = 0;
let excludeYear2020 = false;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizePath(pathname) {
  if (!pathname || pathname === "/") {
    return "/";
  }

  return pathname.endsWith("/") ? pathname : `${pathname}/`;
}

function currentRoute() {
  return parseInventoryRoute(window.location.pathname);
}

function setDocumentTitle(title) {
  document.title = title;
}

function alertBadge(kind, labelOverride = "") {
  if (!kind) {
    return "";
  }

  const label = labelOverride || (kind === "extreme-low" ? "Below seasonal" : "Above seasonal");
  const className = kind === "extreme-low" ? "is-alert-low" : "is-alert-high";
  return `<span class="inventory-badge ${className}">${escapeHtml(label)}</span>`;
}

function freshnessBadge(state) {
  const badge = FRESHNESS_BADGES[state] || FRESHNESS_BADGES.current;
  return `<span class="inventory-badge is-${escapeHtml(badge.tone)}">${escapeHtml(badge.label)}</span>`;
}

function snapshotFreshnessBadge(state) {
  if (state === "aged") {
    return '<span class="inventory-badge is-aged">Delayed</span>';
  }
  if (state === "structural") {
    return '<span class="inventory-badge is-structural">Structural</span>';
  }
  return "";
}

function sparklineTrend(values) {
  if (!Array.isArray(values) || values.length < 2) {
    return "flat";
  }

  const delta = values[values.length - 1] - values[0];
  if (delta > 0) {
    return "up";
  }
  if (delta < 0) {
    return "down";
  }
  return "flat";
}

function sparklineWindowLabel(values, frequency) {
  const count = Array.isArray(values) ? values.length : 0;
  if (!count) {
    return "Trend";
  }

  const suffixByFrequency = {
    daily: "D",
    weekly: "W",
    monthly: "M",
    quarterly: "Q",
    annual: "Y",
  };

  return `${count}${suffixByFrequency[frequency] || "P"} trend`;
}

function renderSparkline(values, frequency) {
  const points = Array.isArray(values) && values.length ? values : [0, 0];
  const minValue = Math.min(...points);
  const maxValue = Math.max(...points);
  const width = 104;
  const height = 40;
  const step = width / Math.max(points.length - 1, 1);
  const span = maxValue - minValue || 1;
  const trend = sparklineTrend(points);
  const line = points
    .map((value, index) => {
      const x = Number((index * step).toFixed(2));
      const y = Number((height - ((value - minValue) / span) * height).toFixed(2));
      return `${index === 0 ? "M" : "L"}${x} ${y}`;
    })
    .join(" ");
  const area = `${line} L ${width} ${height} L 0 ${height} Z`;

  return `
    <div class="inventory-card-spark-wrap">
      <div class="inventory-card-spark-meta">${escapeHtml(sparklineWindowLabel(points, frequency))}</div>
      <svg class="inventory-card-sparkline is-${escapeHtml(trend)}" viewBox="0 0 ${width} ${height}" aria-hidden="true">
        <path class="sparkline-area" d="${area}"></path>
        <path class="sparkline-line" d="${line}"></path>
      </svg>
    </div>
  `;
}

function frequencyLabel(frequency) {
  if (!frequency) {
    return "Periodic";
  }

  return String(frequency).charAt(0).toUpperCase() + String(frequency).slice(1);
}

function unitDefinition(unit) {
  const normalized = String(unit || "").toLowerCase();
  if (normalized === "mb") {
    return { unit: "mb", label: "million barrels" };
  }
  if (normalized === "bcf") {
    return { unit: "bcf", label: "billion cubic feet" };
  }
  if (normalized === "twh") {
    return { unit: "twh", label: "terawatt-hours" };
  }
  if (normalized === "%") {
    return { unit: "%", label: "percent" };
  }
  return null;
}

function renderSectionUnitLegend(cards) {
  const items = [...new Map(cards.map((card) => [card.unit, unitDefinition(card.unit)]).filter(([, value]) => value)).values()];
  if (!items.length) {
    return "";
  }

  return `
    <div class="snapshot-section-units">
      ${items
        .map(
          (item) => `
            <span class="snapshot-section-unit">
              <strong>${escapeHtml(item.unit)}</strong>
              <span>${escapeHtml(item.label)}</span>
            </span>
          `
        )
        .join("")}
    </div>
  `;
}

function sectionAccent(sectionName) {
  if (sectionName === "Crude Oil" || sectionName === "Refined Products") {
    return "var(--color-energy)";
  }
  if (sectionName === "Natural Gas") {
    return "var(--color-natural-gas)";
  }
  if (sectionName === "Base Metals") {
    return "var(--color-metals)";
  }
  if (sectionName === "Grains") {
    return "var(--color-agri)";
  }
  if (sectionName === "Softs") {
    return "var(--color-softs)";
  }
  if (sectionName === "Precious Metals") {
    return "var(--color-precious)";
  }
  return "rgba(15, 25, 35, 0.24)";
}

function deviationHeadline(value, unit) {
  if (value == null) {
    return "Median unavailable";
  }
  if (value === 0) {
    return "At median";
  }
  return `${formatSignedValue(value, unit)} ${value < 0 ? "below" : "above"} median`;
}

function renderHistoricalRange(card, signalState) {
  const low = card.seasonalLow;
  const high = card.seasonalHigh;
  const median = card.seasonalMedian;
  const samples = card.seasonalSamples || 0;
  const hasRange =
    typeof low === "number" &&
    Number.isFinite(low) &&
    typeof high === "number" &&
    Number.isFinite(high) &&
    typeof median === "number" &&
    Number.isFinite(median);

  if (!hasRange) {
    return `
      <div class="inventory-card-range">
        <div class="inventory-card-range-head">
          <span>vs historical range</span>
          <span class="inventory-card-range-value">Context unavailable</span>
        </div>
        <div class="inventory-card-range-bar is-unavailable" aria-hidden="true"></div>
        <div class="inventory-card-range-note">Historical range is not yet available for this indicator.</div>
      </div>
    `;
  }

  const span = high - low;
  const positionFor = (value) => {
    if (!Number.isFinite(value)) {
      return 50;
    }
    if (span <= 0) {
      return 50;
    }
    return Math.max(0, Math.min(100, ((value - low) / span) * 100));
  };

  const lowLabel = samples >= 5 ? "5Y Low" : "Low";
  const highLabel = samples >= 5 ? "5Y High" : "High";

  return `
    <div class="inventory-card-range">
      <div class="inventory-card-range-head">
        <span>vs historical range</span>
        <span class="inventory-card-range-value">${escapeHtml(deviationHeadline(card.deviationAbs, card.unit))}</span>
      </div>
      <div class="inventory-card-range-bar" aria-hidden="true">
        <span class="inventory-card-range-rail"></span>
        <span class="inventory-card-range-median" style="left:${escapeHtml(String(positionFor(median)))}%;"></span>
        <span class="inventory-card-range-dot is-${escapeHtml(signalState)}" style="left:${escapeHtml(String(positionFor(card.latestValue)))}%;"></span>
      </div>
      <div class="inventory-card-range-labels">
        <span class="inventory-card-range-label">
          <span class="inventory-card-range-key">${escapeHtml(lowLabel)}</span>
          <span class="inventory-card-range-number">${escapeHtml(formatValue(low, card.unit))}</span>
        </span>
        <span class="inventory-card-range-label is-center">
          <span class="inventory-card-range-key">Median</span>
          <span class="inventory-card-range-number">${escapeHtml(formatValue(median, card.unit))}</span>
        </span>
        <span class="inventory-card-range-label is-end">
          <span class="inventory-card-range-key">${escapeHtml(highLabel)}</span>
          <span class="inventory-card-range-number">${escapeHtml(formatValue(high, card.unit))}</span>
        </span>
      </div>
      ${
        samples >= 5
          ? ""
          : '<div class="inventory-card-range-note">Historical depth is still limited for this reporting window.</div>'
      }
    </div>
  `;
}

function renderPageHead({ title, description, meta = "", breadcrumbs = "", notes = "" }) {
  return `
    <header class="inventory-page-head">
      <p class="inventory-eyebrow">InventoryWatch</p>
      <div class="inventory-title-row">
        <div class="inventory-title-block">
          <h1 class="inventory-title">${escapeHtml(title)}</h1>
          <p class="inventory-description">${escapeHtml(description)}</p>
        </div>
      </div>
      ${breadcrumbs ? `<div class="inventory-breadcrumb-row">${breadcrumbs}</div>` : ""}
      ${meta ? `<div class="inventory-status-bar">${meta}</div>` : ""}
      ${notes ? `<div class="inventory-note-grid">${notes}</div>` : ""}
    </header>
  `;
}

function filterColorForGroup(groupSlug) {
  if (groupSlug === "all") {
    return "var(--color-primary)";
  }

  if (groupSlug === "energy") {
    return "#e8a020";
  }

  if (groupSlug === "natural-gas") {
    return "#4a90d9";
  }

  if (groupSlug === "base-metals") {
    return "#64748b";
  }

  if (groupSlug === "grains") {
    return "#5ba85c";
  }

  if (groupSlug === "softs") {
    return "#2f8b84";
  }

  if (groupSlug === "precious-metals") {
    return "#c9a227";
  }

  return "var(--color-subtext)";
}

function renderLoading(view) {
  const title = view === "detail" ? "Loading indicator detail" : "Loading market snapshot";
  return `
    <section class="inventory-loading" aria-live="polite">
      <div class="inventory-loading-line is-short"></div>
      <div class="inventory-loading-line is-medium"></div>
      <div class="inventory-loading-grid">
        ${Array.from({ length: view === "detail" ? 3 : 6 })
          .map(() => '<div class="inventory-loading-card"></div>')
          .join("")}
      </div>
      <p class="inventory-description">${escapeHtml(title)}.</p>
    </section>
  `;
}

function renderError(title, copy, { retry = true } = {}) {
  return `
    <section class="inventory-error" aria-live="polite">
      <h2 class="inventory-error-title">${escapeHtml(title)}</h2>
      <p class="inventory-error-copy">${escapeHtml(copy)}</p>
      <div class="inventory-actions">
        ${retry ? '<button class="inventory-button" type="button" data-retry-inventory>Retry</button>' : ""}
        <a class="inventory-button" data-inventory-nav href="${escapeHtml(buildInventorySnapshotHref())}">Back to snapshot</a>
      </div>
    </section>
  `;
}

function renderEmpty(title, copy) {
  return `
    <section class="inventory-empty" aria-live="polite">
      <h2 class="inventory-empty-title">${escapeHtml(title)}</h2>
      <p class="inventory-empty-copy">${escapeHtml(copy)}</p>
    </section>
  `;
}

function humanizeInventoryError(error) {
  const message = error instanceof Error ? error.message : "The InventoryWatch backend did not return a valid response.";

  if (message.includes("InventoryWatch API unavailable")) {
    return "CommodityWatch could not reach the live InventoryWatch API, and no local archive fallback was available.";
  }

  if (message.includes("Failed to fetch")) {
    return "CommodityWatch could not load InventoryWatch data from this session.";
  }

  return message;
}

function createLinePath(points, xFor, yFor, accessor = (point) => point.value) {
  return points
    .filter((point) => accessor(point) != null)
    .map((point, index) => `${index === 0 ? "M" : "L"}${xFor(point.periodIndex)} ${yFor(accessor(point))}`)
    .join(" ");
}

function createAreaPath(points, xFor, yFor, topAccessor, bottomAccessor) {
  const topPoints = points.filter((point) => topAccessor(point) != null);
  const bottomPoints = [...points].reverse().filter((point) => bottomAccessor(point) != null);

  if (!topPoints.length || !bottomPoints.length) {
    return "";
  }

  const topPath = topPoints
    .map((point, index) => `${index === 0 ? "M" : "L"}${xFor(point.periodIndex)} ${yFor(topAccessor(point))}`)
    .join(" ");
  const bottomPath = bottomPoints
    .map((point) => `L${xFor(point.periodIndex)} ${yFor(bottomAccessor(point))}`)
    .join(" ");

  return `${topPath} ${bottomPath} Z`;
}

function renderSeasonalChart(data, chartSeries) {
  const seasonalPoints = data.seasonalRange.filter(
    (point) => point.p10 != null || point.p25 != null || point.p50 != null || point.p75 != null || point.p90 != null
  );
  const currentPoints = chartSeries.currentYear;
  const priorPoints = chartSeries.priorYear;

  if (!seasonalPoints.length || !currentPoints.length) {
    return renderEmpty("No seasonal range", "Seasonal bands are not available for this indicator yet.");
  }

  const width = 920;
  const height = 320;
  const pad = { top: 14, right: 22, bottom: 48, left: 64 };
  const indices = [...new Set([...seasonalPoints, ...currentPoints, ...priorPoints].map((point) => point.periodIndex))].sort(
    (left, right) => left - right
  );
  const values = [...seasonalPoints.flatMap((point) => [point.p10, point.p25, point.p50, point.p75, point.p90]), ...currentPoints.map((point) => point.value), ...priorPoints.map((point) => point.value)].filter(
    (value) => typeof value === "number" && Number.isFinite(value)
  );

  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = maxValue - minValue || Math.max(Math.abs(maxValue), 1);
  const domainMin = minValue - span * 0.08;
  const domainMax = maxValue + span * 0.08;
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;
  const xStep = innerWidth / Math.max(indices.length - 1, 1);
  const xFor = (periodIndex) => pad.left + indices.indexOf(periodIndex) * xStep;
  const yFor = (value) => pad.top + (1 - (value - domainMin) / (domainMax - domainMin || 1)) * innerHeight;
  const tickValues = Array.from({ length: 5 }, (_, index) => domainMin + ((domainMax - domainMin) / 4) * index);
  const xLabelStep = Math.max(1, Math.ceil(indices.length / 6));
  const latestPoint = currentPoints[currentPoints.length - 1];

  const outerBand = createAreaPath(seasonalPoints, xFor, yFor, (point) => point.p90, (point) => point.p10);
  const innerBand = createAreaPath(seasonalPoints, xFor, yFor, (point) => point.p75, (point) => point.p25);
  const medianPath = createLinePath(seasonalPoints, xFor, yFor, (point) => point.p50);
  const priorPath = createLinePath(priorPoints, xFor, yFor);
  const currentPath = createLinePath(currentPoints, xFor, yFor);

  return `
    <div class="inventory-chart">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Seasonal range chart">
        ${tickValues
          .map(
            (tick) => `
              <line class="inventory-chart-grid" x1="${pad.left}" y1="${yFor(tick)}" x2="${width - pad.right}" y2="${yFor(tick)}"></line>
              <text class="inventory-chart-value" x="${pad.left - 10}" y="${yFor(tick) + 4}" text-anchor="end">${escapeHtml(
                formatValue(tick, data.indicator.unit)
              )}</text>
            `
          )
          .join("")}
        <line class="inventory-chart-axis" x1="${pad.left}" y1="${height - pad.bottom}" x2="${width - pad.right}" y2="${height - pad.bottom}"></line>
        ${outerBand ? `<path class="inventory-chart-band-outer" d="${outerBand}"></path>` : ""}
        ${innerBand ? `<path class="inventory-chart-band-inner" d="${innerBand}"></path>` : ""}
        ${medianPath ? `<path class="inventory-chart-median" d="${medianPath}"></path>` : ""}
        ${priorPath ? `<path class="inventory-chart-prior" d="${priorPath}"></path>` : ""}
        ${currentPath ? `<path class="inventory-chart-current" d="${currentPath}"></path>` : ""}
        ${
          latestPoint
            ? `<circle class="inventory-chart-current-dot" cx="${xFor(latestPoint.periodIndex)}" cy="${yFor(latestPoint.value)}" r="4.8"></circle>`
            : ""
        }
        ${indices
          .filter((_, index) => index % xLabelStep === 0 || index === indices.length - 1)
          .map(
            (periodIndex) => `
              <text class="inventory-chart-label" x="${xFor(periodIndex)}" y="${height - pad.bottom + 22}" text-anchor="middle">
                ${escapeHtml(currentPoints.find((point) => point.periodIndex === periodIndex)?.label || seasonalPoints.find((point) => point.periodIndex === periodIndex)?.label || String(periodIndex))}
              </text>
            `
          )
          .join("")}
      </svg>
    </div>
  `;
}

function renderChangeChart(changeSeries, semanticMode, unit) {
  if (!changeSeries.length) {
    return renderEmpty("No change history", "Not enough history is available to compute period-over-period changes.");
  }

  const width = 920;
  const height = 260;
  const pad = { top: 12, right: 20, bottom: 44, left: 56 };
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;
  const maxAbs = Math.max(...changeSeries.map((point) => Math.abs(point.value)), 1);
  const baseline = pad.top + innerHeight / 2;
  const barStep = innerWidth / changeSeries.length;
  const barWidth = Math.max(8, barStep - 8);
  const scale = innerHeight / (maxAbs * 2);
  const labelStep = Math.max(1, Math.ceil(changeSeries.length / 6));

  return `
    <div class="inventory-chart">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Period change chart">
        <line class="inventory-bar-axis" x1="${pad.left}" y1="${baseline}" x2="${width - pad.right}" y2="${baseline}"></line>
        <text class="inventory-chart-value" x="${pad.left - 10}" y="${baseline + 4}" text-anchor="end">0</text>
        ${changeSeries
          .map((point, index) => {
            const isPositive = semanticMode === "inventory" ? point.value < 0 : point.value > 0;
            const barHeight = Math.abs(point.value) * scale;
            const x = pad.left + index * barStep + (barStep - barWidth) / 2;
            const y = point.value >= 0 ? baseline - barHeight : baseline;

            return `
              <rect
                class="${isPositive ? "inventory-bar-positive" : "inventory-bar-negative"}"
                x="${x}"
                y="${y}"
                width="${barWidth}"
                height="${barHeight}"
                rx="4"
                ry="4"
              ></rect>
              ${
                index % labelStep === 0 || index === changeSeries.length - 1
                  ? `<text class="inventory-chart-label" x="${x + barWidth / 2}" y="${height - pad.bottom + 22}" text-anchor="middle">${escapeHtml(point.label)}</text>`
                  : ""
              }
            `;
          })
          .join("")}
        <text class="inventory-chart-value" x="${pad.left - 10}" y="${pad.top + 4}" text-anchor="end">${escapeHtml(
          formatSignedValue(maxAbs, unit)
        )}</text>
        <text class="inventory-chart-value" x="${pad.left - 10}" y="${height - pad.bottom - 4}" text-anchor="end">${escapeHtml(
          formatSignedValue(-maxAbs, unit)
        )}</text>
      </svg>
    </div>
  `;
}

function renderRecentReleasesTable(rows, unit) {
  return `
    <div class="inventory-table-wrap">
      <table class="inventory-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Value</th>
            <th>Change</th>
            <th>% Change</th>
            <th>vs 5Y Median</th>
            <th>Percentile Rank</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  <td class="is-mono">${escapeHtml(isoDate(row.date))}</td>
                  <td class="is-strong">${escapeHtml(formatValue(row.value, unit))}</td>
                  <td class="is-mono">${escapeHtml(formatSignedValue(row.change, unit))}</td>
                  <td class="is-mono">${escapeHtml(formatPercent(row.percentChange))}</td>
                  <td class="is-mono">${escapeHtml(formatSignedValue(row.vsMedian, unit))}</td>
                  <td>${escapeHtml(row.percentileRankLabel)}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderSnapshotCard(card, currentGroupSlug) {
  const groupSlug = commodityGroupForCode(card.commodityCode);
  const routeGroup = groupSlug === "all" ? currentGroupSlug : groupSlug;
  const href = buildInventoryDetailHref(routeGroup || "all", card.indicatorId);
  const descriptor = snapshotSignalDescriptor(card);
  const changeTone = card.changeAbs > 0 ? "positive" : card.changeAbs < 0 ? "negative" : "flat";
  const cadence = frequencyLabel(card.frequency);

  return `
    <a
      class="inventory-card is-${escapeHtml(descriptor.state)}"
      data-inventory-nav
      href="${escapeHtml(href)}"
    >
      <div class="inventory-card-head">
        <div>
          <p class="inventory-card-kicker">${escapeHtml(card.snapshotGroup)}</p>
          <h2 class="inventory-card-title">${escapeHtml(card.name)}</h2>
          <p class="inventory-card-signal is-${escapeHtml(descriptor.state)}">${escapeHtml(descriptor.label)}</p>
        </div>
        <div class="inventory-card-badges">
          ${snapshotFreshnessBadge(card.freshness)}
        </div>
      </div>
      <div class="inventory-card-main">
        <p class="inventory-card-value">${escapeHtml(formatValue(card.latestValue, card.unit))}</p>
        <p class="inventory-card-change">
          <span class="inventory-card-change-label">vs prior</span>
          <span class="inventory-card-change-value is-${escapeHtml(changeTone)}">${escapeHtml(
            formatSignedValue(card.changeAbs, card.unit)
          )}</span>
        </p>
      </div>
      ${renderHistoricalRange(card, descriptor.state)}
      <div class="inventory-card-foot">
        <div class="inventory-card-source">
          <span class="inventory-card-source-name">${escapeHtml(card.sourceLabel)}</span>
          <span class="inventory-card-source-time">${escapeHtml(cadence)} <span class="inventory-inline-sep">·</span> ${escapeHtml(formatUtcTimestamp(card.lastUpdatedAt))}</span>
        </div>
        ${renderSparkline(card.sparkline, card.frequency)}
      </div>
    </a>
  `;
}

function renderSnapshotView(route, snapshot) {
  const cards = filterCardsByCommodityGroup(snapshot.cards, route.groupSlug);
  const title = route.groupSlug === "all" ? "Market snapshot" : COMMODITY_GROUPS.find((group) => group.slug === route.groupSlug)?.label || "InventoryWatch";
  const description =
    route.groupSlug === "all"
      ? `Cross-commodity storage and stocks coverage built for fast balance checks. ${cards.length} indicators in the current snapshot.`
      : `${groupDescriptionFor(route.groupSlug)} ${cards.length ? `· ${cards.length} indicators in the current snapshot.` : ""}`;
  const grouped = groupCardsForSnapshot(cards);
  const sections = snapshotSectionEntries(grouped);

  setDocumentTitle(route.groupSlug === "all" ? "CommodityWatch | InventoryWatch" : `CommodityWatch | InventoryWatch | ${title}`);

  return `
    <div class="inventory-stage">
      ${renderPageHead({
        title,
        description,
        meta: `
          <div class="inventory-status-meta">
            <span class="inventory-status-item"><strong>Snapshot</strong> <span class="is-mono">${escapeHtml(formatUtcTimestamp(snapshot.generatedAt))}</span></span>
            <span class="inventory-inline-sep">·</span>
            <span class="inventory-status-item"><strong>Coverage</strong> ${escapeHtml(String(cards.length))} indicators</span>
          </div>
        `,
        notes: `
          <div class="inventory-note-card">
            <div class="inventory-note-label">Update cadence</div>
            <p class="inventory-note-copy">Snapshot values follow source publication schedules and reflect the latest reported period, not real-time storage levels.</p>
          </div>
          <div class="inventory-note-card">
            <div class="inventory-note-label">Signal basis</div>
            <p class="inventory-note-copy">Signal states compare the latest inventory level with its historical percentile range for the same reporting window.</p>
          </div>
        `,
      })}
      ${
        !cards.length
          ? renderEmpty("No data available", `No inventory indicators are currently available for ${title}.`)
          : `
            <div class="snapshot-sections">
              ${sections
                .map(
                  ([sectionName, sectionCards]) => `
                    <section class="snapshot-section" style="--section-accent:${escapeHtml(sectionAccent(sectionName))};">
                      <div class="snapshot-section-head">
                        <div class="snapshot-section-title-wrap">
                          <h2 class="snapshot-section-title">${escapeHtml(sectionName)}</h2>
                          <div class="snapshot-section-subline">
                            <div class="snapshot-section-meta">${escapeHtml(sectionCards.length)} indicator${sectionCards.length === 1 ? "" : "s"}</div>
                            ${renderSectionUnitLegend(sectionCards)}
                          </div>
                        </div>
                      </div>
                      <div class="snapshot-grid">
                        ${sectionCards.map((card) => renderSnapshotCard(card, route.groupSlug)).join("")}
                      </div>
                    </section>
                  `
                )
                .join("")}
            </div>
          `
      }
    </div>
  `;
}

function renderDetailView(route, data, latest, alternateData, alternateUnavailable) {
  const resolvedGroupSlug = commodityGroupForCode(data.indicator.commodityCode) || route.groupSlug;
  const activeGroupSlug = resolvedGroupSlug === "all" ? route.groupSlug : resolvedGroupSlug;
  const activeData = excludeYear2020 && alternateData?.seasonalRange?.length ? alternateData : data;
  const chartSeries = buildSeasonalSeries(activeData);
  const changeSeries = buildChangeBarSeries(activeData.series);
  const recentRows = buildRecentReleaseRows(activeData);
  const latestValue = latest?.latest?.value ?? activeData.series[activeData.series.length - 1]?.value ?? null;
  const latestTimestamp =
    latest?.latest?.releaseDate ??
    activeData.metadata.latestReleaseAt ??
    activeData.series[activeData.series.length - 1]?.releaseDate ??
    activeData.series[activeData.series.length - 1]?.periodEndAt;
  const latestPeriodDate = latest?.latest?.periodEndAt ?? activeData.series[activeData.series.length - 1]?.periodEndAt;
  const latestSeasonalPoint =
    latestPeriodDate && activeData.seasonalRange.length
      ? seasonalPointForLatest(latestPeriodDate, activeData.seasonalRange, activeData.indicator.frequency)
      : null;
  const alertKind =
    latestValue != null && latestSeasonalPoint ? alertKindFromSeasonal(latestValue, latestSeasonalPoint) : null;
  const percentileLabel =
    latestValue != null && latestSeasonalPoint ? percentileBracketLabel(latestValue, latestSeasonalPoint) : "Unavailable";
  const detailFreshness = freshnessFor(activeData.indicator.frequency, latestTimestamp);
  const detailTitle = data.indicator.name;
  const semanticMode = semanticModeForCommodity(data.indicator.commodityCode);
  const groupLabel = COMMODITY_GROUPS.find((group) => group.slug === activeGroupSlug)?.label || "Inventory";

  setDocumentTitle(`CommodityWatch | InventoryWatch | ${detailTitle}`);

  return `
    <div class="inventory-stage">
      ${alternateUnavailable ? '<div class="inventory-banner">Exclude-2020 seasonal profiles are not available for this indicator. Showing the standard seasonal range instead.</div>' : ""}
      ${renderPageHead({
        title: detailTitle,
        description: data.indicator.description || "Seasonal range, period change, and release history.",
        breadcrumbs: `
          <div class="inventory-breadcrumbs">
            <a class="inventory-breadcrumb-link" data-inventory-nav href="${escapeHtml(buildInventorySnapshotHref())}">InventoryWatch</a>
            <span class="inventory-inline-sep">/</span>
            <a class="inventory-breadcrumb-link" data-inventory-nav href="${escapeHtml(buildInventorySnapshotHref(activeGroupSlug))}">${escapeHtml(groupLabel)}</a>
            <span class="inventory-inline-sep">/</span>
            <span class="inventory-breadcrumb-current">${escapeHtml(detailTitle)}</span>
          </div>
        `,
        meta: `
          <div class="inventory-inline-meta">
            <span><strong>Source</strong> ${escapeHtml(activeData.metadata.sourceLabel || "CommodityWatch API")}</span>
            <span class="inventory-inline-sep">·</span>
            <span><strong>Updated</strong> ${escapeHtml(formatUtcTimestamp(latestTimestamp))}</span>
          </div>
        `,
      })}
      <div class="inventory-detail-grid">
        <div class="inventory-stack">
          <section class="inventory-panel">
            <div class="inventory-panel-head">
              <div>
                <h2 class="inventory-panel-title">Seasonal range</h2>
                <p class="inventory-panel-copy">Current year against the 5-year seasonal percentile bands.</p>
              </div>
              <label class="inventory-toggle">
                <input type="checkbox" data-exclude-year-toggle ${excludeYear2020 ? "checked" : ""} />
                <span>Exclude 2020</span>
              </label>
            </div>
            <div class="inventory-panel-body">
              ${renderSeasonalChart(activeData, chartSeries)}
            </div>
          </section>

          <section class="inventory-panel">
            <div class="inventory-panel-head">
              <div>
                <h2 class="inventory-panel-title">Period change</h2>
                <p class="inventory-panel-copy">Recent builds and draws with inventory-aware color conventions.</p>
              </div>
            </div>
            <div class="inventory-panel-body">
              ${renderChangeChart(changeSeries, semanticMode, activeData.indicator.unit)}
            </div>
          </section>

          <section class="inventory-panel">
            <div class="inventory-panel-head">
              <div>
                <h2 class="inventory-panel-title">Recent releases</h2>
                <p class="inventory-panel-copy">Latest published observations with change and seasonal context.</p>
              </div>
            </div>
            <div class="inventory-panel-body">
              ${renderRecentReleasesTable(recentRows, activeData.indicator.unit)}
            </div>
          </section>
        </div>

        <aside class="inventory-stack">
          <section class="inventory-panel">
            <div class="inventory-summary">
              <div class="inventory-card-badges">
                ${freshnessBadge(detailFreshness)}
                ${alertBadge(alertKind)}
                <span class="inventory-badge is-structural">${escapeHtml(percentileLabel)}</span>
              </div>
              <div class="inventory-summary-value">${escapeHtml(formatValue(latestValue, activeData.indicator.unit))}</div>
              <div class="inventory-summary-change">${escapeHtml(formatSignedValue(latest?.latest?.changeFromPriorAbs, activeData.indicator.unit))} vs prior period</div>
              <div class="inventory-summary-grid">
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Release date</div>
                  <div class="inventory-summary-definition is-mono">${escapeHtml(formatUtcTimestamp(latest?.latest?.releaseDate || latestPeriodDate))}</div>
                </div>
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">vs 5Y median</div>
                  <div class="inventory-summary-definition is-mono">${escapeHtml(
                    formatSignedValue(latest?.latest?.deviationFromSeasonalAbs, activeData.indicator.unit)
                  )}</div>
                </div>
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Frequency</div>
                  <div class="inventory-summary-definition is-mono">${escapeHtml(activeData.indicator.frequency.toUpperCase())}</div>
                </div>
              </div>
            </div>
          </section>

          <section class="inventory-panel">
            <div class="inventory-summary">
              <h2 class="inventory-panel-title">Source</h2>
              <div class="inventory-summary-grid">
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Publisher</div>
                  <div class="inventory-summary-definition">${escapeHtml(activeData.metadata.sourceLabel || "CommodityWatch API")}</div>
                </div>
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Link</div>
                  <div class="inventory-summary-definition">
                    ${
                      activeData.metadata.sourceUrl
                        ? `<a href="${escapeHtml(activeData.metadata.sourceUrl)}" target="_blank" rel="noreferrer noopener">Open source</a>`
                        : "No source URL"
                    }
                  </div>
                </div>
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Commodity</div>
                  <div class="inventory-summary-definition">${escapeHtml(groupLabel)}</div>
                </div>
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Last updated</div>
                  <div class="inventory-summary-definition is-mono">${escapeHtml(formatUtcTimestamp(latestTimestamp))}</div>
                </div>
              </div>
            </div>
          </section>
        </aside>
      </div>
    </div>
  `;
}

function renderFilterBar(route) {
  const selectedGroup = route.groupSlug || "all";

  filterRoot.innerHTML = `
    <div class="filter-wrap inventory-filter-wrap">
      <nav class="filter-bar inventory-filter-bar" aria-label="Inventory commodity groups">
        <div class="inventory-group-row">
          ${COMMODITY_GROUPS.map((group) => {
            const href = buildInventorySnapshotHref(group.slug);
            const isSelected = group.slug === selectedGroup;
            const color = filterColorForGroup(group.slug);
            return `
              <a
                class="filter-pill inventory-filter-pill${isSelected ? " is-selected" : ""}"
                data-group="${escapeHtml(group.slug)}"
                data-inventory-nav
                href="${escapeHtml(href)}"
                ${isSelected ? 'aria-current="page"' : ""}
                style="--filter-pill-color:${escapeHtml(color)};"
              >
                ${escapeHtml(group.label)}
              </a>
            `;
          }).join("")}
        </div>
      </nav>
    </div>
  `;
}

async function getSnapshot() {
  if (snapshotCache.value) {
    return snapshotCache.value;
  }

  if (!snapshotCache.promise) {
    snapshotCache.promise = fetchInventorySnapshot({ includeSparklines: true, limit: 100 })
      .then((payload) => {
        snapshotCache.value = payload;
        snapshotCache.promise = null;
        return payload;
      })
      .catch((error) => {
        snapshotCache.promise = null;
        throw error;
      });
  }

  return snapshotCache.promise;
}

function alternateSeasonalProfile(groupSlug) {
  return groupSlug === "natural-gas" ? "inventorywatch_5y_daily_ex_2020" : "inventorywatch_5y_ex_2020";
}

async function getDetailData(indicatorId, groupSlug, { alternate = false } = {}) {
  const cache = alternate ? alternateDataCache : detailDataCache;
  const cacheKey = alternate ? `${groupSlug}:${indicatorId}` : indicatorId;

  if (cache.has(cacheKey)) {
    return cache.get(cacheKey);
  }

  const params = {
    includeSeasonal: true,
    limitPoints: 2500,
  };

  if (alternate) {
    params.seasonalProfile = alternateSeasonalProfile(groupSlug);
  }

  const promise = fetchIndicatorData(indicatorId, params)
    .then((payload) => {
      cache.set(cacheKey, payload);
      return payload;
    })
    .catch((error) => {
      cache.delete(cacheKey);
      throw error;
    });

  cache.set(cacheKey, promise);
  return promise;
}

async function getLatest(indicatorId) {
  if (latestCache.has(indicatorId)) {
    return latestCache.get(indicatorId);
  }

  const promise = fetchIndicatorLatest(indicatorId)
    .catch(() => null)
    .then((payload) => {
      latestCache.set(indicatorId, payload);
      return payload;
    });

  latestCache.set(indicatorId, promise);
  return promise;
}

function invalidateCurrentRouteCache(route) {
  if (route.view === "detail" && route.indicatorId) {
    detailDataCache.delete(route.indicatorId);
    alternateDataCache.delete(`${route.groupSlug}:${route.indicatorId}`);
    latestCache.delete(route.indicatorId);
    return;
  }

  snapshotCache.value = null;
  snapshotCache.promise = null;
}

async function renderRoute() {
  const route = currentRoute();
  const requestId = ++renderRequestId;
  const isCurrent = () => requestId === renderRequestId;

  renderFilterBar(route);
  appRoot.innerHTML = renderLoading(route.view);

  if (route.view === "not-found") {
    setDocumentTitle("CommodityWatch | InventoryWatch");
    appRoot.innerHTML = renderError("Inventory route not found", route.reason || "The requested InventoryWatch route is invalid.", {
      retry: false,
    });
    return;
  }

  try {
    if (route.view === "detail" && route.indicatorId) {
      const [data, latest, alternateData] = await Promise.all([
        getDetailData(route.indicatorId, route.groupSlug),
        getLatest(route.indicatorId),
        excludeYear2020 ? getDetailData(route.indicatorId, route.groupSlug, { alternate: true }).catch(() => null) : Promise.resolve(null),
      ]);

      if (!isCurrent()) {
        return;
      }

      if (!data?.series?.length) {
        appRoot.innerHTML = renderEmpty(
          "No data available",
          "This indicator route is live, but the backend has not published any observations yet."
        );
        return;
      }

      appRoot.innerHTML = renderDetailView(route, data, latest, alternateData, Boolean(excludeYear2020 && !alternateData?.seasonalRange?.length));
      return;
    }

    const snapshot = await getSnapshot();
    if (!isCurrent()) {
      return;
    }

    appRoot.innerHTML = renderSnapshotView(route, snapshot);
  } catch (error) {
    if (!isCurrent()) {
      return;
    }

    setDocumentTitle("CommodityWatch | InventoryWatch");
    appRoot.innerHTML = renderError(
      "InventoryWatch unavailable",
      humanizeInventoryError(error)
    );
  }
}

function navigate(href) {
  const nextPath = normalizePath(new URL(href, window.location.origin).pathname);
  const currentPath = normalizePath(window.location.pathname);
  if (nextPath === currentPath) {
    return;
  }

  excludeYear2020 = false;
  window.history.pushState({}, "", nextPath);
  window.scrollTo({ top: 0, behavior: "auto" });
  renderRoute();
}

document.addEventListener("click", (event) => {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) {
    return;
  }

  const retryButton = target.closest("[data-retry-inventory]");
  if (retryButton) {
    invalidateCurrentRouteCache(currentRoute());
    renderRoute();
    return;
  }

  const link = target.closest("a[data-inventory-nav]");
  if (!link) {
    return;
  }

  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
    return;
  }

  event.preventDefault();
  navigate(link.href);
});

document.addEventListener("change", (event) => {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) {
    return;
  }

  const toggle = target.closest("[data-exclude-year-toggle]");
  if (!toggle) {
    return;
  }

  excludeYear2020 = toggle instanceof HTMLInputElement ? toggle.checked : toggle.getAttribute("checked") !== null;
  renderRoute();
});

window.addEventListener("popstate", () => {
  excludeYear2020 = false;
  renderRoute();
});

if (toTopButton) {
  const syncToTopVisibility = () => {
    toTopButton.classList.toggle("visible", window.scrollY > 360);
  };

  window.addEventListener("scroll", syncToTopVisibility, { passive: true });
  syncToTopVisibility();

  toTopButton.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });
}

renderRoute();
