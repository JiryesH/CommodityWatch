import {
  COMMODITY_GROUPS,
  FRESHNESS_BADGES,
  commodityGroupForCode,
  filterPillColorForGroup,
  freshnessFor,
  formatPercent,
  formatSignedValue,
  formatUtcTimestamp,
  formatValue,
  groupDescriptionFor,
  isoDate,
  isGroupPopulated,
  nextReleaseStatus,
  semanticModeForCommodity,
} from "./catalog.js";
import { fetchIndicatorData, fetchIndicatorLatest, fetchInventorySnapshot } from "./api-client.js";
import {
  areAllInventoryGroupsSelected,
  buildChangeBarSeries,
  buildRecentReleaseRows,
  buildSeasonalSeries,
  buildYtdChangeStats,
  changeToneForValue,
  filterCardsByCommodityGroups,
  groupCardsForSnapshot,
  percentileBracketLabel,
  seasonalPointForLatest,
  alertToneFromAlerts,
  snapshotSignalDescriptor,
  snapshotSectionEntries,
  searchSnapshotCards,
  toggleInventoryGroupSelection,
} from "./model.js";
import {
  buildInventoryDetailHref,
  buildInventorySnapshotHref,
  parseInventoryRoute,
} from "./router.js";

const filterRoot = document.getElementById("inventory-filter-root");
const appRoot = document.getElementById("inventory-root");
const toTopButton = document.getElementById("to-top-btn");
const navSearch = document.getElementById("nav-search");
const searchToggle = document.getElementById("search-toggle");
const searchInput = document.getElementById("inventory-search");

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
let searchQuery = "";
let searchOpen = false;
let detailReturnState = null;
let pendingSnapshotRestore = null;
let snapshotGroupFilters = null;
let snapshotFilterRouteKey = null;

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

function compactSearchQuery(query) {
  return String(query ?? "")
    .trim()
    .replace(/\s+/g, " ");
}

function hasActiveSearch() {
  return searchQuery.length > 0;
}

function eventPathIncludes(event, element) {
  if (!element) {
    return false;
  }

  if (typeof event.composedPath === "function") {
    return event.composedPath().includes(element);
  }

  return element.contains(event.target);
}

function setDocumentTitle(title) {
  document.title = title;
}

function selectorEscape(value) {
  if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
    return CSS.escape(String(value));
  }

  return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function stickyViewportOffset() {
  const navHeight = document.querySelector(".nav")?.offsetHeight || 0;
  const filterHeight = filterRoot.querySelector(".filter-wrap")?.offsetHeight || 0;
  return navHeight + filterHeight + 20;
}

function resolveBackLabel() {
  return detailReturnState?.mode === "search" ? "Back to results" : "Back to snapshot";
}

function renderBackLink(label = resolveBackLabel()) {
  return `
    <div class="inventory-back-row">
      <a class="inventory-back-link" data-inventory-back href="${escapeHtml(buildInventorySnapshotHref())}">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M15 18l-6-6 6-6"></path>
          <path d="M21 12H9"></path>
        </svg>
        <span>${escapeHtml(label)}</span>
      </a>
    </div>
  `;
}

function captureDetailReturnState({ indicatorId } = {}) {
  detailReturnState = {
    indicatorId: indicatorId || null,
    mode: hasActiveSearch() ? "search" : "snapshot",
    query: searchQuery,
    scrollY: window.scrollY,
  };
}

function prepareSnapshotRestore() {
  pendingSnapshotRestore = detailReturnState
    ? { ...detailReturnState }
    : {
        indicatorId: null,
        mode: "snapshot",
        query: "",
        scrollY: 0,
      };

  snapshotGroupFilters = null;
  snapshotFilterRouteKey = null;
  searchQuery = pendingSnapshotRestore.mode === "search" ? pendingSnapshotRestore.query || "" : "";
  renderSearchUi();
}

function restoreSnapshotPosition() {
  if (!pendingSnapshotRestore) {
    return;
  }

  const restoreState = pendingSnapshotRestore;
  pendingSnapshotRestore = null;

  window.requestAnimationFrame(() => {
    const target =
      restoreState.indicatorId != null
        ? appRoot.querySelector(`[data-indicator-id="${selectorEscape(restoreState.indicatorId)}"]`)
        : null;

    if (target) {
      const top = target.getBoundingClientRect().top + window.scrollY - stickyViewportOffset();
      window.scrollTo({ top: Math.max(0, top), behavior: "auto" });
      return;
    }

    if (typeof restoreState.scrollY === "number" && Number.isFinite(restoreState.scrollY)) {
      window.scrollTo({ top: Math.max(0, restoreState.scrollY), behavior: "auto" });
    }
  });
}

function availableSnapshotGroupSlugs(cards = null) {
  return COMMODITY_GROUPS.filter((group) => {
    if (group.slug === "all") {
      return false;
    }

    if (Array.isArray(cards)) {
      return isGroupPopulated(group.slug, cards);
    }

    return !["coal", "fertilisers"].includes(group.slug);
  }).map((group) => group.slug);
}

function normalizeSnapshotGroupFilters(selectedSlugs, availableSlugs) {
  const available = Array.isArray(availableSlugs) ? availableSlugs.filter(Boolean) : [];
  if (!available.length) {
    return [];
  }

  const filtered = (Array.isArray(selectedSlugs) ? selectedSlugs : []).filter((slug) => available.includes(slug));
  if (!filtered.length || filtered.length === available.length) {
    return [...available];
  }

  return available.filter((slug) => filtered.includes(slug));
}

function snapshotGroupSelectionState(route, cards = null) {
  const available = availableSnapshotGroupSlugs(cards);
  const routeKey = normalizePath(window.location.pathname);

  if (route.view === "snapshot") {
    if (!snapshotGroupFilters || snapshotFilterRouteKey !== routeKey) {
      snapshotGroupFilters =
        route.groupSlug !== "all" && available.includes(route.groupSlug) ? [route.groupSlug] : [...available];
      snapshotFilterRouteKey = routeKey;
    } else {
      snapshotGroupFilters = normalizeSnapshotGroupFilters(snapshotGroupFilters, available);
    }

    return {
      selected: snapshotGroupFilters,
      available,
      allSelected: areAllInventoryGroupsSelected(snapshotGroupFilters, available),
    };
  }

  const selected =
    route.groupSlug !== "all" && available.includes(route.groupSlug)
      ? [route.groupSlug]
      : [...available];

  return {
    selected,
    available,
    allSelected: areAllInventoryGroupsSelected(selected, available),
  };
}

function snapshotScopeLabel(selectedGroups, availableGroups) {
  const allSelected = areAllInventoryGroupsSelected(selectedGroups, availableGroups);
  if (allSelected || selectedGroups.length !== 1) {
    return "Market snapshot";
  }

  return COMMODITY_GROUPS.find((group) => group.slug === selectedGroups[0])?.label || "Market snapshot";
}

function renderSearchUi() {
  if (!navSearch || !searchToggle || !searchInput) {
    return;
  }

  navSearch.classList.toggle("open", searchOpen);
  navSearch.classList.toggle("has-query", hasActiveSearch());
  searchToggle.setAttribute("aria-expanded", String(searchOpen));

  if (searchInput.value !== searchQuery) {
    searchInput.value = searchQuery;
  }
}

function alertBadge(alert) {
  if (!alert?.label) {
    return "";
  }

  const toneClassByAlert = {
    tight: "is-alert-tight",
    ample: "is-alert-ample",
    watch: "is-alert-watch",
    cool: "is-alert-cool",
  };

  return `<span class="inventory-badge ${toneClassByAlert[alert.tone] || "is-structural"}">${escapeHtml(alert.label)}</span>`;
}

function renderAlertBadges(alerts = []) {
  return alerts.map((alert) => alertBadge(alert)).join("");
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

function releaseCountdownMarkup(indicatorLike) {
  const release = nextReleaseStatus(indicatorLike);
  return `<span class="inventory-release inventory-release-${escapeHtml(release.state)}">${escapeHtml(release.label)}</span>`;
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

function renderPageHead({ title, description, meta = "", breadcrumbs = "", notes = "", lead = "" }) {
  return `
    <header class="inventory-page-head">
      ${lead}
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

function sourceLabelMarkup(label, href, className = "inventory-inline-link") {
  if (!href) {
    return escapeHtml(label);
  }

  return `<a class="${escapeHtml(className)}" href="${escapeHtml(href)}" target="_blank" rel="noreferrer noopener">${escapeHtml(label)}</a>`;
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

function renderDetailUnavailable(route, data, title, copy) {
  const resolvedGroupSlug = data?.indicator?.commodityCode ? commodityGroupForCode(data.indicator.commodityCode) : route.groupSlug;
  const activeGroupSlug = resolvedGroupSlug && resolvedGroupSlug !== "all" ? resolvedGroupSlug : route.groupSlug;
  const groupLabel = COMMODITY_GROUPS.find((group) => group.slug === activeGroupSlug)?.label || "Inventory";
  const detailTitle = data?.indicator?.name || route.indicatorId || "Inventory indicator";

  setDocumentTitle(`CommodityWatch | InventoryWatch | ${detailTitle}`);

  return `
    <div class="inventory-stage">
      ${renderPageHead({
        title: detailTitle,
        description: data?.indicator?.description || "Indicator route is live, but the backend has not published observations yet.",
        lead: renderBackLink(),
        breadcrumbs: `
          <div class="inventory-breadcrumbs">
            <a class="inventory-breadcrumb-link" data-inventory-nav href="${escapeHtml(buildInventorySnapshotHref())}">InventoryWatch</a>
            <span class="inventory-inline-sep">/</span>
            <span class="inventory-breadcrumb-current">${escapeHtml(groupLabel)}</span>
          </div>
        `,
      })}
      ${renderEmpty(title, copy)}
    </div>
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
  const averagePath = changeSeries
    .filter((point) => point.seasonalAverageChange != null)
    .map((point, index) => {
      const x = pad.left + index * barStep + barStep / 2;
      const y = baseline - point.seasonalAverageChange * scale;
      return `${index === 0 ? "M" : "L"}${x} ${y}`;
    })
    .join(" ");

  return `
    <div class="inventory-chart">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Period change chart">
        <line class="inventory-bar-axis" x1="${pad.left}" y1="${baseline}" x2="${width - pad.right}" y2="${baseline}"></line>
        ${averagePath ? `<path class="inventory-chart-average" d="${averagePath}"></path>` : ""}
        <text class="inventory-chart-value" x="${pad.left - 10}" y="${baseline + 4}" text-anchor="end">0</text>
        ${changeSeries
          .map((point, index) => {
            const toneClass = changeToneForValue(point.value, semanticMode);
            const barHeight = Math.abs(point.value) * scale;
            const x = pad.left + index * barStep + (barStep - barWidth) / 2;
            const y = point.value >= 0 ? baseline - barHeight : baseline;

            return `
              <rect
                class="${
                  toneClass === "positive"
                    ? "inventory-bar-positive"
                    : toneClass === "negative"
                      ? "inventory-bar-negative"
                      : "inventory-bar-neutral"
                }"
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
                  <td class="is-mono">${row.revisionFlag ? '<span class="inventory-revision-flag" title="This observation was revised by a later source release">~</span>' : ""}${escapeHtml(isoDate(row.date))}</td>
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
  const changeTone = changeToneForValue(card.changeAbs, card.semanticMode);
  const cadence = frequencyLabel(card.frequency);

  return `
    <a
      class="inventory-card is-${escapeHtml(descriptor.state)}"
      data-inventory-nav
      data-indicator-id="${escapeHtml(card.indicatorId)}"
      href="${escapeHtml(href)}"
    >
      <div class="inventory-card-head">
        <div>
          <p class="inventory-card-kicker">${escapeHtml(card.snapshotGroup)}</p>
          <h2 class="inventory-card-title">${escapeHtml(card.name)}</h2>
          ${card.alerts?.length ? `<div class="inventory-inline-badges">${renderAlertBadges(card.alerts)}</div>` : ""}
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
          <span class="inventory-card-source-time">${releaseCountdownMarkup(card)}</span>
        </div>
        ${renderSparkline(card.sparkline, card.frequency)}
      </div>
    </a>
  `;
}

function renderSearchSummary(route, matchingCards, totalCards) {
  const { selected, available } = snapshotGroupSelectionState(route, snapshotCache.value?.cards ?? null);
  const scopeLabel = snapshotScopeLabel(selected, available);

  return `
    <section class="inventory-search-summary">
      <div class="inventory-search-copy">
        <p class="inventory-search-label">Indicator search</p>
        <h2 class="inventory-search-title">${escapeHtml(searchQuery)}</h2>
        <p class="inventory-search-description">
          ${escapeHtml(String(matchingCards.length))} indicator${matchingCards.length === 1 ? "" : "s"} match in ${escapeHtml(scopeLabel)}
          ${totalCards !== matchingCards.length ? `<span class="inventory-search-total">out of ${escapeHtml(String(totalCards))}</span>` : ""}
        </p>
      </div>
      <div class="inventory-actions">
        <button class="inventory-button" type="button" data-clear-search>Clear search</button>
      </div>
    </section>
  `;
}

function renderSnapshotView(route, snapshot) {
  const { selected, available, allSelected } = snapshotGroupSelectionState(route, snapshot.cards);
  const scopedCards = filterCardsByCommodityGroups(snapshot.cards, selected);
  const cards = hasActiveSearch() ? searchSnapshotCards(scopedCards, searchQuery) : scopedCards;
  const selectedLabels = selected
    .map((slug) => COMMODITY_GROUPS.find((group) => group.slug === slug)?.label)
    .filter(Boolean);
  const title = allSelected || selected.length !== 1
    ? "Market snapshot"
    : COMMODITY_GROUPS.find((group) => group.slug === selected[0])?.label || "Market snapshot";
  const description =
    allSelected || selected.length !== 1
      ? `Cross-commodity storage and stocks coverage built for fast balance checks. ${scopedCards.length} indicators in the current snapshot${
          selectedLabels.length && !allSelected ? ` across ${selectedLabels.join(", ")}.` : "."
        }`
      : `${groupDescriptionFor(selected[0])} ${scopedCards.length ? `· ${scopedCards.length} indicators in the current snapshot.` : ""}`;
  const grouped = groupCardsForSnapshot(cards);
  const sections = snapshotSectionEntries(grouped);

  setDocumentTitle(title === "Market snapshot" ? "CommodityWatch | InventoryWatch" : `CommodityWatch | InventoryWatch | ${title}`);

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
      ${hasActiveSearch() ? renderSearchSummary(route, cards, scopedCards.length) : ""}
      ${
        !cards.length
          ? renderEmpty(
              hasActiveSearch() ? "No matches found" : "No data available",
              hasActiveSearch()
                ? `No inventory indicators match "${searchQuery}" in ${title}.`
                : `No inventory indicators are currently available for ${title}.`
            )
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
                          </div>
                        </div>
                      </div>
                      <div class="snapshot-grid">
                        ${sectionCards.map((card) => renderSnapshotCard(card, selected.length === 1 ? selected[0] : "all")).join("")}
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
  const changeSeries = buildChangeBarSeries(activeData);
  const recentRows = buildRecentReleaseRows(activeData);
  const ytdStats = buildYtdChangeStats(activeData);
  const latestValue = latest?.latest?.value ?? activeData.series[activeData.series.length - 1]?.value ?? null;
  const latestTimestamp =
    latest?.latest?.releaseDate ??
    activeData.metadata.latestReleaseAt ??
    activeData.series[activeData.series.length - 1]?.releaseDate ??
    activeData.series[activeData.series.length - 1]?.periodEndAt;
  const latestPeriodDate = latest?.latest?.periodEndAt ?? activeData.series[activeData.series.length - 1]?.periodEndAt;
  const latestSeasonalPoint =
    latestPeriodDate && activeData.seasonalRange.length
      ? seasonalPointForLatest(latestPeriodDate, activeData.seasonalRange, activeData.indicator)
      : null;
  const percentileLabel =
    latestValue != null && latestSeasonalPoint ? percentileBracketLabel(latestValue, latestSeasonalPoint) : "Unavailable";
  const detailFreshness = freshnessFor(activeData.indicator.frequency, latestTimestamp);
  const detailTitle = data.indicator.name;
  const semanticMode = semanticModeForCommodity(data.indicator.commodityCode);
  const groupLabel = COMMODITY_GROUPS.find((group) => group.slug === activeGroupSlug)?.label || "Inventory";
  const releaseStatus = nextReleaseStatus({
    code: activeData.indicator.code,
    commodityCode: activeData.indicator.commodityCode,
    releaseSchedule: activeData.indicator.releaseSchedule,
    latestReleaseDate: latest?.latest?.releaseDate || activeData.metadata.latestReleaseAt,
    lastUpdatedAt: latestTimestamp,
  });
  const alerts = latest?.latest?.alerts || [];
  const summaryTone = alertToneFromAlerts(alerts);
  const sourceLabel = activeData.metadata.sourceLabel || "CommodityWatch API";
  const sourceUrl = activeData.metadata.sourceUrl;

  setDocumentTitle(`CommodityWatch | InventoryWatch | ${detailTitle}`);

  return `
    <div class="inventory-stage">
      ${alternateUnavailable ? '<div class="inventory-banner">Exclude-2020 seasonal profiles are not available for this indicator. Showing the standard seasonal range instead.</div>' : ""}
      ${renderPageHead({
        title: detailTitle,
        description: data.indicator.description || "Seasonal range, period change, and release history.",
        lead: renderBackLink(),
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
            <span><strong>Source</strong> ${sourceLabelMarkup(sourceLabel, sourceUrl)}</span>
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
                <p class="inventory-panel-copy">Recent builds and draws with the seasonal average change overlay.</p>
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
            <div class="inventory-summary is-${escapeHtml(summaryTone)}">
              <div class="inventory-card-badges">
                ${freshnessBadge(detailFreshness)}
                ${renderAlertBadges(alerts)}
                <span class="inventory-badge is-structural">${escapeHtml(percentileLabel)}</span>
              </div>
              <div class="inventory-summary-value">${escapeHtml(formatValue(latestValue, activeData.indicator.unit))}</div>
              <div class="inventory-summary-change">${escapeHtml(formatSignedValue(latest?.latest?.changeFromPriorAbs, activeData.indicator.unit))} vs prior period</div>
              <div class="inventory-summary-grid">
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Next release</div>
                  <div class="inventory-summary-definition is-mono">${escapeHtml(releaseStatus.label)}</div>
                </div>
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
                ${
                  ytdStats
                    ? `
                      <div class="inventory-summary-row">
                        <div class="inventory-summary-term">YTD change</div>
                        <div class="inventory-summary-definition is-mono">${escapeHtml(formatSignedValue(ytdStats.ytdChange, activeData.indicator.unit))}</div>
                      </div>
                      <div class="inventory-summary-row">
                        <div class="inventory-summary-term">vs 5Y median YTD</div>
                        <div class="inventory-summary-definition is-mono">${escapeHtml(formatSignedValue(ytdStats.deviationFromMedian, activeData.indicator.unit))}</div>
                      </div>
                    `
                    : ""
                }
              </div>
            </div>
          </section>

          <section class="inventory-panel">
            <div class="inventory-summary">
              <h2 class="inventory-panel-title">Source</h2>
              <div class="inventory-summary-grid">
                <div class="inventory-summary-row">
                  <div class="inventory-summary-term">Publisher</div>
                  <div class="inventory-summary-definition">${sourceLabelMarkup(sourceLabel, sourceUrl)}</div>
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
  const cards = Array.isArray(snapshotCache.value?.cards) ? snapshotCache.value.cards : null;
  const { selected, available, allSelected } = snapshotGroupSelectionState(route, cards);
  const scopedCards = cards ? filterCardsByCommodityGroups(cards, selected) : [];
  const visibleCards = hasActiveSearch() ? searchSnapshotCards(scopedCards, searchQuery) : scopedCards;
  const activeGroupLabel =
    allSelected || selected.length !== 1
      ? "All sectors"
      : COMMODITY_GROUPS.find((group) => group.slug === selected[0])?.label || "All sectors";
  const showMeta = Boolean(cards && (route.view !== "detail" || hasActiveSearch()));

  filterRoot.innerHTML = `
    <div class="filter-wrap inventory-filter-wrap">
      <nav class="filter-bar inventory-filter-bar" aria-label="Inventory commodity groups">
        ${(() => {
          const group = COMMODITY_GROUPS[0];
          const href = buildInventorySnapshotHref(group.slug);
          const isSelected = allSelected;

          return `
            <a
              class="filter-pill inventory-filter-pill inventory-filter-reset${isSelected ? " is-selected" : ""}"
              data-group="${escapeHtml(group.slug)}"
              data-inventory-nav
              href="${escapeHtml(href)}"
              ${isSelected ? 'aria-current="page"' : ""}
              style="--filter-pill-color:${escapeHtml(filterPillColorForGroup(group.slug))};"
            >
              ${escapeHtml(group.label)}
            </a>
          `;
        })()}
        <div class="filter-divider" aria-hidden="true"></div>
        <div class="inventory-group-row">
          ${COMMODITY_GROUPS.filter((group) => group.slug !== "all").map((group) => {
            const href = buildInventorySnapshotHref(group.slug);
            const populated = cards ? isGroupPopulated(group.slug, cards) : !["coal", "fertilisers"].includes(group.slug);
            const disabled = !populated;
            const showSelected = !disabled && (allSelected || selected.includes(group.slug));
            return `
              <a
                class="filter-pill inventory-filter-pill inventory-sector-pill${showSelected ? " is-selected" : ""}${disabled ? " is-disabled" : ""}"
                data-group="${escapeHtml(group.slug)}"
                data-inventory-nav
                href="${escapeHtml(href)}"
                ${showSelected && !allSelected ? 'aria-current="page"' : ""}
                ${disabled ? 'aria-disabled="true" title="Coming soon"' : ""}
                ${disabled ? 'tabindex="-1"' : ""}
                style="--filter-pill-color:${escapeHtml(filterPillColorForGroup(group.slug))};"
              >
                ${escapeHtml(group.label)}
              </a>
            `;
          }).join("")}
        </div>
      </nav>
      ${
        showMeta
          ? `
            <div class="inventory-filter-meta">
              <div class="inventory-filter-status">
                <div class="filter-summary">
                  <span>${escapeHtml(String(visibleCards.length))} indicator${visibleCards.length === 1 ? "" : "s"} ${hasActiveSearch() ? "matching search" : "in view"}</span>
                  <span class="filter-summary-divider" aria-hidden="true"></span>
                  <span>${escapeHtml(activeGroupLabel)}</span>
                  <span class="filter-summary-divider" aria-hidden="true"></span>
                  <span>Updated ${escapeHtml(formatUtcTimestamp(snapshotCache.value.generatedAt))}</span>
                </div>
                <div class="inventory-filter-actions">
                  ${
                    hasActiveSearch()
                      ? `
                        <span class="search-status">Search: ${escapeHtml(searchQuery)}</span>
                        <button class="clear-filters" type="button" data-clear-search>clear search</button>
                      `
                      : `<span class="filter-summary-muted">${
                          allSelected ? "All sectors selected." : `${selected.length} sector${selected.length === 1 ? "" : "s"} selected.`
                        } Search by indicator, code, source, or commodity.</span>`
                  }
                </div>
              </div>
            </div>
          `
          : ""
      }
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
  renderSearchUi();
  appRoot.innerHTML = renderLoading(route.view);

  if (route.view === "not-found") {
    setDocumentTitle("CommodityWatch | InventoryWatch");
    appRoot.innerHTML = renderError("Inventory route not found", route.reason || "The requested InventoryWatch route is invalid.", {
      retry: false,
    });
    renderSearchUi();
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
        appRoot.innerHTML = renderDetailUnavailable(
          route,
          data,
          "No data available",
          "This indicator route is live, but the backend has not published any observations yet."
        );
        renderSearchUi();
        return;
      }

      appRoot.innerHTML = renderDetailView(route, data, latest, alternateData, Boolean(excludeYear2020 && !alternateData?.seasonalRange?.length));
      renderSearchUi();
      return;
    }

    const snapshot = await getSnapshot();
    if (!isCurrent()) {
      return;
    }

    renderFilterBar(route);
    appRoot.innerHTML = renderSnapshotView(route, snapshot);
    renderSearchUi();
    restoreSnapshotPosition();
  } catch (error) {
    if (!isCurrent()) {
      return;
    }

    setDocumentTitle("CommodityWatch | InventoryWatch");
    appRoot.innerHTML = renderError(
      "InventoryWatch unavailable",
      humanizeInventoryError(error)
    );
    renderSearchUi();
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

if (navSearch && searchToggle && searchInput) {
  searchToggle.addEventListener("click", (event) => {
    event.stopPropagation();
    searchOpen = !searchOpen;
    renderSearchUi();

    if (searchOpen) {
      window.requestAnimationFrame(() => searchInput.focus());
    }
  });

  searchInput.addEventListener("input", () => {
    searchQuery = compactSearchQuery(searchInput.value);
    const route = currentRoute();

    if (route.view === "detail" && hasActiveSearch()) {
      snapshotGroupFilters = null;
      snapshotFilterRouteKey = null;
      window.history.pushState({}, "", buildInventorySnapshotHref());
      window.scrollTo({ top: 0, behavior: "auto" });
    }

    renderRoute();
    renderSearchUi();
  });

  searchInput.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }

    event.preventDefault();
    if (searchInput.value) {
      searchInput.value = "";
      searchQuery = "";
      renderRoute();
      renderSearchUi();
      return;
    }

    searchOpen = false;
    renderSearchUi();
    searchToggle.focus();
  });
}

document.addEventListener("click", (event) => {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) {
    return;
  }

  if (searchOpen && !eventPathIncludes(event, navSearch)) {
    searchOpen = false;
    renderSearchUi();
  }

  const retryButton = target.closest("[data-retry-inventory]");
  if (retryButton) {
    invalidateCurrentRouteCache(currentRoute());
    renderRoute();
    return;
  }

  const clearSearchButton = target.closest("[data-clear-search]");
  if (clearSearchButton) {
    searchQuery = "";
    if (searchInput) {
      searchInput.value = "";
    }
    renderRoute();
    renderSearchUi();
    return;
  }

  const groupPill = target.closest(".inventory-filter-pill[data-group]");
  if (groupPill instanceof HTMLAnchorElement) {
    if (groupPill.getAttribute("aria-disabled") === "true") {
      event.preventDefault();
      return;
    }

    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }

    const route = currentRoute();
    const available = availableSnapshotGroupSlugs(snapshotCache.value?.cards ?? null);
    const groupSlug = groupPill.dataset.group || "all";

    event.preventDefault();

    if (route.view === "snapshot") {
      const { selected } = snapshotGroupSelectionState(route, snapshotCache.value?.cards ?? null);
      snapshotGroupFilters =
        groupSlug === "all"
          ? [...available]
          : toggleInventoryGroupSelection(selected, groupSlug, available);
      snapshotFilterRouteKey = normalizePath(window.location.pathname);
      renderRoute();
      return;
    }

    snapshotGroupFilters =
      groupSlug === "all" ? [...available] : normalizeSnapshotGroupFilters([groupSlug], available);
    snapshotFilterRouteKey = normalizePath(buildInventorySnapshotHref());
    detailReturnState = null;
    navigate(buildInventorySnapshotHref());
    return;
  }

  const backLink = target.closest("[data-inventory-back]");
  if (backLink instanceof HTMLAnchorElement) {
    event.preventDefault();
    prepareSnapshotRestore();
    navigate(backLink.href);
    return;
  }

  const link = target.closest("a[data-inventory-nav]");
  if (!link) {
    return;
  }

  if (link.getAttribute("aria-disabled") === "true") {
    event.preventDefault();
    return;
  }

  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
    return;
  }

  event.preventDefault();
  if (link.classList.contains("inventory-card")) {
    captureDetailReturnState({ indicatorId: link.getAttribute("data-indicator-id") });
  } else {
    detailReturnState = null;
  }
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

document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") {
    return;
  }

  if (searchOpen) {
    searchOpen = false;
    renderSearchUi();
  }
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
window.setInterval(() => {
  renderRoute();
}, 60 * 1000);
