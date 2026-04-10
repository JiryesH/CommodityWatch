import { loadCalendarEvents, findCalendarEventForDemandRelease } from "../calendar-watch/calendar-data.js";
import { buildCalendarEventHref } from "../calendar-watch/router.js";
import { fetchDemandConceptDetail, fetchDemandWatchPageData } from "./api-client.js";
import {
  DEMAND_TAXONOMY,
  DEMAND_VERTICALS,
  buildDemandWatchPageModel,
  getDemandVerticalById,
  mapDemandConceptDetail,
} from "./data.js";
import {
  buildDemandConceptHref,
  buildDemandOverviewHref,
  normalizeDemandPath,
  parseDemandRoute,
} from "./router.js";

const VERTICAL_SECTOR_MAP = {
  "crude-products": "energy",
  electricity: "natural-gas",
  grains: "agriculture",
  "base-metals": "metals_and_mining",
};

const PAGE_TIMESTAMP_FORMATTER = new Intl.DateTimeFormat("en-GB", {
  day: "2-digit",
  month: "short",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const DEFAULT_VISIBLE_MOVERS = 6;
const TAXONOMY_BY_ID = new Map(DEMAND_TAXONOMY.map((tier) => [String(tier.id || "").toLowerCase(), tier]));
const TAXONOMY_BY_LABEL = new Map(DEMAND_TAXONOMY.map((tier) => [String(tier.shortLabel || "").toLowerCase(), tier]));

let activeFilters = new Set();
let currentVerticalErrors = new Map();
let currentPageData = null;
let moversExpanded = false;
let activeTierBadge = null;
let tierDismissalBound = false;
let detailReturnState = null;
let pendingOverviewRestore = null;
let activeConceptDetail = null;

const conceptDetailCache = new Map();

const appRoot = document.getElementById("demand-root");
const filterRoot = document.getElementById("demand-filter-root");
const toTopButton = document.getElementById("to-top-btn");

if (!appRoot) {
  throw new Error("DemandWatch root is missing.");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setDocumentTitle(title) {
  document.title = title;
}

function currentRoute() {
  return parseDemandRoute(window.location.pathname);
}

function selectorEscape(value) {
  if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
    return CSS.escape(String(value));
  }

  return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function normalizeTimestamp(value) {
  const timestamp = Date.parse(String(value || ""));
  return Number.isFinite(timestamp) ? timestamp : null;
}

function stickyViewportOffset() {
  const navHeight = document.querySelector(".nav")?.offsetHeight || 0;
  const filterHeight = filterRoot?.querySelector(".filter-wrap")?.offsetHeight || 0;
  return navHeight + filterHeight + 20;
}

function captureDetailReturnState({ conceptCode } = {}) {
  detailReturnState = {
    conceptCode: conceptCode || null,
    filters: [...activeFilters],
    scrollY: window.scrollY,
    hash: window.location.hash || "",
  };
}

function prepareOverviewRestore() {
  pendingOverviewRestore = detailReturnState
    ? {
        ...detailReturnState,
        filters: [...detailReturnState.filters],
      }
    : {
        conceptCode: null,
        filters: [...activeFilters],
        scrollY: window.scrollY,
        hash: window.location.hash || "",
      };
  detailReturnState = null;
  activeConceptDetail = null;
}

function queueOverviewRestore() {
  if (!detailReturnState) {
    return;
  }

  pendingOverviewRestore = {
    ...detailReturnState,
    filters: [...detailReturnState.filters],
  };
  activeConceptDetail = null;
}

function restoreOverviewPosition() {
  if (!pendingOverviewRestore) {
    return;
  }

  const restoreState = pendingOverviewRestore;
  pendingOverviewRestore = null;

  window.requestAnimationFrame(() => {
    const target =
      restoreState.conceptCode != null
        ? appRoot.querySelector(`[data-concept-code="${selectorEscape(restoreState.conceptCode)}"]`)
        : null;

    if (target) {
      const top = target.getBoundingClientRect().top + window.scrollY - stickyViewportOffset();
      window.scrollTo({ top: Math.max(0, top), behavior: "auto" });
      return;
    }

    if (typeof restoreState.scrollY === "number" && Number.isFinite(restoreState.scrollY)) {
      window.scrollTo({ top: Math.max(0, restoreState.scrollY), behavior: "auto" });
      return;
    }

    if (restoreState.hash) {
      const hashTarget = appRoot.querySelector(restoreState.hash);
      if (hashTarget) {
        hashTarget.scrollIntoView({ block: "start" });
      }
    }
  });
}

function navigate(href) {
  const nextPath = normalizeDemandPath(new URL(href, window.location.origin).pathname);
  const currentPath = normalizeDemandPath(window.location.pathname);
  if (nextPath === currentPath) {
    return;
  }

  window.history.pushState({}, "", nextPath);
  window.scrollTo({ top: 0, behavior: "auto" });
  void renderRoute();
}

function signalClassForTrend(trend) {
  switch (trend) {
    case "up":
      return "is-up";
    case "down":
      return "is-down";
    default:
      return "is-flat";
  }
}

function trendArrow(trend) {
  switch (trend) {
    case "up":
      return "↑";
    case "down":
      return "↓";
    default:
      return "→";
  }
}

function trendArrowTooltip(trend) {
  switch (trend) {
    case "up":
      return "Trending above recent baseline";
    case "down":
      return "Tracking below recent baseline";
    default:
      return "Tracking near recent baseline";
  }
}

function formatPageTimestamp(value) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    return null;
  }

  return `${PAGE_TIMESTAMP_FORMATTER.format(parsed).replace(",", "")} UTC`;
}

function formatExactTimestamp(value) {
  return formatPageTimestamp(value);
}

function sourceDisplayLabel(label, href) {
  if (label) {
    return label;
  }

  if (!href) {
    return null;
  }

  try {
    return new URL(href).hostname.replace(/^www\./, "");
  } catch {
    return "Source";
  }
}

function sourceLinkMarkup(label, href, className = "dw-source-link") {
  const displayLabel = sourceDisplayLabel(label, href);
  if (!displayLabel) {
    return "";
  }

  if (!href) {
    return `<span class="${escapeHtml(className)}">${escapeHtml(displayLabel)}</span>`;
  }

  return `<a class="${escapeHtml(className)}" href="${escapeHtml(href)}" target="_blank" rel="noreferrer noopener">${escapeHtml(displayLabel)}</a>`;
}

function renderSourceRow(label, href, className = "dw-source-row") {
  const markup = sourceLinkMarkup(label, href);
  if (!markup) {
    return "";
  }

  return `
    <p class="${escapeHtml(className)}">
      <span class="dw-source-label">Source</span>
      <span class="dw-source-sep" aria-hidden="true">·</span>
      ${markup}
    </p>
  `;
}

function getTierDefinition(tierKey, tierLabel = tierKey) {
  const normalizedKey = String(tierKey || "").trim().toLowerCase();
  if (normalizedKey) {
    if (TAXONOMY_BY_ID.has(normalizedKey)) {
      return TAXONOMY_BY_ID.get(normalizedKey);
    }

    const compactKey = normalizedKey.match(/^t\d+/)?.[0];
    if (compactKey && TAXONOMY_BY_ID.has(compactKey)) {
      return TAXONOMY_BY_ID.get(compactKey);
    }
  }

  const normalizedLabel = String(tierLabel || "").trim().toLowerCase();
  if (normalizedLabel) {
    if (TAXONOMY_BY_LABEL.has(normalizedLabel)) {
      return TAXONOMY_BY_LABEL.get(normalizedLabel);
    }

    const compactLabel = normalizedLabel.match(/^t\d+/)?.[0];
    if (compactLabel && TAXONOMY_BY_ID.has(compactLabel)) {
      return TAXONOMY_BY_ID.get(compactLabel);
    }
  }

  return null;
}

function renderTierBadge(label, tierKey) {
  const tier = getTierDefinition(tierKey, label);
  if (!tier) {
    return `<span class="tier-badge">${escapeHtml(label)}</span>`;
  }

  return `
    <button
      class="tier-badge tier-badge-button"
      type="button"
      data-tier-key="${escapeHtml(tier.id)}"
      data-tier-label="${escapeHtml(tier.shortLabel)}"
      aria-haspopup="dialog"
      aria-expanded="false"
    >${escapeHtml(label)}</button>
  `;
}

function renderSparkline(values, trend = "flat") {
  const points = Array.isArray(values) && values.length ? values : [0, 0];
  const minValue = Math.min(...points);
  const maxValue = Math.max(...points);
  const width = 160;
  const height = 52;
  const span = maxValue - minValue || 1;
  const step = width / Math.max(points.length - 1, 1);
  const line = points
    .map((value, index) => {
      const x = Number((index * step).toFixed(2));
      const y = Number((height - ((value - minValue) / span) * height).toFixed(2));
      return `${index === 0 ? "M" : "L"}${x} ${y}`;
    })
    .join(" ");
  const area = `${line} L ${width} ${height} L 0 ${height} Z`;

  return `
    <svg class="dw-sparkline ${escapeHtml(signalClassForTrend(trend))}" viewBox="0 0 ${width} ${height}" aria-hidden="true">
      <path class="dw-sparkline-area" d="${area}"></path>
      <path class="dw-sparkline-line" d="${line}"></path>
    </svg>
  `;
}

function renderFilterBar() {
  return `
    <nav class="filter-wrap" aria-label="DemandWatch filter">
      <div class="filter-bar">
        <button class="filter-pill" data-filter="all" data-sector="ALL" aria-pressed="false">All</button>
        <div class="filter-divider"></div>
        ${DEMAND_VERTICALS.map(
          (vertical) => `
            <button
              class="filter-pill"
              data-filter="${escapeHtml(vertical.id)}"
              data-sector="${escapeHtml(VERTICAL_SECTOR_MAP[vertical.id] || "")}"
              aria-pressed="false"
            >${escapeHtml(vertical.navLabel)}</button>
          `
        ).join("")}
      </div>
    </nav>
  `;
}

function renderPageHeader(pageData) {
  const updatedStamp = formatPageTimestamp(pageData.generatedAt);

  return `
    <header class="demand-page-head">
      <div class="demand-page-head-row">
        <div class="demand-page-title-block">
          <h1 class="demand-page-title">Demand Pulse</h1>
          <p class="demand-page-description">Published demand signals across crude, power, grains, and base metals.</p>
        </div>
      </div>
      ${
        updatedStamp
          ? `
              <div class="demand-page-meta">
                <span class="demand-page-meta-item"><strong>Updated</strong> ${escapeHtml(updatedStamp)}</span>
              </div>
            `
          : ""
      }
    </header>
  `;
}

function renderMacroStrip(items) {
  if (!items.length) {
    return "";
  }

  return `
    <section class="demand-backdrop-wrap" data-dw-overview="macro" aria-label="Demand macro context">
      <div class="dw-card-head">
        <p class="section-kicker demand-backdrop-kicker">Macro Context</p>
      </div>
      <div class="macro-strip">
        ${items
          .map(
            (item) => `
              <article class="macro-card ${escapeHtml(signalClassForTrend(item.trend))}">
                <p class="macro-label">${escapeHtml(item.label)}</p>
                <p class="macro-value">${escapeHtml(item.value)}</p>
                ${item.descriptor ? `<p class="macro-descriptor">${escapeHtml(item.descriptor)}</p>` : ""}
                <p class="macro-change">
                  <span class="macro-arrow" aria-hidden="true">${escapeHtml(trendArrow(item.trend))}</span>
                  ${escapeHtml(item.change)}
                </p>
                <div class="macro-meta">
                  <p class="macro-freshness">${escapeHtml(item.freshness)}</p>
                  ${renderSourceRow(item.sourceLabel, item.sourceUrl, "dw-source-row macro-source-row")}
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderScorecard(items) {
  if (!items.length) {
    return "";
  }

  return `
    <section class="dw-card" data-dw-overview="scorecard">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Scorecard</p>
        <h2 class="section-title">Where does demand stand?</h2>
      </div>
      <div class="scorecard-table" aria-label="Demand scorecard">
        <div class="scorecard-head" aria-hidden="true">
          <span>Vertical</span>
          <span>Signal</span>
          <span>YoY</span>
          <span>As of</span>
          <span>Updated</span>
          <span>Source</span>
        </div>
        ${items
          .map(
            (vertical) => `
              <article
                class="scorecard-row ${escapeHtml(signalClassForTrend(vertical.scorecard.trend))}"
                tabindex="0"
                data-dw-filter="${escapeHtml(vertical.id)}"
                aria-label="Filter to ${escapeHtml(vertical.shortLabel)}"
                style="--vertical-accent:${escapeHtml(vertical.accent)};"
              >
                <span class="scorecard-vertical">
                  <span class="scorecard-dot"></span>
                  ${escapeHtml(vertical.shortLabel)}
                </span>
                <span class="scorecard-cell scorecard-cell-signal">
                  <strong>${escapeHtml(vertical.scorecard.label)}</strong>
                  <span class="scorecard-secondary">${escapeHtml(vertical.scorecard.value)}</span>
                </span>
                <span class="scorecard-cell">${escapeHtml(vertical.scorecard.yoyLabel)}</span>
                <span class="scorecard-cell">${escapeHtml(vertical.scorecard.latestData)}</span>
                <span class="scorecard-cell">${escapeHtml(vertical.scorecard.freshness)}</span>
                <span class="scorecard-cell scorecard-cell-source">
                  ${sourceLinkMarkup(vertical.scorecard.sourceLabel, vertical.scorecard.sourceUrl, "scorecard-source-link")}
                </span>
                <span class="scorecard-drill" aria-hidden="true">→</span>
              </article>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderMovers(movers) {
  if (!movers.length) {
    return "";
  }

  const visibleMovers = moversExpanded ? movers : movers.slice(0, DEFAULT_VISIBLE_MOVERS);
  const hasDisclosure = movers.length > DEFAULT_VISIBLE_MOVERS;

  return `
    <section class="dw-card" data-dw-overview="movers">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Movers</p>
        <h2 class="section-title">Latest releases</h2>
      </div>
      <div class="mover-grid">
        ${visibleMovers
          .map((mover) => {
            const vertical = getDemandVerticalById(mover.verticalId);
            const surpriseMarkup =
              mover.surprise && !/^No active surprise flag$/i.test(mover.surprise)
                ? `<p class="mover-surprise">${escapeHtml(mover.surprise)}</p>`
                : "";
            const href = buildDemandConceptHref(mover.verticalId, mover.code);
            return `
              <article
                class="mover-card ${escapeHtml(signalClassForTrend(mover.trend))}"
                style="--vertical-accent:${escapeHtml(vertical?.accent || mover.accent || "var(--color-amber)")};"
                data-vertical="${escapeHtml(mover.verticalId)}"
                data-concept-nav
                data-concept-code="${escapeHtml(mover.code)}"
                data-concept-vertical="${escapeHtml(mover.verticalId)}"
                data-concept-href="${escapeHtml(href)}"
                role="link"
                tabindex="0"
                aria-label="Open detail for ${escapeHtml(mover.title)}"
              >
                <div class="mover-head">
                  <p class="mover-vertical">${escapeHtml(vertical?.shortLabel || "Demand")}</p>
                  ${renderTierBadge(mover.tier, mover.tierKey)}
                </div>
                <h3 class="mover-title">${escapeHtml(mover.title)}</h3>
                <p class="mover-value">${escapeHtml(mover.value)}</p>
                <p class="mover-change">${escapeHtml(mover.change)}</p>
                ${surpriseMarkup}
                <div class="mover-meta">
                  <p class="mover-freshness">${escapeHtml(mover.freshness)}</p>
                  ${renderSourceRow(mover.sourceLabel, mover.sourceUrl)}
                </div>
                <p class="mover-drill" aria-hidden="true">View detail →</p>
              </article>
            `;
          })
          .join("")}
      </div>
      ${
        hasDisclosure
          ? `
              <div class="section-disclosure">
                <button class="section-disclosure-button" type="button" data-toggle-movers>
                  ${moversExpanded ? "Show less" : "Show more"}
                </button>
              </div>
            `
          : ""
      }
    </section>
  `;
}

function renderIndicatorCard(indicator, accent, verticalId) {
  const trendTooltip = trendArrowTooltip(indicator.trend);
  const valueHtml = indicator.valueHref
    ? `<a href="${escapeHtml(indicator.valueHref)}" class="indicator-value-link">${escapeHtml(indicator.value)}</a>`
    : escapeHtml(indicator.value);
  const headMeta = indicator.placeholder
    ? `<span class="indicator-status is-${escapeHtml(indicator.coverageTone)}">${escapeHtml(indicator.coverageLabel)}</span>`
    : `<span class="indicator-trend" title="${escapeHtml(trendTooltip)}" aria-label="${escapeHtml(trendTooltip)}">${escapeHtml(trendArrow(indicator.trend))}</span>`;
  const href = indicator.placeholder ? "" : buildDemandConceptHref(verticalId, indicator.code);

  return `
    <article
      class="indicator-card ${escapeHtml(signalClassForTrend(indicator.trend))}${indicator.placeholder ? " is-placeholder" : ""}${indicator.placeholder ? "" : " is-drilldown"}"
      style="--vertical-accent:${escapeHtml(accent)};"
      ${indicator.placeholder ? "" : `data-concept-nav data-concept-code="${escapeHtml(indicator.code)}" data-concept-vertical="${escapeHtml(verticalId)}" data-concept-href="${escapeHtml(href)}" role="link" tabindex="0" aria-label="Open detail for ${escapeHtml(indicator.title)}"`}
    >
      <div class="indicator-head">
        ${renderTierBadge(indicator.tier, indicator.tierKey)}
        ${headMeta}
      </div>
      <h4 class="indicator-title">${escapeHtml(indicator.title)}</h4>
      <p class="indicator-value">${valueHtml}</p>
      <p class="indicator-change">${escapeHtml(indicator.change)}</p>
      <div class="indicator-chart">
        ${renderSparkline(indicator.sparkline, indicator.trend)}
        <p class="sparkline-range">Recent trend</p>
      </div>
      ${indicator.detail ? `<p class="indicator-detail">${escapeHtml(indicator.detail)}</p>` : ""}
      ${renderSourceRow(indicator.sourceLabel, indicator.sourceUrl)}
      ${indicator.placeholder ? "" : '<p class="indicator-drill" aria-hidden="true">View detail →</p>'}
    </article>
  `;
}

function renderDataTable(rows) {
  if (!rows.length) {
    return "";
  }

  return `
    <div class="detail-table-wrap">
      <table class="detail-table">
        <thead>
          <tr>
            <th scope="col">Indicator</th>
            <th scope="col">Latest</th>
            <th scope="col">vs prior</th>
            <th scope="col">YoY</th>
            <th scope="col">Updated</th>
            <th scope="col">Source</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr class="${row.placeholder ? "is-placeholder" : ""}">
                  <th scope="row">${escapeHtml(row.label)}</th>
                  <td>${escapeHtml(row.latest)}</td>
                  <td>${escapeHtml(row.change)}</td>
                  <td>${escapeHtml(row.yoy)}</td>
                  <td>${escapeHtml(row.freshness)}</td>
                  <td>${sourceLinkMarkup(row.sourceLabel, row.sourceUrl, "detail-table-source-link")}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderCalendarCard(items) {
  if (!items.length) {
    return "";
  }

  return `
    <section class="sidebar-card">
      <p class="detail-kicker">Release timing</p>
      <div class="calendar-list">
        ${items
          .map(
            (item) => `
              <article class="calendar-item">
                <p class="calendar-item-label">${escapeHtml(item.label)}</p>
                <p class="calendar-item-value">${escapeHtml(item.value)}</p>
                <div class="calendar-item-meta">
                  ${item.note ? `<p class="calendar-item-note">${escapeHtml(item.note)}</p>` : ""}
                  ${renderSourceRow(item.sourceLabel, item.sourceUrl, "dw-source-row calendar-source-row")}
                  ${
                    item.calendarHref
                      ? `<p class="calendar-item-link-row"><a class="calendar-item-link" href="${escapeHtml(item.calendarHref)}">Open in CalendarWatch</a></p>`
                      : ""
                  }
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderBackLink() {
  return `
    <div class="demand-back-row">
      <a class="demand-back-link" data-demand-back href="${escapeHtml(buildDemandOverviewHref())}">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M15 18l-6-6 6-6"></path>
          <path d="M21 12H9"></path>
        </svg>
        <span>Back to Demand Pulse</span>
      </a>
    </div>
  `;
}

function renderConceptHistoryChart(history, trend = "flat") {
  if (!history.length) {
    return "";
  }

  const width = 860;
  const height = 260;
  const pad = { top: 18, right: 28, bottom: 42, left: 22 };
  const minValue = Math.min(...history.map((point) => point.value));
  const maxValue = Math.max(...history.map((point) => point.value));
  const span = maxValue - minValue || 1;
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;
  const step = innerWidth / Math.max(history.length - 1, 1);
  const linePath = history
    .map((point, index) => {
      const x = Number((pad.left + index * step).toFixed(2));
      const y = Number((pad.top + innerHeight - ((point.value - minValue) / span) * innerHeight).toFixed(2));
      return `${index === 0 ? "M" : "L"}${x} ${y}`;
    })
    .join(" ");
  const areaPath = `${linePath} L ${pad.left + innerWidth} ${pad.top + innerHeight} L ${pad.left} ${pad.top + innerHeight} Z`;
  const firstPoint = history[0];
  const lastPoint = history[history.length - 1];

  return `
    <div class="concept-history-chart">
      <svg class="concept-history-svg ${escapeHtml(signalClassForTrend(trend))}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Recent concept history">
        <path class="concept-history-area" d="${areaPath}"></path>
        <path class="concept-history-line" d="${linePath}"></path>
        <text class="concept-history-label" x="${pad.left}" y="${height - 12}" text-anchor="start">${escapeHtml(firstPoint.periodLabel)}</text>
        <text class="concept-history-label" x="${width - pad.right}" y="${height - 12}" text-anchor="end">${escapeHtml(lastPoint.periodLabel)}</text>
      </svg>
    </div>
  `;
}

function renderConceptObservationTable(observations) {
  if (!observations.length) {
    return "";
  }

  return `
    <div class="detail-table-wrap concept-observations-wrap">
      <table class="detail-table concept-observations-table">
        <thead>
          <tr>
            <th scope="col">Period</th>
            <th scope="col">Value</th>
            <th scope="col">Released</th>
            <th scope="col">Source</th>
          </tr>
        </thead>
        <tbody>
          ${observations
            .map(
              (row) => `
                <tr>
                  <th scope="row">${escapeHtml(row.periodLabel)}</th>
                  <td>${escapeHtml(row.displayValue)}</td>
                  <td>${escapeHtml(formatExactTimestamp(row.releaseDate) || "Unavailable")}</td>
                  <td>${sourceLinkMarkup(row.sourceLabel, row.sourceUrl, "detail-table-source-link")}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderConceptDetail(concept) {
  const updatedStamp = formatPageTimestamp(concept.generatedAt);
  const metaPills = [
    concept.change ? `<div class="concept-stat"><span class="concept-stat-label">vs prior</span><strong>${escapeHtml(concept.change)}</strong></div>` : "",
    concept.yoy ? `<div class="concept-stat"><span class="concept-stat-label">YoY</span><strong>${escapeHtml(concept.yoy)}</strong></div>` : "",
    concept.freshness ? `<div class="concept-stat"><span class="concept-stat-label">Freshness</span><strong>${escapeHtml(concept.freshness)}</strong></div>` : "",
    concept.cadence ? `<div class="concept-stat"><span class="concept-stat-label">Cadence</span><strong>${escapeHtml(concept.cadence)}</strong></div>` : "",
  ]
    .filter(Boolean)
    .join("");

  return `
    <div class="demand-shell demand-shell--detail">
      ${renderBackLink()}
      <section class="dw-card concept-hero-card" style="--vertical-accent:${escapeHtml(getDemandVerticalById(concept.verticalId)?.accent || "var(--color-amber)")};">
        <div class="concept-hero-head">
          <div>
            <p class="section-kicker">${escapeHtml(concept.verticalLabel)}</p>
            <h1 class="concept-title">${escapeHtml(concept.title)}</h1>
          </div>
          ${renderTierBadge(concept.tier, concept.tierKey)}
        </div>
        <div class="concept-value-row">
          <p class="concept-value">${escapeHtml(concept.value)}</p>
          ${concept.latestPeriodLabel ? `<p class="concept-period">${escapeHtml(concept.latestPeriodLabel)}</p>` : ""}
        </div>
        ${metaPills ? `<div class="concept-stat-grid">${metaPills}</div>` : ""}
        <div class="concept-meta-row">
          ${renderSourceRow(concept.sourceLabel, concept.sourceUrl)}
          ${
            updatedStamp
              ? `<p class="concept-updated"><span class="dw-source-label">Updated</span><span class="dw-source-sep" aria-hidden="true">·</span>${escapeHtml(updatedStamp)}</p>`
              : ""
          }
        </div>
        ${concept.detail ? `<p class="concept-detail-copy">${escapeHtml(concept.detail)}</p>` : ""}
      </section>
      <section class="dw-card concept-history-card">
        <div class="dw-card-head">
          <p class="section-kicker">History</p>
          <h2 class="section-title">Recent observations</h2>
        </div>
        ${renderConceptHistoryChart(concept.history, concept.trend)}
      </section>
      ${concept.calendar.length ? renderCalendarCard(concept.calendar) : ""}
      ${
        concept.observations.length
          ? `
            <section class="dw-card concept-table-card">
              <div class="dw-card-head">
                <p class="section-kicker">Observations</p>
                <h2 class="section-title">Recent releases</h2>
              </div>
              ${renderConceptObservationTable(concept.observations)}
            </section>
          `
          : ""
      }
      <div id="demand-tier-explainer" class="tier-explainer" role="dialog" aria-live="polite" hidden></div>
    </div>
  `;
}

function renderVerticalSection(vertical) {
  const visibleSections = vertical.sections.filter(
    (section) => !section.isCoverageGap && (section.indicators.length || section.tableRows.length)
  );
  const calendarMarkup = renderCalendarCard(vertical.calendar);

  if (!visibleSections.length && !calendarMarkup) {
    return "";
  }

  return `
    <section
      id="${escapeHtml(vertical.id)}"
      class="dw-section vertical-section"
      data-dw-section="${escapeHtml(vertical.id)}"
      data-dw-vertical="${escapeHtml(vertical.id)}"
      style="--vertical-accent:${escapeHtml(vertical.accent)};"
    >
      <div class="vertical-header">
        <div>
          <p class="section-kicker vertical-kicker">${escapeHtml(vertical.shortLabel)}</p>
          <h2 class="section-title">${escapeHtml(vertical.label)}</h2>
          ${vertical.summary ? `<p class="vertical-summary">${escapeHtml(vertical.summary)}</p>` : ""}
        </div>
      </div>
      <div class="vertical-layout${calendarMarkup ? "" : " vertical-layout--full"}">
        <div class="vertical-main">
          ${visibleSections
            .map(
              (section) => `
                <article class="detail-block">
                  <div class="detail-block-head">
                    <div>
                      <p class="detail-kicker">${escapeHtml(section.title)}</p>
                      <h3>${escapeHtml(section.description)}</h3>
                    </div>
                  </div>
                  ${section.indicators.length ? `<div class="indicator-grid">${section.indicators.map((indicator) => renderIndicatorCard(indicator, vertical.accent, vertical.id)).join("")}</div>` : ""}
                  ${renderDataTable(section.tableRows)}
                </article>
              `
            )
            .join("")}
        </div>
        ${calendarMarkup ? `<aside class="vertical-sidebar">${calendarMarkup}</aside>` : ""}
      </div>
    </section>
  `;
}

function renderVerticalStateSection(meta, title, copy, tone = "neutral") {
  return `
    <section
      id="${escapeHtml(meta.id)}"
      class="dw-section vertical-section vertical-section--state"
      data-dw-section="${escapeHtml(meta.id)}"
      data-dw-vertical="${escapeHtml(meta.id)}"
      style="--vertical-accent:${escapeHtml(meta.accent)};"
    >
      <div class="vertical-header">
        <div>
          <p class="section-kicker vertical-kicker">${escapeHtml(meta.shortLabel)}</p>
          <h2 class="section-title">${escapeHtml(meta.label)}</h2>
        </div>
      </div>
      <div class="dw-inline-state is-${escapeHtml(tone)}">
        <p class="dw-inline-state-title">${escapeHtml(title)}</p>
        <p class="dw-inline-state-copy">${escapeHtml(copy)}</p>
      </div>
    </section>
  `;
}

function renderPageState({ title, copy, buttonLabel = "", buttonAttr = "" }) {
  if (filterRoot) {
    filterRoot.innerHTML = "";
  }

  closeTierExplainer();
  appRoot.innerHTML = `
    <div class="demand-shell">
      <section class="demand-state-card">
        <h1 class="demand-page-title">${escapeHtml(title)}</h1>
        <p class="demand-page-description">${escapeHtml(copy)}</p>
        ${
          buttonLabel
            ? `
                <div class="dw-state-actions">
                  <button class="dw-state-button" type="button" ${buttonAttr}>${escapeHtml(buttonLabel)}</button>
                </div>
              `
            : ""
        }
      </section>
    </div>
  `;
}

function renderApp(pageData) {
  if (filterRoot) {
    filterRoot.innerHTML = renderFilterBar();
  }

  const overviewMarkup = [renderScorecard(pageData.scorecard), renderMovers(pageData.movers)]
    .filter(Boolean)
    .join("");
  const verticalsById = new Map(pageData.verticals.map((vertical) => [vertical.id, vertical]));
  const verticalMarkup = DEMAND_VERTICALS.map((meta) => {
    const vertical = verticalsById.get(meta.id);
    if (vertical) {
      return renderVerticalSection(vertical);
    }

    if (currentVerticalErrors.has(meta.id)) {
      return renderVerticalStateSection(meta, "Detail unavailable", currentVerticalErrors.get(meta.id), "error");
    }

    return "";
  })
    .filter(Boolean)
    .join("");

  appRoot.innerHTML = `
    <div class="demand-shell">
      ${renderPageHeader(pageData)}
      ${renderMacroStrip(pageData.macroStrip)}
      ${overviewMarkup ? `<section id="overview" class="dw-section overview-section" data-dw-section="overview">${overviewMarkup}</section>` : ""}
      ${verticalMarkup}
      <div id="demand-tier-explainer" class="tier-explainer" role="dialog" aria-live="polite" hidden></div>
    </div>
  `;
}

function getTierExplainerElement() {
  return document.getElementById("demand-tier-explainer");
}

function closeTierExplainer() {
  const explainer = getTierExplainerElement();
  if (explainer) {
    explainer.hidden = true;
    explainer.classList.remove("is-open");
  }

  if (activeTierBadge) {
    activeTierBadge.setAttribute("aria-expanded", "false");
    activeTierBadge = null;
  }
}

function positionTierExplainer(button, explainer) {
  if (!button || !explainer) {
    return;
  }

  const rect = button.getBoundingClientRect();
  const explainerRect = explainer.getBoundingClientRect();
  const viewportWidth = window.innerWidth || document.documentElement?.clientWidth || 0;
  const viewportHeight = window.innerHeight || document.documentElement?.clientHeight || 0;
  const margin = 16;
  const preferredTop = rect.bottom + 10;
  const aboveTop = rect.top - explainerRect.height - 10;
  const top =
    preferredTop + explainerRect.height <= viewportHeight - margin
      ? preferredTop
      : Math.max(margin, aboveTop);
  const left = Math.min(
    Math.max(margin, rect.left),
    Math.max(margin, viewportWidth - explainerRect.width - margin)
  );

  explainer.style.top = `${Math.round(top)}px`;
  explainer.style.left = `${Math.round(left)}px`;
}

function openTierExplainer(button) {
  const explainer = getTierExplainerElement();
  const tier = getTierDefinition(button?.dataset?.tierKey, button?.dataset?.tierLabel);
  if (!explainer || !button || !tier) {
    return;
  }

  if (activeTierBadge && activeTierBadge !== button) {
    activeTierBadge.setAttribute("aria-expanded", "false");
  }

  activeTierBadge = button;
  activeTierBadge.setAttribute("aria-expanded", "true");
  explainer.innerHTML = `
    <div class="tier-explainer-head">
      <p class="tier-explainer-kicker">${escapeHtml(tier.shortLabel)}</p>
      <p class="tier-explainer-reliability">${escapeHtml(tier.reliability)} reliability</p>
    </div>
    <h3 class="tier-explainer-title">${escapeHtml(tier.label)}</h3>
    <p class="tier-explainer-copy">${escapeHtml(tier.description)}</p>
  `;
  explainer.hidden = false;
  explainer.classList.add("is-open");
  positionTierExplainer(button, explainer);
}

function bindTierDismissal() {
  if (tierDismissalBound) {
    return;
  }

  tierDismissalBound = true;

  if (typeof document === "undefined" || typeof document.addEventListener !== "function") {
    return;
  }

  document.addEventListener("click", (event) => {
    const explainer = getTierExplainerElement();
    if (!explainer || explainer.hidden) {
      return;
    }

    if (event.target instanceof Element && event.target.closest(".tier-badge-button")) {
      return;
    }

    if (event.target instanceof Node && explainer.contains(event.target)) {
      return;
    }

    closeTierExplainer();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeTierExplainer();
    }
  });
}

function bindTierExplainers() {
  bindTierDismissal();

  appRoot.querySelectorAll(".tier-badge-button").forEach((button) => {
    button.addEventListener("mouseenter", () => openTierExplainer(button));
    button.addEventListener("mouseleave", () => {
      if (document.activeElement !== button) {
        closeTierExplainer();
      }
    });
    button.addEventListener("focus", () => openTierExplainer(button));
    button.addEventListener("blur", () => {
      window.setTimeout(() => {
        if (document.activeElement !== button) {
          closeTierExplainer();
        }
      }, 0);
    });
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();

      if (activeTierBadge === button) {
        closeTierExplainer();
        return;
      }

      openTierExplainer(button);
    });
    button.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        if (activeTierBadge === button) {
          closeTierExplainer();
        } else {
          openTierExplainer(button);
        }
      }
    });
  });
}

function bindMoversToggle() {
  appRoot.querySelector("[data-toggle-movers]")?.addEventListener("click", () => {
    moversExpanded = !moversExpanded;
    void renderRoute();
  });
}

function syncFilterUI() {
  const allMode = activeFilters.size === 0;

  document.querySelectorAll(".filter-pill[data-filter]").forEach((pill) => {
    const active =
      pill.dataset.filter === "all" ? allMode : allMode || activeFilters.has(pill.dataset.filter);
    pill.classList.toggle("is-selected", active);
    pill.setAttribute("aria-pressed", active ? "true" : "false");
  });

  const moverCards = document.querySelectorAll(".mover-card[data-vertical]");
  const verticalSections = document.querySelectorAll("[data-dw-vertical]");
  const overviewOnlyItems = document.querySelectorAll("[data-dw-overview]");

  if (allMode) {
    moverCards.forEach((card) => card.classList.remove("is-filtered-out"));
    verticalSections.forEach((section) => section.classList.remove("is-filtered-out"));
    overviewOnlyItems.forEach((section) => section.classList.remove("is-filtered-out"));
    return;
  }

  overviewOnlyItems.forEach((section) => section.classList.add("is-filtered-out"));
  moverCards.forEach((card) => {
    card.classList.toggle("is-filtered-out", !activeFilters.has(card.dataset.vertical));
  });
  verticalSections.forEach((section) => {
    section.classList.toggle("is-filtered-out", !activeFilters.has(section.dataset.dwVertical));
  });

  if (activeFilters.size === 1) {
    const [targetId] = [...activeFilters];
    const target = document.getElementById(targetId);
    if (target) {
      window.setTimeout(() => target.scrollIntoView({ behavior: "smooth", block: "start" }), 50);
    }
  }
}

function toggleFilter(filterId) {
  if (filterId === "all") {
    activeFilters.clear();
  } else if (activeFilters.size === 0) {
    activeFilters = new Set([filterId]);
  } else if (activeFilters.has(filterId)) {
    activeFilters.delete(filterId);
  } else {
    activeFilters.add(filterId);
    if (activeFilters.size === DEMAND_VERTICALS.length) {
      activeFilters.clear();
    }
  }

  syncFilterUI();
}

function focusFilter(filterId) {
  if (activeFilters.size === 1 && activeFilters.has(filterId)) {
    activeFilters.clear();
  } else {
    activeFilters = new Set([filterId]);
  }

  syncFilterUI();
}

function bindFilterBar() {
  if (!filterRoot) {
    return;
  }

  filterRoot.querySelectorAll(".filter-pill[data-filter]").forEach((pill) => {
    pill.addEventListener("click", () => toggleFilter(pill.dataset.filter));
  });

  appRoot.querySelectorAll(".scorecard-row[data-dw-filter]").forEach((row) => {
    row.addEventListener("click", (event) => {
      if (event.target instanceof Element && event.target.closest("a")) {
        return;
      }

      focusFilter(row.dataset.dwFilter);
    });
    row.addEventListener("keydown", (event) => {
      if (event.target instanceof Element && event.target.closest("a")) {
        return;
      }

      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        focusFilter(row.dataset.dwFilter);
      }
    });
  });

  syncFilterUI();
}

function bindConceptNavigation() {
  appRoot.querySelectorAll("[data-concept-nav]").forEach((card) => {
    const navigateToConcept = () => {
      const href = card.getAttribute("data-concept-href");
      const conceptCode = card.getAttribute("data-concept-code");
      if (!href || !conceptCode) {
        return;
      }

      captureDetailReturnState({ conceptCode });
      navigate(href);
    };

    card.addEventListener("click", (event) => {
      if (!(event.target instanceof Element)) {
        navigateToConcept();
        return;
      }

      const interactiveChild = event.target.closest("a,button");
      if (interactiveChild && interactiveChild !== card) {
        return;
      }

      navigateToConcept();
    });

    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        navigateToConcept();
      }
    });
  });
}

function bindDemandNavigationButtons() {
  appRoot.querySelector("[data-demand-back]")?.addEventListener("click", (event) => {
    event.preventDefault();
    prepareOverviewRestore();
    navigate(buildDemandOverviewHref());
  });

  appRoot.querySelector("[data-demand-home]")?.addEventListener("click", () => {
    activeFilters.clear();
    detailReturnState = null;
    pendingOverviewRestore = null;
    navigate(buildDemandOverviewHref());
  });

  appRoot.querySelector("[data-retry-demand]")?.addEventListener("click", () => {
    conceptDetailCache.clear();
    void renderRoute({ force: true });
  });
}

function bindDemandWatchInteractions() {
  if (currentRoute().view === "overview") {
    bindFilterBar();
    bindMoversToggle();
    bindConceptNavigation();
  }

  bindDemandNavigationButtons();
  bindTierExplainers();
}

async function hydrateCalendarLinks(releaseItems) {
  const candidates = (Array.isArray(releaseItems) ? releaseItems : []).filter((item) => normalizeTimestamp(item?.scheduledFor));
  if (!candidates.length) {
    return Array.isArray(releaseItems) ? releaseItems : [];
  }

  const timestamps = candidates.map((item) => normalizeTimestamp(item.scheduledFor)).filter(Number.isFinite);
  const from = new Date(Math.min(...timestamps));
  const to = new Date(Math.max(...timestamps));

  try {
    const calendarEvents = await loadCalendarEvents({ from, to });
    return releaseItems.map((item) => {
      const calendarEvent = findCalendarEventForDemandRelease(item, calendarEvents);
      if (!calendarEvent) {
        return {
          ...item,
          calendarEventId: null,
          calendarHref: null,
        };
      }

      return {
        ...item,
        calendarEventId: calendarEvent.id,
        calendarHref: buildCalendarEventHref(calendarEvent.id),
      };
    });
  } catch {
    return releaseItems.map((item) => ({
      ...item,
      calendarEventId: null,
      calendarHref: null,
    }));
  }
}

async function ensurePageData({ force = false } = {}) {
  if (force) {
    currentPageData = null;
    currentVerticalErrors = new Map();
  }

  if (currentPageData && !force) {
    return currentPageData;
  }

  renderPageState({
    title: "Loading Demand Pulse",
    copy: "Loading current demand signals.",
  });

  const payload = await fetchDemandWatchPageData({ force });
  const pageData = buildDemandWatchPageModel(payload);
  currentVerticalErrors = new Map(payload.verticalErrors.map((item) => [item.verticalId, item.message]));

  if (!pageData.scorecard.length && !pageData.verticals.length) {
    renderPageState({
      title: "Demand data unavailable",
      copy: "No published demand signals are available right now.",
      buttonLabel: "Retry",
      buttonAttr: 'data-retry-demand="true"',
    });
    bindDemandNavigationButtons();
    return null;
  }

  currentPageData = pageData;
  return pageData;
}

async function getConceptDetailData(verticalId, conceptCode) {
  const cacheKey = `${verticalId}:${conceptCode}`;
  if (conceptDetailCache.has(cacheKey)) {
    return conceptDetailCache.get(cacheKey);
  }

  const promise = fetchDemandConceptDetail(verticalId, conceptCode)
    .then(async (payload) => {
      const concept = mapDemandConceptDetail(payload);
      concept.calendar = (await hydrateCalendarLinks(concept.calendar)).filter((item) => item.calendarHref);
      conceptDetailCache.set(cacheKey, concept);
      return concept;
    })
    .catch((error) => {
      conceptDetailCache.delete(cacheKey);
      throw error;
    });

  conceptDetailCache.set(cacheKey, promise);
  return promise;
}

async function renderOverviewRoute({ force = false } = {}) {
  try {
    const pageData = await ensurePageData({ force });
    if (!pageData) {
      return;
    }

    activeConceptDetail = null;
    setDocumentTitle("Demand Pulse — DemandWatch | CommodityWatch");
    renderApp(pageData);
    bindDemandWatchInteractions();

    if (pendingOverviewRestore) {
      activeFilters = new Set(pendingOverviewRestore.filters || []);
      syncFilterUI();
      restoreOverviewPosition();
      return;
    }

    scrollToHashOnLoad();
  } catch (error) {
    const message = error instanceof Error && error.message ? error.message : "DemandWatch data could not be loaded.";
    currentVerticalErrors = new Map();
    setDocumentTitle("DemandWatch unavailable | CommodityWatch");
    renderPageState({
      title: "DemandWatch unavailable",
      copy: message,
      buttonLabel: "Retry",
      buttonAttr: 'data-retry-demand="true"',
    });
    bindDemandNavigationButtons();
  }
}

async function renderConceptRoute(route) {
  if (filterRoot) {
    filterRoot.innerHTML = "";
  }

  renderPageState({
    title: "Loading concept detail",
    copy: "Loading detailed concept view.",
  });

  try {
    const concept = await getConceptDetailData(route.verticalId, route.conceptCode);
    activeConceptDetail = concept;
    setDocumentTitle(`${concept.title} — DemandWatch | CommodityWatch`);
    appRoot.innerHTML = renderConceptDetail(concept);
    bindDemandWatchInteractions();
  } catch (error) {
    const message = error instanceof Error && error.message ? error.message : "DemandWatch concept detail is unavailable.";
    setDocumentTitle("DemandWatch concept unavailable | CommodityWatch");
    renderPageState({
      title: "DemandWatch concept unavailable",
      copy: message,
      buttonLabel: "Back to Demand Pulse",
      buttonAttr: 'data-demand-home="true"',
    });
    bindDemandNavigationButtons();
  }
}

async function renderRoute({ force = false } = {}) {
  closeTierExplainer();
  const route = currentRoute();

  if (route.view === "not-found") {
    setDocumentTitle("DemandWatch route unavailable | CommodityWatch");
    renderPageState({
      title: "DemandWatch route unavailable",
      copy: route.reason || "The requested DemandWatch route does not exist.",
      buttonLabel: "Back to Demand Pulse",
      buttonAttr: 'data-demand-home="true"',
    });
    bindDemandNavigationButtons();
    return;
  }

  if (route.view === "detail" && route.verticalId && route.conceptCode) {
    await renderConceptRoute(route);
    return;
  }

  await renderOverviewRoute({ force });
}

function bindToTopButton() {
  if (!toTopButton) {
    return;
  }

  const syncVisibility = () => {
    toTopButton.classList.toggle("visible", window.scrollY > 520);
  };

  window.addEventListener(
    "scroll",
    () => {
      closeTierExplainer();
      syncVisibility();
    },
    { passive: true }
  );
  syncVisibility();

  toTopButton.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });
}

function scrollToHashOnLoad() {
  if (!window.location.hash) {
    return;
  }

  const targetId = window.location.hash.slice(1);
  const isVertical = DEMAND_VERTICALS.some((vertical) => vertical.id === targetId);
  if (isVertical) {
    focusFilter(targetId);
  }
}

export async function loadDemandWatch({ force = false } = {}) {
  if (force) {
    moversExpanded = false;
    conceptDetailCache.clear();
  }

  await renderRoute({ force });
}

let initialDemandWatchLoadPromise = null;

export function initDemandWatch() {
  if (!initialDemandWatchLoadPromise) {
    bindToTopButton();
    window.addEventListener("popstate", () => {
      if (currentRoute().view === "overview") {
        queueOverviewRestore();
      }
      void renderRoute();
    });
    initialDemandWatchLoadPromise = loadDemandWatch();
  }

  return initialDemandWatchLoadPromise;
}

export const initialDemandWatchLoad = initDemandWatch();
