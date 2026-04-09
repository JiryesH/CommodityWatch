import { fetchDemandWatchPageData } from "./api-client.js";
import { DEMAND_TAXONOMY, DEMAND_VERTICALS, buildDemandWatchPageModel, getDemandVerticalById } from "./data.js";

const VERTICAL_SECTOR_MAP = {
  "crude-products": "energy",
  electricity: "natural-gas",
  grains: "agriculture",
  "base-metals": "metals_and_mining",
};

let activeFilters = new Set();
let currentVerticalErrors = new Map();

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

function signalWordForTrend(trend) {
  switch (trend) {
    case "up":
      return "Improving";
    case "down":
      return "Deteriorating";
    default:
      return "Stable";
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

function renderInlineEmpty(copy) {
  return `<p class="dw-inline-empty">${escapeHtml(copy)}</p>`;
}

function renderFilterBar() {
  return `
    <nav class="filter-wrap" aria-label="DemandWatch filter">
      <div class="filter-bar">
        <button
          class="filter-pill"
          data-filter="all"
          data-sector="ALL"
          aria-pressed="false"
        >All</button>
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

function renderMacroStrip(items) {
  const cards = items.length
    ? items
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
              <p class="macro-freshness">${escapeHtml(item.freshness)}</p>
            </article>
          `
        )
        .join("")
    : `
        <article class="macro-card macro-card--empty">
          <p class="macro-label">Macro backdrop</p>
          <p class="macro-value">Awaiting data</p>
          <p class="macro-change">No live macro strip items are available yet.</p>
        </article>
      `;

  return `
    <div class="demand-backdrop-wrap">
      <p class="section-kicker demand-backdrop-kicker">Demand Backdrop</p>
      <section class="macro-strip" aria-label="Demand macro context">
        ${cards}
      </section>
    </div>
  `;
}

function renderTaxonomy() {
  return `
    <section class="dw-card" data-dw-overview="taxonomy">
      <div class="dw-card-head">
        <p class="section-kicker">Signal Tiers</p>
        <h2 class="section-title">Every indicator carries a tier. Every tier has a reliability standard.</h2>
      </div>
      <div class="taxonomy-grid">
        ${DEMAND_TAXONOMY.map(
          (tier) => `
            <article class="taxonomy-card">
              <p class="tier-badge">${escapeHtml(tier.shortLabel)}</p>
              <h3>${escapeHtml(tier.label)}</h3>
              <p class="taxonomy-copy">${escapeHtml(tier.description)}</p>
              <p class="taxonomy-reliability">${escapeHtml(tier.reliability)} reliability</p>
            </article>
          `
        ).join("")}
      </div>
      <p class="sparkline-legend">
        <span class="sparkline-legend-dot sparkline-legend-direct">●</span> Direct measurement
        <span class="sparkline-legend-gap"></span>
        <span class="sparkline-legend-dot sparkline-legend-proxy">●</span> Proxy / trade flow
      </p>
    </section>
  `;
}

function renderScorecard(items) {
  return `
    <section class="dw-card" data-dw-overview="scorecard">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Scorecard</p>
        <h2 class="section-title">Where does demand stand?</h2>
      </div>
      ${
        items.length
          ? `
              <div class="scorecard-table" role="table" aria-label="Demand scorecard - click a row to drill into the vertical">
                <div class="scorecard-head" role="row">
                  <span role="columnheader">Vertical</span>
                  <span role="columnheader">Signal</span>
                  <span role="columnheader">YoY</span>
                  <span role="columnheader">Direction</span>
                  <span role="columnheader">As of</span>
                  <span role="columnheader">Updated</span>
                </div>
                ${items
                  .map(
                    (vertical) => `
                      <button
                        class="scorecard-row ${escapeHtml(signalClassForTrend(vertical.scorecard.trend))}"
                        role="row"
                        data-dw-filter="${escapeHtml(vertical.id)}"
                        style="--vertical-accent:${escapeHtml(vertical.accent)};"
                      >
                        <span class="scorecard-vertical" role="cell">
                          <span class="scorecard-dot"></span>
                          ${escapeHtml(vertical.shortLabel)}
                        </span>
                        <span role="cell">
                          <strong>${escapeHtml(vertical.scorecard.label)}</strong>
                          <span class="scorecard-secondary">${escapeHtml(vertical.scorecard.value)}</span>
                        </span>
                        <span role="cell">${escapeHtml(vertical.scorecard.yoyLabel)}</span>
                        <span role="cell">${escapeHtml(signalWordForTrend(vertical.scorecard.trend))}</span>
                        <span role="cell">${escapeHtml(vertical.scorecard.latestData)}</span>
                        <span role="cell" class="scorecard-freshness-cell">
                          ${escapeHtml(vertical.scorecard.freshness)}
                          <span class="scorecard-drill" aria-hidden="true">→</span>
                        </span>
                      </button>
                    `
                  )
                  .join("")}
              </div>
            `
          : renderInlineEmpty("No live scorecard items are available yet.")
      }
    </section>
  `;
}

function renderMovers(movers) {
  return `
    <section class="dw-card" data-dw-overview="movers">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Movers</p>
        <h2 class="section-title">Latest releases</h2>
      </div>
      ${
        movers.length
          ? `
              <div class="mover-grid">
                ${movers
                  .map((mover) => {
                    const vertical = getDemandVerticalById(mover.verticalId);
                    return `
                      <article class="mover-card ${escapeHtml(signalClassForTrend(mover.trend))}" style="--vertical-accent:${escapeHtml(vertical?.accent || mover.accent || "var(--color-amber)")};" data-vertical="${escapeHtml(mover.verticalId)}">
                        <div class="mover-head">
                          <p class="mover-vertical">${escapeHtml(vertical?.shortLabel || "Demand")}</p>
                          <span class="tier-badge">${escapeHtml(mover.tier)}</span>
                        </div>
                        <h3 class="mover-title">${escapeHtml(mover.title)}</h3>
                        <p class="mover-value">${escapeHtml(mover.value)}</p>
                        <p class="mover-change">${escapeHtml(mover.change)}</p>
                        <p class="mover-surprise">${escapeHtml(mover.surprise)}</p>
                        <p class="mover-freshness">${escapeHtml(mover.freshness)}</p>
                      </article>
                    `;
                  })
                  .join("")}
              </div>
            `
          : renderInlineEmpty("No recent DemandWatch movers are available yet.")
      }
    </section>
  `;
}

function renderIndicatorCard(indicator, accent) {
  const trendTooltip = trendArrowTooltip(indicator.trend);
  const valueHtml = indicator.valueHref
    ? `<a href="${escapeHtml(indicator.valueHref)}" class="indicator-value-link">${escapeHtml(indicator.value)}</a>`
    : escapeHtml(indicator.value);
  const headMeta = indicator.placeholder
    ? `<span class="indicator-status is-${escapeHtml(indicator.coverageTone)}">${escapeHtml(indicator.coverageLabel)}</span>`
    : `<span class="indicator-trend" title="${escapeHtml(trendTooltip)}" aria-label="${escapeHtml(trendTooltip)}">${escapeHtml(trendArrow(indicator.trend))}</span>`;

  return `
    <article class="indicator-card ${escapeHtml(signalClassForTrend(indicator.trend))}${indicator.placeholder ? " is-placeholder" : ""}" style="--vertical-accent:${escapeHtml(accent)};">
      <div class="indicator-head">
        <span class="tier-badge">${escapeHtml(indicator.tier)}</span>
        ${headMeta}
      </div>
      <h4 class="indicator-title">${escapeHtml(indicator.title)}</h4>
      <p class="indicator-value">${valueHtml}</p>
      <p class="indicator-change">${escapeHtml(indicator.change)}</p>
      <div class="indicator-chart">
        ${renderSparkline(indicator.sparkline, indicator.trend)}
        <p class="sparkline-range">${indicator.placeholder ? "Placeholder" : "Recent trend"}</p>
      </div>
      <p class="indicator-detail">${escapeHtml(indicator.detail)}</p>
    </article>
  `;
}

function renderDataTable(rows) {
  return `
    <div class="detail-table-wrap">
      <table class="detail-table">
        <thead>
          <tr>
            <th scope="col">Indicator</th>
            <th scope="col">Latest</th>
            <th scope="col">vs prior</th>
            <th scope="col">YoY</th>
            <th scope="col">Freshness</th>
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
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderVerticalSection(vertical) {
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
          <p class="vertical-summary">${escapeHtml(vertical.summary)}</p>
        </div>
        <div class="vertical-facts">
          ${vertical.facts
            .map(
              (fact) => `
                <article class="fact-card">
                  <p class="fact-label">${escapeHtml(fact.label)}</p>
                  <p class="fact-value">${escapeHtml(fact.value)}</p>
                  <p class="fact-note">${escapeHtml(fact.note)}</p>
                </article>
              `
            )
            .join("")}
        </div>
      </div>
      <div class="vertical-layout">
        <div class="vertical-main">
          ${vertical.sections
            .map(
              (section) => `
                <article class="detail-block${section.isCoverageGap ? " is-gap-block" : ""}">
                  <div class="detail-block-head">
                    <div>
                      <p class="detail-kicker">${escapeHtml(section.title)}</p>
                      <h3>${escapeHtml(section.description)}</h3>
                    </div>
                  </div>
                  ${
                    section.indicators.length
                      ? `<div class="indicator-grid">${section.indicators.map((indicator) => renderIndicatorCard(indicator, vertical.accent)).join("")}</div>`
                      : renderInlineEmpty("No indicators are available in this section yet.")
                  }
                  ${section.tableRows.length ? renderDataTable(section.tableRows) : ""}
                </article>
              `
            )
            .join("")}
        </div>
        <aside class="vertical-sidebar">
          <section class="sidebar-card">
            <p class="detail-kicker">Data Calendar</p>
            ${
              vertical.calendar.length
                ? `
                    <div class="calendar-list">
                      ${vertical.calendar
                        .map(
                          (item) => `
                            <article class="calendar-item">
                              <p class="calendar-item-label">${escapeHtml(item.label)}</p>
                              <p class="calendar-item-value">${escapeHtml(item.value)}</p>
                              <p class="calendar-item-note">${escapeHtml(item.note)}</p>
                            </article>
                          `
                        )
                        .join("")}
                    </div>
                  `
                : renderInlineEmpty("No release timing is available for this vertical yet.")
            }
          </section>
          <section class="sidebar-card">
            <p class="detail-kicker">Coverage Notes</p>
            ${
              vertical.notes.length
                ? `<ul class="sidebar-list">${vertical.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")}</ul>`
                : renderInlineEmpty("No coverage notes are available yet.")
            }
          </section>
        </aside>
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
          <p class="vertical-summary">${escapeHtml(copy)}</p>
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

  appRoot.innerHTML = `
    <div class="demand-shell">
      <section class="hero-card hero-card--state">
        <div class="dw-state-card">
          <p class="hero-kicker">DemandWatch</p>
          <h1 class="hero-title">${escapeHtml(title)}</h1>
          <p class="hero-subtitle">${escapeHtml(copy)}</p>
          ${
            buttonLabel
              ? `
                  <div class="dw-state-actions">
                    <button class="dw-state-button" type="button" ${buttonAttr}>${escapeHtml(buttonLabel)}</button>
                  </div>
                `
              : ""
          }
        </div>
      </section>
    </div>
  `;
}

function renderApp(pageData) {
  if (filterRoot) {
    filterRoot.innerHTML = renderFilterBar();
  }

  const verticalsById = new Map(pageData.verticals.map((vertical) => [vertical.id, vertical]));
  const verticalMarkup = DEMAND_VERTICALS.map((meta) => {
    const vertical = verticalsById.get(meta.id);
    if (vertical) {
      return renderVerticalSection(vertical);
    }

    if (currentVerticalErrors.has(meta.id)) {
      return renderVerticalStateSection(meta, "Detail unavailable", currentVerticalErrors.get(meta.id), "error");
    }

    return renderVerticalStateSection(meta, "Awaiting detail", "No live DemandWatch detail is available for this vertical yet.");
  }).join("");

  appRoot.innerHTML = `
    <div class="demand-shell">
      <section class="hero-card">
        <div class="hero-copy">
          <p class="hero-kicker">DemandWatch</p>
          <h1 class="hero-title">Demand Pulse</h1>
          <p class="hero-subtitle">
            Direct consumption where it is legally safe, proxies where they add signal, and explicit placeholders where coverage remains blocked or deferred.
          </p>
          <div class="hero-stats">
            ${pageData.hero.stats
              .map(
                (stat) => `
                  <span class="hero-stat"><strong>${escapeHtml(stat.value)}</strong>${escapeHtml(stat.label)}</span>
                `
              )
              .join("")}
          </div>
        </div>
        <div class="hero-summary">
          <p class="hero-summary-label">${escapeHtml(pageData.hero.label)}</p>
          <p class="hero-summary-value">${escapeHtml(pageData.hero.value)}</p>
          <p class="hero-summary-copy">${escapeHtml(pageData.hero.copy)}</p>
        </div>
      </section>

      ${renderMacroStrip(pageData.macroStrip)}

      <section id="overview" class="dw-section overview-section" data-dw-section="overview">
        ${renderScorecard(pageData.scorecard)}
        ${renderMovers(pageData.movers)}
        ${renderTaxonomy()}
      </section>

      ${verticalMarkup}
    </div>
  `;
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
      setTimeout(() => target.scrollIntoView({ behavior: "smooth", block: "start" }), 50);
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
    row.addEventListener("click", () => focusFilter(row.dataset.dwFilter));
  });

  syncFilterUI();
}

function bindRetryButton() {
  appRoot.querySelector("[data-retry-demand]")?.addEventListener("click", () => {
    loadDemandWatch({ force: true });
  });
}

function bindToTopButton() {
  if (!toTopButton) {
    return;
  }

  const syncVisibility = () => {
    toTopButton.classList.toggle("visible", window.scrollY > 520);
  };

  window.addEventListener("scroll", syncVisibility, { passive: true });
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
  renderPageState({
    title: "Loading Demand Pulse",
    copy: "Fetching the live DemandWatch snapshot from the backend.",
  });

  try {
    const payload = await fetchDemandWatchPageData({ force });
    const pageData = buildDemandWatchPageModel(payload);
    currentVerticalErrors = new Map(payload.verticalErrors.map((item) => [item.verticalId, item.message]));

    if (!pageData.scorecard.length && !pageData.verticals.length) {
      renderPageState({
        title: "Demand data unavailable",
        copy: "No live DemandWatch payload is currently available from the backend.",
        buttonLabel: "Retry",
        buttonAttr: 'data-retry-demand="true"',
      });
      bindRetryButton();
      return;
    }

    renderApp(pageData);
    bindFilterBar();
    scrollToHashOnLoad();
  } catch (error) {
    const message = error instanceof Error && error.message ? error.message : "Live DemandWatch data could not be loaded.";
    currentVerticalErrors = new Map();

    renderPageState({
      title: "DemandWatch unavailable",
      copy: message,
      buttonLabel: "Retry",
      buttonAttr: 'data-retry-demand="true"',
    });
    bindRetryButton();
  }
}

let initialDemandWatchLoadPromise = null;

export function initDemandWatch() {
  if (!initialDemandWatchLoadPromise) {
    bindToTopButton();
    initialDemandWatchLoadPromise = loadDemandWatch();
  }

  return initialDemandWatchLoadPromise;
}

export const initialDemandWatchLoad = initDemandWatch();
