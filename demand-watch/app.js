import {
  DEMAND_GAP_NOTES,
  DEMAND_MACRO_STRIP,
  DEMAND_MOVERS,
  DEMAND_TAXONOMY,
  DEMAND_VERTICALS,
  getDemandVerticalById,
} from "./data.js";

const VERTICAL_SECTOR_MAP = {
  "crude-products": "energy",
  "electricity": "natural-gas",
  "grains": "agriculture",
  "base-metals": "metals_and_mining",
};

// Empty set = "All" mode. Non-empty = show only those vertical IDs.
let activeFilters = new Set();

const appRoot = document.getElementById("demand-root");
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
      return "Trending above seasonal norm";
    case "down":
      return "Tracking below seasonal norm";
    default:
      return "Tracking near seasonal norm";
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

function renderMacroStrip() {
  return `
    <div class="demand-backdrop-wrap">
      <p class="section-kicker demand-backdrop-kicker">Demand Backdrop</p>
      <section class="macro-strip" aria-label="Demand macro context">
        ${DEMAND_MACRO_STRIP.map(
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
        ).join("")}
      </section>
    </div>
  `;
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
        &nbsp;&nbsp;
        <span class="sparkline-legend-dot sparkline-legend-proxy">●</span> Proxy / trade flow
      </p>
    </section>
  `;
}

function renderScorecard() {
  return `
    <section class="dw-card" data-dw-overview="scorecard">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Scorecard</p>
        <h2 class="section-title">Where does demand stand?</h2>
      </div>
      <div class="scorecard-table" role="table" aria-label="Demand scorecard — click a row to drill into the vertical">
        <div class="scorecard-head" role="row">
          <span role="columnheader">Vertical</span>
          <span role="columnheader">Signal</span>
          <span role="columnheader">YoY</span>
          <span role="columnheader">Direction</span>
          <span role="columnheader">As of</span>
          <span role="columnheader">Updated</span>
        </div>
        ${DEMAND_VERTICALS.map(
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
        ).join("")}
      </div>
    </section>
  `;
}

function renderMovers() {
  return `
    <section class="dw-card">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Movers</p>
        <h2 class="section-title">Latest releases</h2>
      </div>
      <div class="mover-grid">
        ${DEMAND_MOVERS.map((mover) => {
          const vertical = getDemandVerticalById(mover.verticalId);
          return `
            <article class="mover-card ${escapeHtml(signalClassForTrend(mover.trend))}" style="--vertical-accent:${escapeHtml(vertical?.accent || "var(--color-amber)")};" data-vertical="${escapeHtml(mover.verticalId)}">
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
        }).join("")}
      </div>
    </section>
  `;
}

function renderIndicatorCard(indicator, accent) {
  const arrowTooltip = trendArrowTooltip(indicator.trend);
  const valueHtml = indicator.valueHref
    ? `<a href="${escapeHtml(indicator.valueHref)}" class="indicator-value-link">${escapeHtml(indicator.value)}</a>`
    : escapeHtml(indicator.value);

  return `
    <article class="indicator-card ${escapeHtml(signalClassForTrend(indicator.trend))}" style="--vertical-accent:${escapeHtml(accent)};">
      <div class="indicator-head">
        <span class="tier-badge">${escapeHtml(indicator.tier)}</span>
        <span class="indicator-trend" title="${escapeHtml(arrowTooltip)}" aria-label="${escapeHtml(arrowTooltip)}">${escapeHtml(trendArrow(indicator.trend))}</span>
      </div>
      <h4 class="indicator-title">${escapeHtml(indicator.title)}</h4>
      <p class="indicator-value">${valueHtml}</p>
      <p class="indicator-change">${escapeHtml(indicator.change)}</p>
      <div class="indicator-chart">
        ${renderSparkline(indicator.sparkline, indicator.trend)}
        <p class="sparkline-range">12-month trend</p>
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
                <tr>
                  <th scope="row">${escapeHtml(row[0])}</th>
                  <td>${escapeHtml(row[1])}</td>
                  <td>${escapeHtml(row[2])}</td>
                  <td>${escapeHtml(row[3])}</td>
                  <td>${escapeHtml(row[4])}</td>
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
                <article class="detail-block">
                  <div class="detail-block-head">
                    <div>
                      <p class="detail-kicker">${escapeHtml(section.title)}</p>
                      <h3>${escapeHtml(section.description)}</h3>
                    </div>
                  </div>
                  <div class="indicator-grid">
                    ${section.indicators.map((indicator) => renderIndicatorCard(indicator, vertical.accent)).join("")}
                  </div>
                  ${renderDataTable(section.tableRows)}
                </article>
              `
            )
            .join("")}
        </div>
        <aside class="vertical-sidebar">
          <section class="sidebar-card">
            <p class="detail-kicker">Data Calendar</p>
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
          </section>
          <section class="sidebar-card">
            <p class="detail-kicker">Coverage Notes</p>
            <ul class="sidebar-list">
              ${vertical.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")}
            </ul>
          </section>
        </aside>
      </div>
    </section>
  `;
}


function renderApp() {
  const filterRoot = document.getElementById("demand-filter-root");
  if (filterRoot) {
    filterRoot.innerHTML = renderFilterBar();
  }

  appRoot.innerHTML = `
    <div class="demand-shell">
      <section class="hero-card">
        <div class="hero-copy">
          <p class="hero-kicker">DemandWatch</p>
          <h1 class="hero-title">Demand Pulse</h1>
          <p class="hero-subtitle">
            Direct consumption, ranked by signal quality. Proxies where they add signal. Gaps are named, not estimated.
          </p>
          <div class="hero-stats">
            <span class="hero-stat"><strong>4</strong> verticals</span>
            <span class="hero-stat"><strong>7</strong> signal tiers</span>
            <span class="hero-stat"><strong>20+</strong> indicators</span>
          </div>
        </div>
        <div class="hero-summary">
          <p class="hero-summary-label">Current signal</p>
          <p class="hero-summary-value">Above seasonal norms, still improving.</p>
          <p class="hero-summary-copy">
            Crude + products, electricity, grains, and base metals — tiered signals, sourced to the release, freshness labeled on every datapoint.
          </p>
        </div>
      </section>

      ${renderMacroStrip()}

      <section id="overview" class="dw-section overview-section" data-dw-section="overview">
        ${renderScorecard()}
        ${renderMovers()}
        ${renderTaxonomy()}
      </section>

      ${DEMAND_VERTICALS.map((vertical) => renderVerticalSection(vertical)).join("")}
    </div>
  `;
}

// Apply current activeFilters state to the DOM.
function syncFilterUI() {
  const allMode = activeFilters.size === 0;

  // Pills: in All mode every pill (including verticals) gets is-selected underline.
  // In filtered mode only the "All" pill loses its underline; selected verticals keep it.
  document.querySelectorAll(".filter-pill[data-filter]").forEach((pill) => {
    const active =
      pill.dataset.filter === "all"
        ? allMode
        : allMode || activeFilters.has(pill.dataset.filter);
    pill.classList.toggle("is-selected", active);
    pill.setAttribute("aria-pressed", active ? "true" : "false");
  });

  const moverCards = document.querySelectorAll(".mover-card[data-vertical]");
  const verticalSections = document.querySelectorAll("[data-dw-vertical]");
  const overviewOnlyItems = document.querySelectorAll("[data-dw-overview]");

  if (allMode) {
    moverCards.forEach((c) => c.classList.remove("is-filtered-out"));
    verticalSections.forEach((s) => s.classList.remove("is-filtered-out"));
    overviewOnlyItems.forEach((s) => s.classList.remove("is-filtered-out"));
  } else {
    // Hide cross-vertical scorecard + taxonomy in focused mode
    overviewOnlyItems.forEach((s) => s.classList.add("is-filtered-out"));
    moverCards.forEach((c) => {
      c.classList.toggle("is-filtered-out", !activeFilters.has(c.dataset.vertical));
    });
    verticalSections.forEach((s) => {
      s.classList.toggle("is-filtered-out", !activeFilters.has(s.dataset.dwVertical));
    });
    // Auto-scroll only when exactly one vertical is in view
    if (activeFilters.size === 1) {
      const [targetId] = [...activeFilters];
      const target = document.getElementById(targetId);
      if (target) {
        setTimeout(() => target.scrollIntoView({ behavior: "smooth", block: "start" }), 50);
      }
    }
  }
}

// Toggle behavior — used by filter bar pill clicks.
// From "All" mode: single-select the clicked vertical.
// From filtered mode: toggle the vertical in/out of the active set.
function toggleFilter(filterId) {
  if (filterId === "all") {
    activeFilters.clear();
  } else if (activeFilters.size === 0) {
    // Was in All mode — switch to single-select
    activeFilters = new Set([filterId]);
  } else if (activeFilters.has(filterId)) {
    activeFilters.delete(filterId);
    // If that was the last one, snap back to All mode (empty set)
  } else {
    activeFilters.add(filterId);
    // If all verticals are now manually selected, collapse back to All mode
    if (activeFilters.size === DEMAND_VERTICALS.length) {
      activeFilters.clear();
    }
  }
  syncFilterUI();
}

// Focus behavior — used by scorecard row clicks (always single-select).
// Clicking the already-focused vertical returns to All mode.
function focusFilter(filterId) {
  if (activeFilters.size === 1 && activeFilters.has(filterId)) {
    activeFilters.clear();
  } else {
    activeFilters = new Set([filterId]);
  }
  syncFilterUI();
}

function bindFilterBar() {
  const filterRoot = document.getElementById("demand-filter-root");
  if (filterRoot) {
    filterRoot.querySelectorAll(".filter-pill[data-filter]").forEach((pill) => {
      pill.addEventListener("click", () => toggleFilter(pill.dataset.filter));
    });
  }

  // Scorecard rows are single-focus: always narrow to one vertical
  appRoot.querySelectorAll(".scorecard-row[data-dw-filter]").forEach((row) => {
    row.addEventListener("click", () => focusFilter(row.dataset.dwFilter));
  });

  // Sync initial visual state (All mode: every pill gets its underline)
  syncFilterUI();
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
  if (!window.location.hash) return;
  const targetId = window.location.hash.slice(1);
  const isVertical = DEMAND_VERTICALS.some((v) => v.id === targetId);
  if (isVertical) {
    focusFilter(targetId);
  }
}

renderApp();
bindFilterBar();
bindToTopButton();
scrollToHashOnLoad();
