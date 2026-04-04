import {
  DEMAND_GAP_NOTES,
  DEMAND_MACRO_STRIP,
  DEMAND_MOVERS,
  DEMAND_ROADMAP,
  DEMAND_SCOPE_NOTES,
  DEMAND_TAXONOMY,
  DEMAND_VERTICALS,
  getDemandVerticalById,
} from "./data.js";

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
    <section class="macro-strip" aria-label="Demand macro context">
      ${DEMAND_MACRO_STRIP.map(
        (item) => `
          <article class="macro-card ${escapeHtml(signalClassForTrend(item.trend))}">
            <p class="macro-label">${escapeHtml(item.label)}</p>
            <p class="macro-value">${escapeHtml(item.value)}</p>
            <p class="macro-change">
              <span class="macro-arrow" aria-hidden="true">${escapeHtml(trendArrow(item.trend))}</span>
              ${escapeHtml(item.change)}
            </p>
            <p class="macro-freshness">${escapeHtml(item.freshness)}</p>
          </article>
        `
      ).join("")}
    </section>
  `;
}

function renderSectionNav() {
  const items = [
    { id: "overview", label: "Overview" },
    ...DEMAND_VERTICALS.map((vertical) => ({ id: vertical.id, label: vertical.navLabel })),
    { id: "coverage", label: "Coverage" },
  ];

  return `
    <nav class="section-nav" aria-label="DemandWatch sections">
      ${items
        .map(
          (item, index) => `
            <a
              class="section-nav-link${index === 0 ? " is-active" : ""}"
              href="#${escapeHtml(item.id)}"
              data-dw-nav="${escapeHtml(item.id)}"
              aria-current="${index === 0 ? "true" : "false"}"
            >
              ${escapeHtml(item.label)}
            </a>
          `
        )
        .join("")}
    </nav>
  `;
}

function renderTaxonomy() {
  return `
    <section class="dw-card">
      <div class="dw-card-head">
        <p class="section-kicker">Indicator Tiers</p>
        <h2 class="section-title">Demand data stays explicit about signal quality.</h2>
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
    </section>
  `;
}

function renderScorecard() {
  return `
    <section class="dw-card">
      <div class="dw-card-head">
        <p class="section-kicker">Demand Scorecard</p>
        <h2 class="section-title">The launch table answers one question: hot, cold, or flat?</h2>
      </div>
      <div class="scorecard-table" role="table" aria-label="Demand pulse scorecard">
        <div class="scorecard-head" role="row">
          <span role="columnheader">Vertical</span>
          <span role="columnheader">Direct signal</span>
          <span role="columnheader">YoY</span>
          <span role="columnheader">Trend</span>
          <span role="columnheader">Latest data</span>
          <span role="columnheader">Freshness</span>
        </div>
        ${DEMAND_VERTICALS.map(
          (vertical) => `
            <a
              class="scorecard-row ${escapeHtml(signalClassForTrend(vertical.scorecard.trend))}"
              role="row"
              href="#${escapeHtml(vertical.id)}"
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
              <span role="cell">${escapeHtml(vertical.scorecard.freshness)}</span>
            </a>
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
        <h2 class="section-title">Latest releases, stripped down to what changed.</h2>
      </div>
      <div class="mover-grid">
        ${DEMAND_MOVERS.map((mover) => {
          const vertical = getDemandVerticalById(mover.verticalId);
          return `
            <article class="mover-card ${escapeHtml(signalClassForTrend(mover.trend))}" style="--vertical-accent:${escapeHtml(vertical?.accent || "var(--color-amber)")};">
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

function renderScopeNotes() {
  return `
    <section class="scope-grid" aria-label="Launch scope notes">
      ${DEMAND_SCOPE_NOTES.map(
        (note) => `
          <article class="scope-card">
            <h3>${escapeHtml(note.title)}</h3>
            <p>${escapeHtml(note.copy)}</p>
          </article>
        `
      ).join("")}
    </section>
  `;
}

function renderIndicatorCard(indicator, accent) {
  return `
    <article class="indicator-card ${escapeHtml(signalClassForTrend(indicator.trend))}" style="--vertical-accent:${escapeHtml(accent)};">
      <div class="indicator-head">
        <span class="tier-badge">${escapeHtml(indicator.tier)}</span>
        <span class="indicator-trend">${escapeHtml(trendArrow(indicator.trend))}</span>
      </div>
      <h4 class="indicator-title">${escapeHtml(indicator.title)}</h4>
      <p class="indicator-value">${escapeHtml(indicator.value)}</p>
      <p class="indicator-change">${escapeHtml(indicator.change)}</p>
      <div class="indicator-chart">${renderSparkline(indicator.sparkline, indicator.trend)}</div>
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
      style="--vertical-accent:${escapeHtml(vertical.accent)};"
    >
      <div class="vertical-header">
        <div>
          <p class="section-kicker">${escapeHtml(vertical.shortLabel)}</p>
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

function renderCoverage() {
  return `
    <section id="coverage" class="dw-section coverage-section" data-dw-section="coverage">
      <div class="dw-card-head">
        <p class="section-kicker">Coverage</p>
        <h2 class="section-title">Launch scope stays honest about what is live, linked, and deferred.</h2>
      </div>
      <div class="coverage-grid">
        ${DEMAND_GAP_NOTES.map(
          (note) => `
            <article class="coverage-card">
              <h3>${escapeHtml(note.title)}</h3>
              <p>${escapeHtml(note.copy)}</p>
            </article>
          `
        ).join("")}
      </div>
      <section class="roadmap-card">
        <p class="detail-kicker">Next</p>
        <ul class="sidebar-list">
          ${DEMAND_ROADMAP.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
      </section>
    </section>
  `;
}

function renderApp() {
  appRoot.innerHTML = `
    <div class="demand-shell">
      <section class="hero-card">
        <div class="hero-copy">
          <p class="hero-kicker">DemandWatch</p>
          <h1 class="hero-title">Demand Pulse</h1>
          <p class="hero-subtitle">
            Direct consumption first. Proxies where they add signal. Gaps called out instead of guessed away.
          </p>
        </div>
        <div class="hero-summary">
          <p class="hero-summary-label">Launch answer</p>
          <p class="hero-summary-value">Above seasonal norms, still improving.</p>
          <p class="hero-summary-copy">
            The MVP covers crude + products, electricity, grains, and base metals with tiered demand signals and release discipline built in.
          </p>
        </div>
      </section>

      ${renderMacroStrip()}
      ${renderSectionNav()}

      <section id="overview" class="dw-section overview-section" data-dw-section="overview">
        ${renderTaxonomy()}
        ${renderScorecard()}
        ${renderMovers()}
        ${renderScopeNotes()}
      </section>

      ${DEMAND_VERTICALS.map((vertical) => renderVerticalSection(vertical)).join("")}
      ${renderCoverage()}
    </div>
  `;
}

function syncSectionNav() {
  const navLinks = [...document.querySelectorAll("[data-dw-nav]")];
  const sections = [...document.querySelectorAll("[data-dw-section]")];

  const setActive = (sectionId) => {
    navLinks.forEach((link) => {
      const isActive = link.dataset.dwNav === sectionId;
      link.classList.toggle("is-active", isActive);
      link.setAttribute("aria-current", isActive ? "true" : "false");
    });
  };

  navLinks.forEach((link) => {
    link.addEventListener("click", () => {
      setActive(link.dataset.dwNav);
    });
  });

  if (!("IntersectionObserver" in window)) {
    setActive(window.location.hash ? window.location.hash.slice(1) : "overview");
    return;
  }

  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((left, right) => right.intersectionRatio - left.intersectionRatio)[0];

      if (visible?.target?.dataset?.dwSection) {
        setActive(visible.target.dataset.dwSection);
      }
    },
    {
      rootMargin: "-18% 0px -58% 0px",
      threshold: [0.2, 0.4, 0.6],
    }
  );

  sections.forEach((section) => observer.observe(section));
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

  const target = document.getElementById(window.location.hash.slice(1));
  if (!target) {
    return;
  }

  window.requestAnimationFrame(() => {
    target.scrollIntoView();
  });
}

renderApp();
syncSectionNav();
bindToTopButton();
scrollToHashOnLoad();
