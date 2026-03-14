import { SECTOR_META } from "./config.js";
import {
  clearCommodities,
  createDefaultFilter,
  getAllSectors,
  getExpandedGroupsForSector,
  getFilterLabel,
  getSelectedGroup,
  getSelectedSector,
  getSummaryPills,
  isAllCommoditySelection,
  isAllFilter,
  normalizeFilter,
  readStoredCollapseState,
  readStoredFilter,
  toggleCommodity,
  toggleGroup,
  toggleSector,
  writeStoredCollapseState,
  writeStoredFilter,
} from "./filter-state.js";
import { MODULE_REGISTRY } from "./module-registry.js";

const filterRoot = document.getElementById("home-filter-root");
const dashboardRoot = document.getElementById("dashboard-root");
const toTopButton = document.getElementById("to-top-btn");

let activeFilter = normalizeFilter(readStoredFilter());
let filterCollapsed = readStoredCollapseState();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function handleNavigate(route) {
  window.location.assign(route);
}

function applyFilter(nextFilter) {
  activeFilter = normalizeFilter(nextFilter);
  writeStoredFilter(activeFilter);

  if (isAllFilter(activeFilter)) {
    filterCollapsed = false;
    writeStoredCollapseState(false);
  }

  render();
}

function setCollapsed(nextCollapsed) {
  filterCollapsed = Boolean(nextCollapsed) && !isAllFilter(activeFilter);
  writeStoredCollapseState(filterCollapsed);
  renderFilterBar();
}

function renderSummaryPills() {
  return getSummaryPills(activeFilter)
    .map(
      (pill) => `
        <span class="home-summary-pill${pill.tone === "neutral" ? "" : " has-tone"}" ${
          pill.tone === "neutral" ? "" : `style="--summary-accent:${SECTOR_META[pill.tone]?.accent || "var(--color-cross)"};"`
        }>
          ${escapeHtml(pill.label)}
        </span>
      `
    )
    .join("");
}

function getSectorSelectionState(sectorId) {
  if (activeFilter.sectorId !== sectorId) {
    return "none";
  }

  return activeFilter.groupId ? "partial" : "full";
}

function getGroupSelectionState(groupId) {
  const selectedGroup = getSelectedGroup(activeFilter);
  if (selectedGroup?.id !== groupId) {
    return "none";
  }

  return isAllCommoditySelection(activeFilter) ? "full" : "partial";
}

function renderSectorPills() {
  return getAllSectors()
    .map((sector) => {
      const selectionState = getSectorSelectionState(sector.id);
      const isSelected = selectionState !== "none";
      const isPartial = selectionState === "partial";
      return `
        <button
          class="filter-pill home-sector-pill${isSelected ? " is-selected" : ""}${isPartial ? " is-partial" : ""}"
          type="button"
          data-sector-id="${escapeHtml(sector.id)}"
          aria-pressed="${String(isSelected)}"
          style="--filter-pill-color:${escapeHtml(sector.accent)};"
        >
          ${escapeHtml(sector.label)}
        </button>
      `;
    })
    .join("");
}

function renderGroupLayer() {
  const sector = getSelectedSector(activeFilter);
  if (!sector) {
    return "";
  }

  const selectedGroup = getSelectedGroup(activeFilter);
  const groups = getExpandedGroupsForSector(sector.id);
  const selectedCommodities = new Set(activeFilter.commodityIds);
  const allBenchmarksSelected = isAllCommoditySelection(activeFilter);

  return `
    <div class="filter-layer home-filter-layer open" aria-hidden="false">
      <section class="home-filter-section" aria-label="Commodity group">
        <div class="home-filter-section-head">
          <p class="home-filter-section-label">Commodity group</p>
          ${
            selectedGroup
              ? '<button class="filter-layer-clear" type="button" data-clear-group>clear group</button>'
              : ""
          }
        </div>
        <div class="filter-chip-row home-group-row" style="--sector-accent:${escapeHtml(sector.accent)};">
          ${groups
            .map((group) => {
              const selectionState = getGroupSelectionState(group.id);
              const isSelected = selectionState !== "none";
              const isPartial = selectionState === "partial";

              return `
                <button
                  class="filter-chip home-group-chip${isSelected ? " is-selected" : ""}${isPartial ? " is-partial" : ""}"
                  type="button"
                  data-group-id="${escapeHtml(group.id)}"
                  aria-pressed="${String(isSelected)}"
                >
                  <span class="filter-chip-label">${escapeHtml(group.label)}</span>
                </button>
              `;
            })
            .join("")}
        </div>
      </section>
      ${
        selectedGroup && selectedGroup.commodities.length
          ? `
            <section class="home-filter-section home-filter-commodity-row" aria-label="Specific commodity">
              <div class="home-filter-section-head">
                <p class="home-filter-section-label">Specific commodity</p>
                ${
                  !allBenchmarksSelected
                    ? '<button class="filter-layer-clear" type="button" data-select-all-commodities>all benchmarks</button>'
                    : ""
                }
              </div>
              <div class="filter-chip-row" style="--sector-accent:${escapeHtml(sector.accent)};">
                <button
                  class="filter-chip home-all-commodities-chip${allBenchmarksSelected ? " is-selected" : ""}"
                  type="button"
                  data-select-all-commodities
                  aria-pressed="${String(allBenchmarksSelected)}"
                >
                  <span class="filter-chip-label">All benchmarks</span>
                </button>
                ${selectedGroup.commodities
                  .map((commodity) => {
                    const isSelected = !allBenchmarksSelected && selectedCommodities.has(commodity.id);
                    return `
                      <button
                        class="filter-chip${isSelected ? " is-selected" : ""}"
                        type="button"
                        data-commodity-id="${escapeHtml(commodity.id)}"
                        aria-pressed="${String(isSelected)}"
                      >
                        <span class="filter-chip-label">${escapeHtml(commodity.label)}</span>
                      </button>
                    `;
                  })
                  .join("")}
              </div>
            </section>
          `
          : selectedGroup
            ? `
              <section class="home-filter-section">
                <p class="home-filter-section-label">Specific commodity</p>
                <p class="home-filter-section-copy">
                  Benchmark selection is not configured for ${escapeHtml(selectedGroup.label)} yet. Headline and calendar results remain scoped to this group.
                </p>
              </section>
            `
          : ""
      }
    </div>
  `;
}

function renderExpandedFilterBar() {
  return `
    <div class="filter-wrap home-filter-wrap">
      <div class="filter-bar home-filter-bar" role="toolbar" aria-label="Home commodity filter">
        <button
          class="filter-pill${isAllFilter(activeFilter) ? " is-selected" : ""}"
          id="sector-reset"
          type="button"
          data-reset-filter
          aria-pressed="${String(isAllFilter(activeFilter))}"
        >
          All
        </button>
        <div class="filter-divider" aria-hidden="true"></div>
        <div class="sector-pill-row home-sector-row">
          ${renderSectorPills()}
        </div>
        <div class="home-filter-actions">
          ${
            isAllFilter(activeFilter)
              ? ""
              : `<span class="home-filter-selection" aria-live="polite">${escapeHtml(getFilterLabel(activeFilter))}</span>`
          }
          ${
            isAllFilter(activeFilter)
              ? ""
              : `
                <button class="home-filter-collapse-button" type="button" data-filter-collapse>
                  Collapse
                </button>
              `
          }
          <button class="home-filter-reset-link" type="button" data-reset-filter ${
            isAllFilter(activeFilter) ? "disabled" : ""
          }>
            Reset to All
          </button>
        </div>
      </div>
      ${
        isAllFilter(activeFilter)
          ? `
            <div class="subsector-hint open" aria-hidden="false">
              <span class="subsector-hint-text">Select a sector to refine Home by commodity group and benchmark.</span>
            </div>
          `
          : renderGroupLayer()
      }
    </div>
  `;
}

function renderCollapsedFilterBar() {
  return `
    <div class="filter-wrap home-filter-wrap is-collapsed">
      <div class="filter-bar home-filter-bar is-collapsed" role="toolbar" aria-label="Collapsed home commodity filter">
        <div class="home-filter-collapsed-copy">
          <span class="home-filter-collapsed-label">Home filter</span>
          <div class="home-filter-summary-pills">${renderSummaryPills()}</div>
        </div>
        <div class="home-filter-collapsed-actions">
          <button class="home-filter-collapse-button" type="button" data-filter-expand>Edit filter</button>
          <button class="home-filter-reset-link" type="button" data-reset-filter>Reset to All</button>
        </div>
      </div>
    </div>
  `;
}

function bindFilterEvents() {
  filterRoot.querySelectorAll("[data-sector-id]").forEach((button) => {
    button.addEventListener("click", () => {
      applyFilter(toggleSector(activeFilter, button.dataset.sectorId));
    });
  });

  filterRoot.querySelectorAll("[data-group-id]").forEach((button) => {
    button.addEventListener("click", () => {
      applyFilter(toggleGroup(activeFilter, button.dataset.groupId));
    });
  });

  filterRoot.querySelectorAll("[data-commodity-id]").forEach((button) => {
    button.addEventListener("click", () => {
      applyFilter(toggleCommodity(activeFilter, button.dataset.commodityId));
    });
  });

  filterRoot.querySelectorAll("[data-reset-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      applyFilter(createDefaultFilter());
    });
  });

  filterRoot.querySelector("[data-clear-group]")?.addEventListener("click", () => {
    const selectedSector = getSelectedSector(activeFilter);
    if (!selectedSector) {
      return;
    }

    applyFilter({
      sectorId: selectedSector.id,
      groupId: null,
      commodityIds: [],
    });
  });

  filterRoot.querySelectorAll("[data-select-all-commodities]").forEach((button) => {
    button.addEventListener("click", () => {
      applyFilter(clearCommodities(activeFilter));
    });
  });

  filterRoot.querySelector("[data-filter-collapse]")?.addEventListener("click", () => {
    setCollapsed(true);
  });

  filterRoot.querySelector("[data-filter-expand]")?.addEventListener("click", () => {
    setCollapsed(false);
  });
}

function renderFilterBar() {
  filterRoot.innerHTML = filterCollapsed && !isAllFilter(activeFilter) ? renderCollapsedFilterBar() : renderExpandedFilterBar();
  bindFilterEvents();
}

function renderModules() {
  const grid = document.createElement("div");
  grid.className = "dashboard-grid";

  MODULE_REGISTRY.forEach((moduleDefinition) => {
    const moduleElement = moduleDefinition.component({
      filter: activeFilter,
      onNavigate: handleNavigate,
    });
    moduleElement.dataset.slot = moduleDefinition.slot;
    grid.appendChild(moduleElement);
  });

  dashboardRoot.replaceChildren(grid);
}

function render() {
  renderFilterBar();
  renderModules();
}

function bindToTopButton() {
  if (!toTopButton) {
    return;
  }

  const syncVisibility = () => {
    toTopButton.classList.toggle("visible", window.scrollY > 360);
  };

  window.addEventListener("scroll", syncVisibility, { passive: true });
  syncVisibility();

  toTopButton.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });
}

render();
bindToTopButton();
