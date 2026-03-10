import { CommodityApiClient } from "./commodities-client.js";
import { buildCommodityDefinitions } from "./commodity-presentation.js";

const DETAIL_TIMEFRAMES = [
  { id: "1M", label: "1M", days: 31 },
  { id: "3M", label: "3M", days: 93 },
  { id: "6M", label: "6M", days: 186 },
  { id: "1Y", label: "1Y", days: 366 },
  { id: "MAX", label: "MAX", days: null },
];

const DEFAULT_DETAIL_TIMEFRAME_ID = "3M";

const PERIODIC_TILE_THEME = {
  gold: {
    border: "rgba(157, 115, 45, 0.44)",
    glint: "rgba(255, 241, 195, 0.5)",
    top: "#f6e0a3",
    mid: "#d7ac4c",
    bottom: "#a6752f",
  },
  silver: {
    border: "rgba(121, 136, 152, 0.42)",
    glint: "rgba(250, 253, 255, 0.55)",
    top: "#eef4fb",
    mid: "#cad6e2",
    bottom: "#8d99a6",
  },
  copper: {
    border: "rgba(132, 79, 43, 0.44)",
    glint: "rgba(255, 224, 186, 0.52)",
    top: "#efbf92",
    mid: "#c57c43",
    bottom: "#8d4d27",
  },
  aluminum: {
    border: "rgba(109, 126, 145, 0.42)",
    glint: "rgba(241, 249, 255, 0.54)",
    top: "#dae4ef",
    mid: "#b8c6d3",
    bottom: "#7d8e9f",
  },
  platinum: {
    border: "rgba(112, 101, 84, 0.42)",
    glint: "rgba(255, 249, 235, 0.66)",
    top: "#efe3ca",
    mid: "#ccb48b",
    bottom: "#8d7455",
  },
  palladium: {
    border: "rgba(93, 111, 130, 0.42)",
    glint: "rgba(248, 253, 255, 0.68)",
    top: "#e0e9f2",
    mid: "#b3c3d4",
    bottom: "#6f8094",
  },
  nickel: {
    border: "rgba(78, 96, 86, 0.42)",
    glint: "rgba(238, 247, 240, 0.66)",
    top: "#cfded3",
    mid: "#96ae9e",
    bottom: "#597261",
  },
  zinc: {
    border: "rgba(84, 102, 124, 0.42)",
    glint: "rgba(243, 249, 255, 0.66)",
    top: "#d4e0ee",
    mid: "#9eb2c8",
    bottom: "#637992",
  },
  lead: {
    border: "rgba(66, 78, 98, 0.44)",
    glint: "rgba(232, 239, 248, 0.58)",
    top: "#a7b4c7",
    mid: "#708097",
    bottom: "#3f4c63",
  },
  iron: {
    border: "rgba(110, 66, 42, 0.46)",
    glint: "rgba(248, 221, 193, 0.62)",
    top: "#d3a278",
    mid: "#a86a40",
    bottom: "#6a3f23",
  },
  lithium: {
    border: "rgba(98, 114, 104, 0.42)",
    glint: "rgba(246, 250, 244, 0.66)",
    top: "#d7e0d0",
    mid: "#aebfa4",
    bottom: "#74856c",
  },
  cobalt: {
    border: "rgba(48, 86, 126, 0.46)",
    glint: "rgba(231, 244, 255, 0.66)",
    top: "#8fb2d7",
    mid: "#4f7da9",
    bottom: "#2c5582",
  },
  default: {
    border: "rgba(120, 132, 145, 0.42)",
    glint: "rgba(246, 251, 255, 0.56)",
    top: "#dce4ec",
    mid: "#b8c4d0",
    bottom: "#7d8d9e",
  },
};

const ENERGY_TILE_THEME = {
  oil: {
    border: "rgba(69, 90, 114, 0.52)",
    glint: "rgba(222, 234, 247, 0.46)",
    top: "#9ab1c8",
    mid: "#5f7893",
    bottom: "#2d4257",
    ink: "rgba(8, 26, 41, 0.92)",
    chipBg: "rgba(223, 236, 248, 0.82)",
    chipBorder: "rgba(72, 102, 129, 0.44)",
  },
  naturalGas: {
    border: "rgba(46, 99, 133, 0.5)",
    glint: "rgba(205, 238, 255, 0.58)",
    top: "#a8d2eb",
    mid: "#5a9dc4",
    bottom: "#2d6e93",
    ink: "rgba(7, 30, 49, 0.92)",
    chipBg: "rgba(223, 242, 255, 0.84)",
    chipBorder: "rgba(56, 113, 149, 0.46)",
  },
  lng: {
    border: "rgba(75, 132, 174, 0.5)",
    glint: "rgba(222, 246, 255, 0.64)",
    top: "#d7ecfb",
    mid: "#8bb8db",
    bottom: "#466f94",
    ink: "rgba(10, 33, 56, 0.92)",
    chipBg: "rgba(232, 246, 255, 0.82)",
    chipBorder: "rgba(77, 132, 173, 0.46)",
  },
  gasoline: {
    border: "rgba(143, 95, 39, 0.48)",
    glint: "rgba(255, 225, 166, 0.62)",
    top: "#f2ce86",
    mid: "#d99d49",
    bottom: "#9c632a",
    ink: "rgba(47, 23, 4, 0.9)",
    chipBg: "rgba(255, 241, 213, 0.84)",
    chipBorder: "rgba(161, 110, 49, 0.44)",
  },
  thermalCoal: {
    border: "rgba(48, 58, 69, 0.56)",
    glint: "rgba(197, 205, 216, 0.34)",
    top: "#6f7b88",
    mid: "#3b4652",
    bottom: "#1d252f",
    ink: "rgba(236, 241, 247, 0.92)",
    chipBg: "rgba(29, 38, 48, 0.78)",
    chipBorder: "rgba(141, 157, 177, 0.34)",
  },
  diesel: {
    border: "rgba(70, 94, 120, 0.52)",
    glint: "rgba(215, 233, 248, 0.46)",
    top: "#9fbcd5",
    mid: "#5f7f9b",
    bottom: "#29435b",
    ink: "rgba(7, 24, 39, 0.9)",
    chipBg: "rgba(223, 238, 250, 0.82)",
    chipBorder: "rgba(73, 104, 132, 0.44)",
  },
  rubber: {
    border: "rgba(67, 86, 108, 0.52)",
    glint: "rgba(222, 230, 242, 0.46)",
    top: "#b8c6d6",
    mid: "#748aa1",
    bottom: "#3a4d61",
    ink: "rgba(9, 24, 38, 0.9)",
    chipBg: "rgba(230, 236, 245, 0.82)",
    chipBorder: "rgba(70, 93, 116, 0.44)",
  },
  default: {
    border: "rgba(91, 110, 130, 0.48)",
    glint: "rgba(228, 239, 249, 0.56)",
    top: "#cedbe7",
    mid: "#95a8bc",
    bottom: "#596f84",
    ink: "rgba(9, 26, 41, 0.92)",
    chipBg: "rgba(231, 240, 250, 0.8)",
    chipBorder: "rgba(92, 112, 133, 0.38)",
  },
};

const AGRI_TILE_THEME = {
  wheat: { top: "#efd788", mid: "#cb9e3f", bottom: "#9f7524" },
  corn: { top: "#f0d36d", mid: "#c9962e", bottom: "#8f651b" },
  soybeans: { top: "#c8d69a", mid: "#92a163", bottom: "#617440" },
  soybeanOil: { top: "#e8d18b", mid: "#be993d", bottom: "#8f6f22" },
  palmOil: { top: "#ddb37d", mid: "#b9793d", bottom: "#7d4c25" },
  rice: { top: "#e2dbc8", mid: "#b9ab86", bottom: "#81724f" },
  lumber: { top: "#c78a58", mid: "#90562e", bottom: "#5b341a" },
  coffee: { top: "#d9af84", mid: "#a76d42", bottom: "#633d27" },
  sugar: { top: "#dbe4ef", mid: "#aebccc", bottom: "#75879d" },
  cotton: { top: "#e3dfd7", mid: "#bbb5aa", bottom: "#827b74" },
  cocoa: { top: "#d49d6f", mid: "#a6683d", bottom: "#663c27" },
  default: { top: "#c6d5e2", mid: "#8aa2ba", bottom: "#55697d" },
};

class EnergyTileVisualizer {
  constructor(tile) {
    this.tile = tile;
  }

  createScene() {
    const scene = document.createElement("div");
    scene.className = "energy-tile-scene";
    scene.innerHTML = `
      <div class="energy-tile-stage ${this.tile.family}">
        <div class="energy-market-tile ${this.tile.family}">
          <div class="energy-tile-meta">
            <span>${escapeHtml(this.tile.venue)}</span>
            <span>${escapeHtml(this.tile.ticker)}</span>
          </div>
          <div class="energy-icon-wrap">
            <span class="energy-icon" style="--energy-icon-url: url('${escapeHtml(this.tile.icon)}')" aria-hidden="true"></span>
          </div>
          <div class="energy-tile-code">${escapeHtml(this.tile.code)}</div>
          <div class="energy-tile-name">${escapeHtml(this.tile.name)}</div>
          <div class="energy-tile-shimmer" aria-hidden="true"></div>
        </div>
      </div>
    `;

    this.tileEl = scene.querySelector(".energy-market-tile");
    const theme = ENERGY_TILE_THEME[this.tile.family] || ENERGY_TILE_THEME.default;

    this.tileEl.style.setProperty("--energy-border", theme.border);
    this.tileEl.style.setProperty("--energy-glint", theme.glint);
    this.tileEl.style.setProperty("--energy-top", theme.top);
    this.tileEl.style.setProperty("--energy-mid", theme.mid);
    this.tileEl.style.setProperty("--energy-bottom", theme.bottom);
    this.tileEl.style.setProperty("--energy-ink", theme.ink);
    this.tileEl.style.setProperty("--energy-chip-bg", theme.chipBg);
    this.tileEl.style.setProperty("--energy-chip-border", theme.chipBorder);

    return scene;
  }

  update() {}
}

class PeriodicTileVisualizer {
  constructor(tile) {
    this.tile = tile;
  }

  createScene() {
    const scene = document.createElement("div");
    scene.className = "periodic-scene";
    scene.innerHTML = `
      <div class="periodic-tile ${escapeHtml(this.tile.family)}">
        <div class="tile-meta">
          <span class="tile-number">${escapeHtml(this.tile.number)}</span>
          <span class="tile-mass">${escapeHtml(this.tile.mass)}</span>
        </div>
        <div class="tile-symbol">${escapeHtml(this.tile.symbol)}</div>
        <div class="tile-name">${escapeHtml(this.tile.name)}</div>
        <div class="tile-shimmer" aria-hidden="true"></div>
      </div>
    `;

    this.tileEl = scene.querySelector(".periodic-tile");
    const theme = PERIODIC_TILE_THEME[this.tile.family] || PERIODIC_TILE_THEME.default;
    this.tileEl.style.setProperty("--periodic-border", theme.border);
    this.tileEl.style.setProperty("--periodic-glint", theme.glint);
    this.tileEl.style.setProperty("--periodic-top", theme.top);
    this.tileEl.style.setProperty("--periodic-mid", theme.mid);
    this.tileEl.style.setProperty("--periodic-bottom", theme.bottom);
    return scene;
  }

  update() {}
}

class AgriTileVisualizer {
  constructor(tile) {
    this.tile = tile;
  }

  createScene() {
    const scene = document.createElement("div");
    scene.className = "agri-tile-scene";
    scene.innerHTML = `
      <div class="agri-tile ${escapeHtml(this.tile.family)}">
        <div class="agri-tile-meta">
          <span class="agri-venue">${escapeHtml(this.tile.venue)}</span>
          <span class="agri-ticker">${escapeHtml(this.tile.ticker)}</span>
        </div>
        <div class="agri-code">${escapeHtml(this.tile.code)}</div>
        <div class="agri-name">${escapeHtml(this.tile.name)}</div>
      </div>
    `;

    this.tileEl = scene.querySelector(".agri-tile");
    const theme = AGRI_TILE_THEME[this.tile.family] || AGRI_TILE_THEME.default;
    this.tileEl.style.setProperty("--agri-top", theme.top);
    this.tileEl.style.setProperty("--agri-mid", theme.mid);
    this.tileEl.style.setProperty("--agri-bottom", theme.bottom);
    return scene;
  }

  update() {}
}

function createVisualizer(visualDef) {
  if (visualDef.type === "energyTile") {
    return new EnergyTileVisualizer(visualDef.tile);
  }

  if (visualDef.type === "periodicTile") {
    return new PeriodicTileVisualizer(visualDef.tile);
  }

  if (visualDef.type === "agriTile") {
    return new AgriTileVisualizer(visualDef.tile);
  }

  throw new Error(`Unknown visualizer type: ${visualDef.type}`);
}

class CommodityWatchEngine {
  constructor(apiClient) {
    this.apiClient = apiClient;
    this.definitions = [];
    this.definitionsById = new Map();
    this.views = new Map();
    this.historyCache = new Map();
    this.selectedGroups = new Set(["energy", "metals", "agri"]);
    this.resizeRaf = null;
    this.detailCloseTimer = null;
    this.detailState = {
      openCommodityId: null,
      timeframeByCommodity: new Map(),
      seriesByCommodity: new Map(),
      returnFocusEl: null,
    };

    this.ui = {
      nav: document.querySelector(".nav"),
      filterWrap: document.querySelector(".filter-wrap"),
      pageShell: document.querySelector(".page-shell"),
      footer: document.querySelector(".footer"),
      featuredGrid: document.getElementById("featured-grid"),
      metalsGrid: document.getElementById("metals-grid"),
      agriGrid: document.getElementById("agri-grid"),
      metalsSection: document.querySelector(".metals-section"),
      agriSection: document.querySelector(".agri-section"),
      groupPills: Array.from(document.querySelectorAll(".filter-pill[data-group]")),
      detailOverlay: document.getElementById("detail-overlay"),
      detailFront: document.getElementById("detail-front"),
      detailBack: document.getElementById("detail-back"),
      detailClose: document.getElementById("detail-close"),
    };
  }

  async init() {
    this.bindGlobalEvents();

    try {
      await this.loadInitialData();
    } catch (error) {
      this.renderGlobalError(error);
    }
  }

  bindGlobalEvents() {
    this.ui.groupPills.forEach((pill) => {
      pill.addEventListener("click", () => {
        this.toggleGroupSelection(pill.dataset.group);
      });
    });

    this.ui.detailClose.addEventListener("click", () => {
      this.closeDetail();
    });

    this.ui.detailOverlay.addEventListener("click", (event) => {
      if (event.target === this.ui.detailOverlay) {
        this.closeDetail();
      }
    });

    window.addEventListener("keydown", (event) => {
      if (this.ui.detailOverlay.hidden) {
        return;
      }

      if (event.key === "Escape") {
        event.preventDefault();
        this.closeDetail();
        return;
      }

      if (event.key === "Tab") {
        this.trapDetailFocus(event);
      }
    });

    window.addEventListener("resize", () => {
      if (this.resizeRaf) {
        window.cancelAnimationFrame(this.resizeRaf);
      }

      this.resizeRaf = window.requestAnimationFrame(() => {
        this.renderAll();
      });
    });
  }

  async loadInitialData() {
    const [seriesRows, latestRows] = await Promise.all([this.apiClient.listSeries(), this.apiClient.listLatest()]);
    const definitions = buildCommodityDefinitions(seriesRows, latestRows);

    this.resetCatalog(definitions);
    this.applyLatestRows(latestRows);
    this.applyGroupFilters();
    this.renderAll();
  }

  resetCatalog(definitions) {
    const previousSeriesSelection = this.detailState.seriesByCommodity;

    this.definitions = definitions;
    this.definitionsById = new Map(definitions.map((definition) => [definition.id, definition]));
    this.views = new Map();
    this.historyCache = new Map();
    this.detailState.seriesByCommodity = new Map();

    this.ui.featuredGrid.innerHTML = "";
    this.ui.metalsGrid.innerHTML = "";
    this.ui.agriGrid.innerHTML = "";

    definitions.forEach((definition) => {
      const preferredSeriesKey = previousSeriesSelection.get(definition.id);
      const hasPreferredSeries = definition.seriesOptions.some((series) => series.seriesKey === preferredSeriesKey);
      this.detailState.seriesByCommodity.set(
        definition.id,
        hasPreferredSeries ? preferredSeriesKey : definition.defaultSeriesKey
      );
      this.createCard(definition);
    });
  }

  createCard(definition) {
    const card = document.createElement("article");
    card.className = "viz-card";
    card.dataset.commodity = definition.id;
    card.dataset.group = definition.group;
    card.setAttribute("role", "button");
    card.setAttribute("tabindex", "0");
    card.setAttribute("aria-haspopup", "dialog");
    card.setAttribute("aria-label", `Open ${definition.primaryLabel} details`);

    if (definition.group === "metals") {
      card.classList.add("metal-tile-card");
    } else if (definition.group === "agri") {
      card.classList.add("agri-tile-card");
    } else {
      card.classList.add("feature-card");
    }

    const feedCat = document.createElement("div");
    feedCat.className = "feed-cat";

    const dot = document.createElement("span");
    dot.className = `feed-cat-dot ${definition.group}`;

    const feedLabel = document.createElement("span");
    feedLabel.className = "feed-cat-label";
    feedLabel.textContent = definition.groupLabel;

    feedCat.append(dot, feedLabel);

    const titleEl = document.createElement("h2");
    titleEl.className = "card-title";
    titleEl.textContent = definition.primaryLabel;

    const sceneSlot = document.createElement("div");
    sceneSlot.className = "scene-slot";

    const metricRow = document.createElement("div");
    metricRow.className = "metric-row";

    const valueEl = document.createElement("p");
    valueEl.className = "value";

    const deltaEl = document.createElement("p");
    deltaEl.className = "delta";

    metricRow.append(valueEl, deltaEl);

    const metaEl = document.createElement("p");
    metaEl.className = "card-meta";

    const visualizer = createVisualizer(definition.visual);
    sceneSlot.appendChild(visualizer.createScene());

    card.append(feedCat, titleEl, sceneSlot, metricRow, metaEl);

    if (definition.group === "metals") {
      this.ui.metalsGrid.appendChild(card);
    } else if (definition.group === "agri") {
      this.ui.agriGrid.appendChild(card);
    } else {
      this.ui.featuredGrid.appendChild(card);
    }

    card.addEventListener("click", () => {
      this.openDetail(definition.id, card);
    });

    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        this.openDetail(definition.id, card);
      }
    });

    this.views.set(definition.id, {
      definition,
      card,
      visualizer,
      valueEl,
      deltaEl,
      metaEl,
      titleEl,
    });
  }

  applyLatestRows(latestRows) {
    const latestBySeriesKey = new Map(latestRows.map((row) => [row.series_key, row]));

    this.definitions.forEach((definition) => {
      definition.seriesOptions.forEach((seriesOption) => {
        const latestRow = latestBySeriesKey.get(seriesOption.seriesKey);
        if (!latestRow) {
          return;
        }

        seriesOption.value = latestRow.value;
        seriesOption.previousValue = latestRow.previous_value;
        seriesOption.deltaValue = latestRow.delta_value;
        seriesOption.deltaPct = latestRow.delta_pct;
        seriesOption.observationDate = latestRow.observation_date;
        seriesOption.updatedAt = latestRow.updated_at;
        seriesOption.sourceName = latestRow.source_name;
        seriesOption.sourceSeriesCode = latestRow.source_series_code || seriesOption.sourceSeriesCode;
        seriesOption.frequency = latestRow.frequency || seriesOption.frequency;
        seriesOption.unit = latestRow.unit || seriesOption.unit;
        seriesOption.currency = latestRow.currency || seriesOption.currency;
        seriesOption.geography = latestRow.geography || seriesOption.geography;
        seriesOption.notes = latestRow.notes || seriesOption.notes;
        seriesOption.decimals = inferDisplayDigits(latestRow.unit || seriesOption.unit, latestRow.value);
      });
    });
  }

  setShellInteractivity(isDetailOpen) {
    [this.ui.nav, this.ui.filterWrap, this.ui.pageShell, this.ui.footer].forEach((element) => {
      if (!element) {
        return;
      }

      if ("inert" in element) {
        element.inert = isDetailOpen;
      }

      if (isDetailOpen) {
        element.setAttribute("aria-hidden", "true");
      } else {
        element.removeAttribute("aria-hidden");
      }
    });
  }

  getFocusableDetailElements() {
    if (this.ui.detailOverlay.hidden) {
      return [];
    }

    return Array.from(
      this.ui.detailOverlay.querySelectorAll('button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])')
    ).filter((element) => !element.hidden && !element.hasAttribute("disabled"));
  }

  focusFirstDetailElement() {
    const [firstFocusable] = this.getFocusableDetailElements();
    firstFocusable?.focus();
  }

  trapDetailFocus(event) {
    const focusable = this.getFocusableDetailElements();

    if (!focusable.length) {
      event.preventDefault();
      return;
    }

    const activeElement = document.activeElement;
    const firstFocusable = focusable[0];
    const lastFocusable = focusable[focusable.length - 1];

    if (!this.ui.detailOverlay.contains(activeElement)) {
      event.preventDefault();
      firstFocusable.focus();
      return;
    }

    if (!event.shiftKey && activeElement === lastFocusable) {
      event.preventDefault();
      firstFocusable.focus();
      return;
    }

    if (event.shiftKey && activeElement === firstFocusable) {
      event.preventDefault();
      lastFocusable.focus();
    }
  }

  toggleGroupSelection(group) {
    if (!group) {
      return;
    }

    const groups = this.ui.groupPills.map((pill) => pill.dataset.group).filter(Boolean);
    const isOnlySelected = this.selectedGroups.size === 1 && this.selectedGroups.has(group);
    this.selectedGroups = isOnlySelected ? new Set(groups) : new Set([group]);
    this.applyGroupFilters();
  }

  applyGroupFilters() {
    this.ui.groupPills.forEach((pill) => {
      const isSelected = this.selectedGroups.has(pill.dataset.group);
      pill.classList.toggle("is-selected", isSelected);
      pill.setAttribute("aria-pressed", String(isSelected));
    });

    this.views.forEach((view) => {
      view.card.hidden = !this.selectedGroups.has(view.definition.group);
    });

    const hasVisibleGroup = (group) =>
      Array.from(this.views.values()).some((view) => view.definition.group === group && !view.card.hidden);

    this.ui.featuredGrid.hidden = !hasVisibleGroup("energy");
    this.ui.metalsSection.hidden = !hasVisibleGroup("metals");
    this.ui.agriSection.hidden = !hasVisibleGroup("agri");
  }

  getActiveSeries(definition) {
    const storedKey = this.detailState.seriesByCommodity.get(definition.id);
    const activeSeries =
      definition.seriesOptions.find((series) => series.seriesKey === storedKey) ||
      definition.seriesOptions.find((series) => series.seriesKey === definition.defaultSeriesKey) ||
      definition.seriesOptions[0] ||
      null;

    if (activeSeries) {
      this.detailState.seriesByCommodity.set(definition.id, activeSeries.seriesKey);
    }

    return activeSeries;
  }

  openDetail(id, triggerEl = null) {
    const view = this.views.get(id);
    if (!view) {
      return;
    }

    if (this.detailCloseTimer) {
      window.clearTimeout(this.detailCloseTimer);
      this.detailCloseTimer = null;
    }

    this.detailState.openCommodityId = id;
    this.detailState.returnFocusEl =
      triggerEl instanceof HTMLElement
        ? triggerEl
        : document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;

    this.ui.detailFront.innerHTML = "";
    const cardClone = view.card.cloneNode(true);
    cardClone.removeAttribute("tabindex");
    cardClone.removeAttribute("role");
    cardClone.removeAttribute("aria-label");
    this.ui.detailFront.appendChild(cardClone);

    this.renderDetailPanel(view.definition);

    this.ui.detailOverlay.hidden = false;
    this.setShellInteractivity(true);
    window.requestAnimationFrame(() => {
      this.ui.detailOverlay.classList.add("open");
      this.focusFirstDetailElement();
    });
    document.body.classList.add("detail-open");
  }

  closeDetail() {
    if (this.ui.detailOverlay.hidden) {
      return;
    }

    if (this.detailCloseTimer) {
      window.clearTimeout(this.detailCloseTimer);
      this.detailCloseTimer = null;
    }

    this.ui.detailOverlay.classList.remove("open");
    document.body.classList.remove("detail-open");
    this.setShellInteractivity(false);

    this.detailCloseTimer = window.setTimeout(() => {
      if (!this.ui.detailOverlay.classList.contains("open")) {
        this.ui.detailOverlay.hidden = true;
      }

      if (
        this.detailState.returnFocusEl instanceof HTMLElement &&
        this.detailState.returnFocusEl.isConnected &&
        !this.detailState.returnFocusEl.hidden
      ) {
        this.detailState.returnFocusEl.focus();
      }

      this.detailState.openCommodityId = null;
      this.detailState.returnFocusEl = null;
      this.detailCloseTimer = null;
    }, 220);
  }

  getSelectedTimeframe(definition) {
    const storedKey = this.detailState.timeframeByCommodity.get(definition.id);
    const activeTimeframe =
      DETAIL_TIMEFRAMES.find((option) => option.id === storedKey) ||
      DETAIL_TIMEFRAMES.find((option) => option.id === DEFAULT_DETAIL_TIMEFRAME_ID) ||
      DETAIL_TIMEFRAMES[0];

    this.detailState.timeframeByCommodity.set(definition.id, activeTimeframe.id);
    return activeTimeframe;
  }

  getTimeframeWindow(seriesOption, timeframe) {
    if (!timeframe.days || !seriesOption?.observationDate) {
      return { start: null, end: seriesOption?.observationDate || null };
    }

    const endDate = new Date(`${seriesOption.observationDate}T00:00:00`);
    if (Number.isNaN(endDate.getTime())) {
      return { start: null, end: seriesOption.observationDate || null };
    }

    const startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - timeframe.days);
    return {
      start: formatDateKey(startDate),
      end: seriesOption.observationDate,
    };
  }

  getHistoryCacheKey(definition, seriesOption, timeframe) {
    const windowRange = this.getTimeframeWindow(seriesOption, timeframe);
    return `${definition.id}:${seriesOption.seriesKey}:${timeframe.id}:${windowRange.start || "MIN"}:${windowRange.end || "MAX"}`;
  }

  async ensureHistory(definition, seriesOption, timeframe) {
    const cacheKey = this.getHistoryCacheKey(definition, seriesOption, timeframe);
    const existing = this.historyCache.get(cacheKey);

    if (existing && (existing.status === "loading" || existing.status === "ready" || existing.status === "error")) {
      return;
    }

    const windowRange = this.getTimeframeWindow(seriesOption, timeframe);
    this.historyCache.set(cacheKey, {
      status: "loading",
      rows: [],
      error: null,
    });

    if (this.detailState.openCommodityId === definition.id) {
      this.renderDetailPanel(definition);
    }

    try {
      const rows = await this.apiClient.getHistory(seriesOption.seriesKey, windowRange);
      this.historyCache.set(cacheKey, {
        status: "ready",
        rows,
        error: null,
      });
    } catch (error) {
      this.historyCache.set(cacheKey, {
        status: "error",
        rows: [],
        error: String(error.message || error),
      });
    }

    if (
      this.detailState.openCommodityId === definition.id &&
      this.getActiveSeries(definition)?.seriesKey === seriesOption.seriesKey
    ) {
      this.renderDetailPanel(definition);
    }
  }

  getDetailViewModel(definition) {
    const activeSeries = this.getActiveSeries(definition);
    const timeframe = this.getSelectedTimeframe(definition);

    if (!activeSeries) {
      return {
        activeSeries: null,
        timeframe,
        historyStatus: "ready",
        error: null,
        rows: [],
        series: [],
        windowStats: null,
        chartPalette: getDetailChartPalette(definition),
      };
    }

    const cacheKey = this.getHistoryCacheKey(definition, activeSeries, timeframe);
    const historyEntry = this.historyCache.get(cacheKey) || {
      status: "loading",
      rows: [],
      error: null,
    };

    if (!this.historyCache.has(cacheKey)) {
      this.ensureHistory(definition, activeSeries, timeframe);
    }

    const series = historyEntry.rows
      .map((row) => ({
        date: row.observation_date,
        value: row.value,
      }))
      .filter((point) => point.date && isFiniteNumber(point.value));

    return {
      activeSeries,
      timeframe,
      historyStatus: historyEntry.status,
      error: historyEntry.error,
      rows: historyEntry.rows,
      series,
      windowStats: series.length ? getSeriesRangeStats(series, 0, Math.max(series.length - 1, 0)) : null,
      chartPalette: getDetailChartPalette(definition),
    };
  }

  renderDetailPanel(definition) {
    const detailView = this.getDetailViewModel(definition);
    const activeElement = document.activeElement;
    let focusSelector = "";

    if (activeElement?.id === "detail-close") {
      focusSelector = "#detail-close";
    } else if (activeElement?.dataset?.detailTimeframe) {
      focusSelector = `[data-detail-timeframe="${activeElement.dataset.detailTimeframe}"]`;
    } else if (activeElement?.dataset?.detailSeries) {
      focusSelector = `[data-detail-series="${activeElement.dataset.detailSeries}"]`;
    }

    this.applyDetailTheme(definition);
    this.ui.detailBack.innerHTML = this.renderDetailBack(definition, detailView);
    this.bindDetailControls(definition);
    this.bindDetailChartRange(definition, detailView);

    if (focusSelector) {
      const nextFocusTarget = this.ui.detailOverlay.querySelector(focusSelector);
      nextFocusTarget?.focus();
    }
  }

  applyDetailTheme(definition) {
    const theme = getDetailBackdropTheme(definition);
    const bubbleTheme = getDetailBubbleTheme(definition);
    this.ui.detailBack.style.setProperty("--detail-back-top", theme.top);
    this.ui.detailBack.style.setProperty("--detail-back-mid", theme.mid);
    this.ui.detailBack.style.setProperty("--detail-back-bottom", theme.bottom);
    this.ui.detailBack.style.setProperty("--detail-bubble-bg", bubbleTheme.background);
    this.ui.detailBack.style.setProperty("--detail-bubble-border", bubbleTheme.border);
    this.ui.detailBack.style.setProperty("--detail-bubble-ink", bubbleTheme.ink);
    this.ui.detailBack.style.setProperty("--detail-bubble-subtle", bubbleTheme.subtle);
  }

  bindDetailControls(definition) {
    const timeframeButtons = this.ui.detailBack.querySelectorAll("[data-detail-timeframe]");
    const seriesButtons = this.ui.detailBack.querySelectorAll("[data-detail-series]");

    timeframeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const timeframeKey = button.dataset.detailTimeframe;
        if (!timeframeKey) {
          return;
        }

        this.detailState.timeframeByCommodity.set(definition.id, timeframeKey);
        this.renderDetailPanel(definition);

        const nextButton = this.ui.detailBack.querySelector(`[data-detail-timeframe="${timeframeKey}"]`);
        nextButton?.focus();
      });
    });

    seriesButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const seriesKey = button.dataset.detailSeries;
        if (!seriesKey) {
          return;
        }

        this.detailState.seriesByCommodity.set(definition.id, seriesKey);
        this.renderCommodity(definition);
        this.renderDetailPanel(definition);

        const nextButton = this.ui.detailBack.querySelector(`[data-detail-series="${seriesKey}"]`);
        nextButton?.focus();
      });
    });
  }

  bindDetailChartRange(definition, detailView) {
    if (detailView.historyStatus !== "ready" || !detailView.series.length || !detailView.activeSeries) {
      return;
    }

    const { activeSeries, series } = detailView;
    const chartWrap = this.ui.detailBack.querySelector(".detail-chart-wrap");
    const chartSvg = chartWrap?.querySelector(".detail-chart");
    const selectionFill = chartSvg?.querySelector(".detail-chart-selection");
    const selectionSegment = chartSvg?.querySelector(".detail-chart-selection-segment");
    const selectionStartLine = chartSvg?.querySelector(".detail-chart-selection-start");
    const selectionEndLine = chartSvg?.querySelector(".detail-chart-selection-end");
    const selectionStartDot = chartSvg?.querySelector(".detail-chart-selection-dot.start");
    const selectionEndDot = chartSvg?.querySelector(".detail-chart-selection-dot.end");
    const hoverLine = chartSvg?.querySelector(".detail-chart-hover-line");
    const hoverDot = chartSvg?.querySelector(".detail-chart-hover-dot");
    const tooltip = chartWrap?.querySelector("[data-chart-tooltip]");
    const selectionBubble = chartWrap?.querySelector("[data-chart-selection-bubble]");

    if (
      !chartWrap ||
      !chartSvg ||
      !selectionFill ||
      !selectionSegment ||
      !selectionStartLine ||
      !selectionEndLine ||
      !selectionStartDot ||
      !selectionEndDot ||
      !hoverLine ||
      !hoverDot ||
      !tooltip ||
      !selectionBubble
    ) {
      return;
    }

    const width = 860;
    const height = 360;
    const pad = { top: 22, right: 24, bottom: 40, left: 56 };
    const values = series.map((point) => point.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = Math.max(max - min, 0.0001);
    const innerWidth = width - pad.left - pad.right;
    const innerHeight = height - pad.top - pad.bottom;
    const lastIndex = series.length - 1;
    const xStep = innerWidth / Math.max(lastIndex, 1);
    const dragThresholdPx = 8;
    let isPointerDown = false;
    let dragStarted = false;
    let anchorIndex = 0;
    let latestIndex = 0;
    let pointerDownX = 0;

    const xCoord = (index) => pad.left + index * xStep;
    const yCoord = (value) => pad.top + ((max - value) / span) * innerHeight;

    const setFloatingPosition = (element, svgX, svgY, xOffset = 0, yOffset = 0) => {
      const rect = chartWrap.getBoundingClientRect();
      if (!rect.width || !rect.height) {
        return;
      }

      const rawLeftPx = (svgX / width) * rect.width + xOffset;
      const rawTopPx = (svgY / height) * rect.height + yOffset;
      const elementHalfWidth = element.offsetWidth / 2;
      const minLeft = elementHalfWidth + 10;
      const maxLeft = rect.width - elementHalfWidth - 10;
      const minTop = element.offsetHeight + 18;
      const maxTop = rect.height - 10;

      element.style.left = `${clamp(rawLeftPx, minLeft, Math.max(minLeft, maxLeft))}px`;
      element.style.top = `${clamp(rawTopPx, minTop, Math.max(minTop, maxTop))}px`;
    };

    const showSvgElements = (...elements) => {
      elements.forEach((element) => {
        element.removeAttribute("hidden");
        element.style.display = "";
      });
    };

    const hideSvgElements = (...elements) => {
      elements.forEach((element) => {
        element.setAttribute("hidden", "");
        element.style.display = "none";
      });
    };

    const hideTooltip = () => {
      tooltip.hidden = true;
    };

    const showTooltip = (index) => {
      const point = series[index];
      const x = xCoord(index);
      const y = yCoord(point.value);

      hoverLine.setAttribute("x1", x.toFixed(2));
      hoverLine.setAttribute("x2", x.toFixed(2));
      hoverDot.setAttribute("cx", x.toFixed(2));
      hoverDot.setAttribute("cy", y.toFixed(2));
      showSvgElements(hoverLine, hoverDot);

      tooltip.innerHTML = `
        <span class="detail-chart-bubble-date">${escapeHtml(formatDateLong(point.date))}</span>
        <strong>${escapeHtml(formatPrice(point.value, activeSeries.decimals, activeSeries.unit))}</strong>
      `;
      tooltip.hidden = false;
      setFloatingPosition(tooltip, x, y, 0, -16);
    };

    const hideHover = () => {
      hideSvgElements(hoverLine, hoverDot);
      hideTooltip();
    };

    const hideSelection = () => {
      hideSvgElements(
        selectionFill,
        selectionSegment,
        selectionStartLine,
        selectionEndLine,
        selectionStartDot,
        selectionEndDot
      );
      selectionBubble.hidden = true;
    };

    const showSelection = (startIndex, endIndex) => {
      const normalized = normalizeRangeSelection(startIndex, endIndex, lastIndex);
      const stats = getSeriesRangeStats(series, normalized.startIndex, normalized.endIndex);
      const leftX = xCoord(normalized.startIndex);
      const rightX = xCoord(normalized.endIndex);
      const segmentPath = buildLinePathSegment(series, normalized.startIndex, normalized.endIndex, xCoord, yCoord);
      const centerX = leftX + (rightX - leftX) / 2;
      const bubbleY = Math.min(yCoord(Math.max(stats.startPoint.value, stats.endPoint.value)), pad.top + 24);

      selectionFill.setAttribute("x", leftX.toFixed(2));
      selectionFill.setAttribute("y", pad.top.toFixed(2));
      selectionFill.setAttribute("width", Math.max(rightX - leftX, 2).toFixed(2));
      selectionFill.setAttribute("height", innerHeight.toFixed(2));
      selectionSegment.setAttribute("d", segmentPath);
      selectionStartLine.setAttribute("x1", leftX.toFixed(2));
      selectionStartLine.setAttribute("x2", leftX.toFixed(2));
      selectionEndLine.setAttribute("x1", rightX.toFixed(2));
      selectionEndLine.setAttribute("x2", rightX.toFixed(2));
      selectionStartDot.setAttribute("cx", leftX.toFixed(2));
      selectionStartDot.setAttribute("cy", yCoord(stats.startPoint.value).toFixed(2));
      selectionEndDot.setAttribute("cx", rightX.toFixed(2));
      selectionEndDot.setAttribute("cy", yCoord(stats.endPoint.value).toFixed(2));
      showSvgElements(
        selectionFill,
        selectionSegment,
        selectionStartLine,
        selectionEndLine,
        selectionStartDot,
        selectionEndDot
      );

      selectionBubble.innerHTML = `
        <strong>${escapeHtml(
          `${formatSigned(stats.delta, activeSeries.decimals, activeSeries.unit)} (${formatSigned(stats.deltaPct, 2)}%)`
        )}</strong>
        <span class="detail-chart-bubble-date">${escapeHtml(
          `${formatDateLong(stats.startPoint.date)} to ${formatDateLong(stats.endPoint.date)}`
        )}</span>
      `;
      selectionBubble.hidden = false;
      setFloatingPosition(selectionBubble, centerX, bubbleY, 0, -22);
    };

    const getIndexFromClientX = (clientX) => {
      const rect = chartSvg.getBoundingClientRect();
      if (!rect.width) {
        return lastIndex;
      }

      const svgX = ((clientX - rect.left) / rect.width) * width;
      const clampedX = clamp(svgX, pad.left, pad.left + innerWidth);
      return Math.round((clampedX - pad.left) / xStep);
    };

    hideHover();
    hideSelection();

    chartWrap.addEventListener("pointerleave", () => {
      hideSelection();
      hideHover();
    });

    chartWrap.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      hideHover();
      isPointerDown = true;
      dragStarted = false;
      pointerDownX = event.clientX;
      anchorIndex = getIndexFromClientX(event.clientX);
      latestIndex = anchorIndex;
      chartWrap.setPointerCapture(event.pointerId);
    });

    chartWrap.addEventListener("pointermove", (event) => {
      const nextIndex = getIndexFromClientX(event.clientX);

      if (!isPointerDown) {
        showTooltip(nextIndex);
        return;
      }

      latestIndex = nextIndex;

      if (!dragStarted && Math.abs(event.clientX - pointerDownX) >= dragThresholdPx && latestIndex !== anchorIndex) {
        dragStarted = true;
      }

      if (dragStarted) {
        hideHover();
        showSelection(anchorIndex, latestIndex);
      }
    });

    chartWrap.addEventListener("pointerup", (event) => {
      if (chartWrap.hasPointerCapture(event.pointerId)) {
        chartWrap.releasePointerCapture(event.pointerId);
      }

      if (isPointerDown) {
        latestIndex = getIndexFromClientX(event.clientX);

        if (dragStarted && latestIndex !== anchorIndex) {
          hideSelection();
        } else {
          hideSelection();
          hideHover();
        }
      }

      isPointerDown = false;
      dragStarted = false;
    });

    chartWrap.addEventListener("pointercancel", (event) => {
      if (chartWrap.hasPointerCapture(event.pointerId)) {
        chartWrap.releasePointerCapture(event.pointerId);
      }

      hideSelection();
      hideHover();
      isPointerDown = false;
      dragStarted = false;
    });
  }

  renderDetailBack(definition, detailView) {
    const activeSeries = detailView.activeSeries;

    if (!activeSeries) {
      return `
        <div class="detail-head">
          <div>
            <p class="detail-kicker">${escapeHtml(definition.primaryLabel)}</p>
            <h3 class="detail-title">Unavailable</h3>
          </div>
        </div>
        <div class="detail-chart-wrap detail-chart-state">
          <p class="detail-chart-message">No published series is available for this card.</p>
        </div>
      `;
    }

    const latestValue = formatPrice(activeSeries.value, activeSeries.decimals, activeSeries.unit);
    const latestObservation = activeSeries.observationDate ? formatDateLong(activeSeries.observationDate) : "Unavailable";
    const sourceMarkup = activeSeries.sourceUrl
      ? `<a class="detail-link" href="${escapeHtml(activeSeries.sourceUrl)}" target="_blank" rel="noreferrer">${escapeHtml(
          activeSeries.sourceName
        )}</a>`
      : escapeHtml(activeSeries.sourceName || "Unknown source");

    const timeframeButtons = DETAIL_TIMEFRAMES.map(
      (option) => `
        <button class="detail-chip ${option.id === detailView.timeframe.id ? "is-active" : ""}" type="button" data-detail-timeframe="${
          option.id
        }" aria-pressed="${String(option.id === detailView.timeframe.id)}">
          ${option.label}
        </button>
      `
    ).join("");

    const seriesButtons =
      definition.seriesOptions.length > 1
        ? definition.seriesOptions
            .map(
              (seriesOption) => `
                <button class="detail-chip ${
                  seriesOption.seriesKey === activeSeries.seriesKey ? "is-active" : ""
                }" type="button" data-detail-series="${escapeHtml(seriesOption.seriesKey)}" aria-pressed="${String(
                  seriesOption.seriesKey === activeSeries.seriesKey
                )}">
                  ${escapeHtml(seriesOption.optionLabel)}
                </button>
              `
            )
            .join("")
        : "";

    const metaGrid = `
      <div class="detail-meta-grid">
        ${buildMetaItem("Published series", activeSeries.actualSeriesName)}
        ${buildMetaItem("Target concept", activeSeries.targetConcept || "Not specified")}
        ${buildMetaItem("Frequency", activeSeries.frequency || "Unknown")}
        ${buildMetaItem("Unit", activeSeries.unit || "Unknown")}
        ${buildMetaItem("Currency", activeSeries.currency || "Unknown")}
        ${buildMetaItem("Geography", activeSeries.geography || "Unspecified")}
        ${buildMetaItem("Observation date", latestObservation)}
        ${buildMetaItem("Source code", activeSeries.sourceSeriesCode || "Unknown")}
      </div>
    `;

    const sourceRow = `
      <p class="detail-source">
        Source: <span class="detail-source-value">${sourceMarkup}</span>
      </p>
    `;

    const historyTable = buildHistoryTable(detailView.rows, activeSeries.decimals, activeSeries.unit);
    const toolbarGroups = [
      definition.seriesOptions.length > 1
        ? `
            <div class="detail-control-group">
              <p class="detail-control-label">Benchmark</p>
              <div class="detail-chip-row">${seriesButtons}</div>
            </div>
          `
        : "",
      `
        <div class="detail-control-group">
          <p class="detail-control-label">Window</p>
          <div class="detail-chip-row">${timeframeButtons}</div>
        </div>
      `,
    ]
      .filter(Boolean)
      .join("");

    const headMarkup = `
      <div class="detail-head-shell">
        <p class="detail-kicker">${escapeHtml(definition.primaryLabel)}</p>
        <div class="detail-head">
          <div>
          <h3 class="detail-title">${escapeHtml(activeSeries.actualSeriesName)}</h3>
          <p class="detail-subtitle">${escapeHtml(activeSeries.targetConcept || definition.primaryLabel)}</p>
          </div>
          <div class="detail-latest-wrap">
            <p class="detail-latest">${latestValue}</p>
            <p class="detail-latest-meta">Observed ${escapeHtml(latestObservation)}</p>
          </div>
        </div>
      </div>
      <div class="detail-toolbar">${toolbarGroups}</div>
    `;

    if (detailView.historyStatus === "loading") {
      return `
        ${headMarkup}
        <div class="detail-chart-wrap detail-chart-state">
          <p class="detail-chart-message">Loading published history...</p>
        </div>
        ${sourceRow}
        ${metaGrid}
        ${historyTable}
      `;
    }

    if (detailView.historyStatus === "error") {
      return `
        ${headMarkup}
        <div class="detail-chart-wrap detail-chart-state">
          <p class="detail-chart-message">Unable to load history: ${escapeHtml(detailView.error || "Unknown error")}</p>
        </div>
        ${sourceRow}
        ${metaGrid}
        ${historyTable}
      `;
    }

    if (!detailView.series.length) {
      return `
        ${headMarkup}
        <div class="detail-chart-wrap detail-chart-state">
          <p class="detail-chart-message">No published history is available for this timeframe.</p>
        </div>
        ${sourceRow}
        ${metaGrid}
        ${historyTable}
      `;
    }

    return `
      ${headMarkup}
      <div class="detail-chart-wrap">
        ${buildDetailChartSvg(detailView.series, activeSeries.decimals, activeSeries.unit, detailView.chartPalette)}
        <div class="detail-chart-bubble" data-chart-tooltip hidden></div>
        <div class="detail-chart-bubble selection" data-chart-selection-bubble hidden></div>
      </div>
      ${sourceRow}
      ${metaGrid}
      ${historyTable}
    `;
  }

  renderGlobalError(error) {
    const message = String(error.message || error);
    this.ui.featuredGrid.innerHTML = `
      <article class="viz-card viz-card-error">
        <div class="feed-cat">
          <span class="feed-cat-dot energy"></span>
          <span class="feed-cat-label">Data</span>
        </div>
        <h2 class="card-title">Unable to load commodity data</h2>
        <p class="card-meta">${escapeHtml(message)}</p>
      </article>
    `;
    this.ui.metalsGrid.innerHTML = "";
    this.ui.agriGrid.innerHTML = "";
  }

  renderAll() {
    this.definitions.forEach((definition) => {
      this.renderCommodity(definition);
    });

    this.refreshOpenDetail();
  }

  renderCommodity(definition) {
    const view = this.views.get(definition.id);
    const activeSeries = this.getActiveSeries(definition);
    if (!view || !activeSeries) {
      return;
    }

    const hasValue = isFiniteNumber(activeSeries.value);
    view.card.classList.toggle("is-unavailable", !hasValue);
    view.titleEl.textContent = definition.primaryLabel;
    view.valueEl.textContent = hasValue ? formatPrice(activeSeries.value, activeSeries.decimals, activeSeries.unit) : "N/A";
    updateDelta(view.deltaEl, activeSeries.deltaValue, activeSeries.deltaPct, activeSeries.decimals, activeSeries.unit);
    view.metaEl.textContent = buildCardMeta(definition, activeSeries);
    view.visualizer.update();
  }

  refreshOpenDetail() {
    if (!this.detailState.openCommodityId) {
      return;
    }

    const openDefinition = this.definitionsById.get(this.detailState.openCommodityId);
    if (!openDefinition) {
      return;
    }

    this.renderDetailPanel(openDefinition);
  }
}

function buildCardMeta(definition, activeSeries) {
  const parts = [];

  if (definition.seriesOptions.length > 1 && activeSeries.optionLabel) {
    parts.push(activeSeries.optionLabel);
  }

  if (activeSeries.sourceName) {
    parts.push(activeSeries.sourceName);
  }

  if (activeSeries.frequency) {
    parts.push(activeSeries.frequency.toUpperCase());
  }

  if (activeSeries.observationDate) {
    parts.push(formatDateShort(activeSeries.observationDate, { includeYear: true, compactYear: true }));
  }

  return parts.join(" · ");
}

function buildMetaItem(label, value) {
  return `
    <div class="detail-meta-item">
      <p class="detail-meta-label">${escapeHtml(label)}</p>
      <p class="detail-meta-value">${escapeHtml(value)}</p>
    </div>
  `;
}

function buildHistoryTable(rows, digits, unit) {
  if (!rows.length) {
    return `
      <div class="detail-history-table-wrap">
        <div class="detail-history-empty">No observations in this window.</div>
      </div>
    `;
  }

  const tableRows = [...rows]
    .sort((left, right) => right.observation_date.localeCompare(left.observation_date))
    .slice(0, 8)
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(formatDateLong(row.observation_date))}</td>
          <td>${escapeHtml(formatPrice(row.value, digits, unit))}</td>
          <td>${escapeHtml(row.source_name)}</td>
        </tr>
      `
    )
    .join("");

  return `
    <div class="detail-history-table-wrap">
      <div class="detail-history-head">
        <p class="detail-history-title">Recent published observations</p>
      </div>
      <table class="detail-history-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Value</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>${tableRows}</tbody>
      </table>
    </div>
  `;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function inferDisplayDigits(unit, value) {
  if (unit && /cents?/i.test(unit)) {
    return 2;
  }

  if (!isFiniteNumber(value)) {
    return 2;
  }

  const absoluteValue = Math.abs(value);

  if (absoluteValue >= 1000) {
    return 0;
  }

  if (absoluteValue >= 100) {
    return 1;
  }

  if (absoluteValue >= 10) {
    return 2;
  }

  return 3;
}

function formatNumericValue(value, digits) {
  return value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function getUnitDescriptor(unit = "USD") {
  if (!unit) {
    return { kind: "plain" };
  }

  if (unit.startsWith("USD/")) {
    return {
      kind: "usdPair",
      suffix: unit.slice(4),
    };
  }

  if (unit.startsWith("USD per ")) {
    return {
      kind: "usdPair",
      suffix: unit.replace("USD per ", ""),
    };
  }

  if (/^US cents/i.test(unit)) {
    const suffix = unit.replace(/^US cents?\s*(per)?\s*/i, "").trim();
    return {
      kind: "cents",
      suffix,
    };
  }

  if (unit === "USD") {
    return { kind: "usd" };
  }

  return {
    kind: "suffix",
    suffix: unit.toLowerCase(),
  };
}

function formatPrice(value, digits, unit = "USD", options = {}) {
  if (!isFiniteNumber(value)) {
    return options.fallback || "N/A";
  }

  const descriptor = getUnitDescriptor(unit);
  const formatted = formatNumericValue(value, digits);

  if (descriptor.kind === "usd") {
    return `$${formatted}`;
  }

  if (descriptor.kind === "usdPair") {
    return options.compactUnit ? `$${formatted}` : `$${formatted} / ${descriptor.suffix}`;
  }

  if (descriptor.kind === "cents") {
    const compactSuffix = descriptor.suffix ? `/${descriptor.suffix.toLowerCase()}` : "";
    return options.compactUnit ? `${formatted}c` : `${formatted} c${compactSuffix}`;
  }

  if (descriptor.kind === "plain") {
    return formatted;
  }

  return `${formatted} ${descriptor.suffix}`;
}

function updateDelta(target, deltaValue, deltaPct, digits, unit = "USD") {
  if (!isFiniteNumber(deltaValue)) {
    target.textContent = "No prior observation";
    target.classList.remove("up", "down");
    return;
  }

  const changeText = formatSigned(deltaValue, digits, unit);
  const pctText = isFiniteNumber(deltaPct) ? ` (${formatSigned(deltaPct, 2)}%)` : "";
  target.textContent = `${changeText}${pctText}`;
  target.classList.toggle("up", deltaValue > 0);
  target.classList.toggle("down", deltaValue < 0);
}

function formatSigned(value, digits, unit = null) {
  if (!isFiniteNumber(value)) {
    return "N/A";
  }

  const sign = value >= 0 ? "+" : "-";
  const absoluteValue = Math.abs(value);

  if (!unit) {
    return `${sign}${absoluteValue.toFixed(digits)}`;
  }

  return `${sign}${formatPrice(absoluteValue, digits, unit)}`;
}

function formatDateKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDateShort(dateIso, options = {}) {
  if (!dateIso) {
    return "--";
  }

  const date = new Date(`${dateIso}T00:00:00`);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }

  return date.toLocaleDateString([], {
    month: "short",
    day: "numeric",
    year: options.includeYear ? (options.compactYear ? "2-digit" : "numeric") : undefined,
  });
}

function formatDateLong(dateIso) {
  if (!dateIso) {
    return "Unavailable";
  }

  const date = new Date(`${dateIso}T00:00:00`);
  if (Number.isNaN(date.getTime())) {
    const directDate = new Date(dateIso);
    if (Number.isNaN(directDate.getTime())) {
      return "Unavailable";
    }

    return directDate.toLocaleDateString([], {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  return date.toLocaleDateString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatChartAxisDate(dateIso, options = {}) {
  if (!dateIso) {
    return "--";
  }

  const date = new Date(`${dateIso}T00:00:00`);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }

  return date.toLocaleDateString([], {
    year: "numeric",
    month: "short",
    day: options.includeDay ? "numeric" : undefined,
  });
}

function formatTimestamp(dateValue) {
  if (!dateValue) {
    return "Unavailable";
  }

  const date = new Date(dateValue);
  if (Number.isNaN(date.getTime())) {
    return "Unavailable";
  }

  return date.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function hexToRgb(hexColor) {
  const value = hexColor.replace("#", "");
  const expanded = value.length === 3 ? value.split("").map((part) => `${part}${part}`).join("") : value;

  if (!/^[0-9a-fA-F]{6}$/.test(expanded)) {
    return null;
  }

  return {
    r: Number.parseInt(expanded.slice(0, 2), 16),
    g: Number.parseInt(expanded.slice(2, 4), 16),
    b: Number.parseInt(expanded.slice(4, 6), 16),
  };
}

function toRgba(rgb, alpha) {
  return `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${alpha})`;
}

function mixRgb(base, overlay, ratio) {
  const t = clamp(ratio, 0, 1);
  return {
    r: Math.round(base.r * (1 - t) + overlay.r * t),
    g: Math.round(base.g * (1 - t) + overlay.g * t),
    b: Math.round(base.b * (1 - t) + overlay.b * t),
  };
}

function getCommodityTheme(definition) {
  if (definition.visual.type === "energyTile") {
    const family = definition.visual.tile.family;
    return ENERGY_TILE_THEME[family] || ENERGY_TILE_THEME.default;
  }

  if (definition.visual.type === "periodicTile") {
    const family = definition.visual.tile.family;
    return PERIODIC_TILE_THEME[family] || PERIODIC_TILE_THEME.default;
  }

  if (definition.visual.type === "agriTile") {
    const family = definition.visual.tile.family;
    return AGRI_TILE_THEME[family] || AGRI_TILE_THEME.default;
  }

  return { top: "#cedbe7", mid: "#95a8bc", bottom: "#596f84" };
}

function getDetailBackdropTheme(definition) {
  const commodityTheme = getCommodityTheme(definition);
  const topRgb = hexToRgb(commodityTheme.top || commodityTheme.mid || "#95a8bc");
  const midRgb = hexToRgb(commodityTheme.mid || "#95a8bc");
  const bottomRgb = hexToRgb(commodityTheme.bottom || commodityTheme.mid || "#596f84");
  const shadeRgb = { r: 8, g: 16, b: 25 };

  if (!topRgb || !midRgb || !bottomRgb) {
    return {
      top: "rgba(17, 29, 40, 0.98)",
      mid: "rgba(11, 20, 29, 0.98)",
      bottom: "rgba(16, 27, 38, 0.98)",
    };
  }

  return {
    top: toRgba(mixRgb(topRgb, shadeRgb, 0.58), 0.98),
    mid: toRgba(mixRgb(midRgb, shadeRgb, 0.66), 0.98),
    bottom: toRgba(mixRgb(bottomRgb, shadeRgb, 0.74), 0.98),
  };
}

function getDetailBubbleTheme(definition) {
  const commodityTheme = getCommodityTheme(definition);
  const topRgb = hexToRgb(commodityTheme.top || commodityTheme.mid || "#95a8bc");
  const midRgb = hexToRgb(commodityTheme.mid || "#95a8bc");
  const shadeRgb = { r: 7, g: 14, b: 21 };
  const lightRgb = { r: 244, g: 248, b: 252 };

  if (!topRgb || !midRgb) {
    return {
      background: "rgba(7, 14, 21, 0.9)",
      border: "rgba(196, 214, 231, 0.22)",
      ink: "rgba(248, 251, 255, 0.98)",
      subtle: "rgba(196, 214, 231, 0.82)",
    };
  }

  return {
    background: toRgba(mixRgb(midRgb, shadeRgb, 0.72), 0.94),
    border: toRgba(mixRgb(topRgb, lightRgb, 0.28), 0.34),
    ink: toRgba(mixRgb(topRgb, lightRgb, 0.78), 0.98),
    subtle: toRgba(mixRgb(topRgb, lightRgb, 0.58), 0.82),
  };
}

function getDetailChartPalette(definition) {
  const commodityTheme = getCommodityTheme(definition);
  const rgb = hexToRgb(commodityTheme.mid || "#7ec4ff");

  if (!rgb) {
    return {
      line: "rgba(126, 196, 255, 0.95)",
      fillTop: "rgba(101, 166, 236, 0.38)",
      fillBottom: "rgba(101, 166, 236, 0.02)",
      grid: "rgba(185, 205, 225, 0.22)",
      label: "rgba(196, 214, 231, 0.78)",
    };
  }

  return {
    line: toRgba(rgb, 0.95),
    fillTop: toRgba(rgb, 0.38),
    fillBottom: toRgba(rgb, 0.03),
    grid: toRgba(rgb, 0.22),
    label: toRgba(rgb, 0.78),
  };
}

function normalizeRangeSelection(startIndex, endIndex, lastIndex, options = {}) {
  const minSpan = options.minSpan || 0;
  let safeStart = clamp(Math.min(startIndex, endIndex), 0, Math.max(lastIndex, 0));
  let safeEnd = clamp(Math.max(startIndex, endIndex), 0, Math.max(lastIndex, 0));

  if (safeEnd - safeStart < minSpan) {
    safeEnd = Math.min(lastIndex, safeStart + minSpan);
    if (safeEnd - safeStart < minSpan) {
      safeStart = Math.max(0, safeEnd - minSpan);
    }
  }

  return {
    startIndex: safeStart,
    endIndex: safeEnd,
  };
}

function getSeriesRangeStats(series, startIndex, endIndex) {
  if (!series.length) {
    return {
      startIndex: 0,
      endIndex: 0,
      startPoint: { date: "", value: 0 },
      endPoint: { date: "", value: 0 },
      delta: 0,
      deltaPct: 0,
      deltaClass: "",
      high: 0,
      low: 0,
    };
  }

  const normalized = normalizeRangeSelection(startIndex, endIndex, series.length - 1);
  const startPoint = series[normalized.startIndex];
  const endPoint = series[normalized.endIndex];
  const delta = endPoint.value - startPoint.value;
  const deltaPct = startPoint.value === 0 ? 0 : (delta / startPoint.value) * 100;
  const deltaClass = delta > 0 ? "up" : delta < 0 ? "down" : "";
  const values = series.map((point) => point.value);

  return {
    ...normalized,
    startPoint,
    endPoint,
    delta,
    deltaPct,
    deltaClass,
    high: Math.max(...values),
    low: Math.min(...values),
  };
}

function buildLinePathSegment(series, startIndex, endIndex, xCoord, yCoord) {
  return series
    .slice(startIndex, endIndex + 1)
    .map((point, offset) => {
      const index = startIndex + offset;
      return `${offset === 0 ? "M" : "L"}${xCoord(index).toFixed(2)} ${yCoord(point.value).toFixed(2)}`;
    })
    .join(" ");
}

function buildDetailChartSvg(series, digits, unit, palette) {
  if (!series.length) {
    return `
      <svg class="detail-chart" viewBox="0 0 860 360" preserveAspectRatio="none" aria-label="Commodity price history chart">
        <text x="430" y="180" fill="${palette.label}" font-size="14" text-anchor="middle" font-family="IBM Plex Mono, monospace">
          No history available
        </text>
      </svg>
    `;
  }

  const width = 860;
  const height = 360;
  const pad = { top: 22, right: 24, bottom: 40, left: 56 };
  const values = series.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(max - min, 0.0001);
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;
  const xStep = innerWidth / Math.max(series.length - 1, 1);

  const xCoord = (index) => pad.left + index * xStep;
  const yCoord = (value) => pad.top + ((max - value) / span) * innerHeight;
  const linePath = buildLinePathSegment(series, 0, series.length - 1, xCoord, yCoord);
  const lastIndex = series.length - 1;
  const areaPath = `${linePath} L ${xCoord(lastIndex).toFixed(2)} ${(pad.top + innerHeight).toFixed(2)} L ${xCoord(
    0
  ).toFixed(2)} ${(pad.top + innerHeight).toFixed(2)} Z`;
  const selectionStartX = xCoord(0);
  const selectionEndX = xCoord(Math.max(lastIndex, 0));
  const selectionPath = buildLinePathSegment(series, 0, Math.max(lastIndex, 0), xCoord, yCoord);
  const selectionStartPoint = series[0];
  const selectionEndPoint = series[Math.max(lastIndex, 0)];

  const gridRows = 5;
  const gridMarkup = [];

  for (let row = 0; row < gridRows; row += 1) {
    const t = row / (gridRows - 1);
    const y = pad.top + innerHeight * t;
    const value = max - span * t;
    gridMarkup.push(
      `<line x1="${pad.left}" y1="${y.toFixed(2)}" x2="${(pad.left + innerWidth).toFixed(2)}" y2="${y.toFixed(
        2
      )}" stroke="${palette.grid}" stroke-width="1" />`
    );
    gridMarkup.push(
      `<text x="${(pad.left - 10).toFixed(2)}" y="${(y + 4).toFixed(
        2
      )}" fill="${palette.label}" font-size="10" text-anchor="end" font-family="IBM Plex Mono, monospace">${formatPrice(
        value,
        digits,
        unit,
        { compactUnit: true }
      )}</text>`
    );
  }

  const firstDate = new Date(`${series[0].date}T00:00:00`);
  const lastDate = new Date(`${series[lastIndex].date}T00:00:00`);
  const spanDays =
    Number.isNaN(firstDate.getTime()) || Number.isNaN(lastDate.getTime())
      ? 0
      : Math.max(0, Math.round((lastDate.getTime() - firstDate.getTime()) / 86_400_000));
  const tickRatios = [0, 0.25, 0.5, 0.75, 1];
  const tickIndices = [...new Set(tickRatios.map((ratio) => Math.round(lastIndex * ratio)))];
  const xAxisMarkup = tickIndices
    .map((index, tickPosition) => {
      const x = xCoord(index);
      const isFirst = tickPosition === 0;
      const isLast = tickPosition === tickIndices.length - 1;
      return `
        <text
          x="${x.toFixed(2)}"
          y="${height - 10}"
          fill="${palette.label}"
          font-size="10"
          text-anchor="${isFirst ? "start" : isLast ? "end" : "middle"}"
          font-family="IBM Plex Mono, monospace"
        >${formatChartAxisDate(series[index].date, { includeDay: spanDays <= 45 })}</text>
      `;
    })
    .join("");

  return `
    <svg class="detail-chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Commodity price history chart">
      <defs>
        <linearGradient id="chart-fill-gradient" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${palette.fillTop}" />
          <stop offset="100%" stop-color="${palette.fillBottom}" />
        </linearGradient>
      </defs>
      ${gridMarkup.join("")}
      <path d="${areaPath}" fill="url(#chart-fill-gradient)" />
      <path d="${linePath}" fill="none" stroke="${palette.line}" stroke-width="2.5" />
      <rect class="detail-chart-selection" x="${selectionStartX.toFixed(2)}" y="${pad.top.toFixed(2)}" width="${Math.max(
        selectionEndX - selectionStartX,
        2
      ).toFixed(2)}" height="${innerHeight.toFixed(
        2
      )}" fill="${palette.fillTop}" opacity="0.16" rx="8" ry="8" hidden style="display:none" />
      <path class="detail-chart-selection-segment" d="${selectionPath}" fill="none" stroke="${palette.line}" stroke-width="4" hidden style="display:none" />
      <line class="detail-chart-selection-start" x1="${selectionStartX.toFixed(2)}" y1="${pad.top.toFixed(
        2
      )}" x2="${selectionStartX.toFixed(2)}" y2="${(pad.top + innerHeight).toFixed(
        2
      )}" stroke="${palette.label}" stroke-width="1.4" stroke-dasharray="4 4" hidden style="display:none" />
      <line class="detail-chart-selection-end" x1="${selectionEndX.toFixed(2)}" y1="${pad.top.toFixed(
        2
      )}" x2="${selectionEndX.toFixed(2)}" y2="${(pad.top + innerHeight).toFixed(
        2
      )}" stroke="${palette.label}" stroke-width="1.4" stroke-dasharray="4 4" hidden style="display:none" />
      <circle class="detail-chart-selection-dot start" cx="${selectionStartX.toFixed(2)}" cy="${yCoord(
        selectionStartPoint.value
      ).toFixed(2)}" r="5.2" fill="${palette.line}" stroke="${palette.fillTop}" stroke-width="2" hidden style="display:none" />
      <circle class="detail-chart-selection-dot end" cx="${selectionEndX.toFixed(2)}" cy="${yCoord(
        selectionEndPoint.value
      ).toFixed(2)}" r="5.2" fill="${palette.line}" stroke="${palette.fillTop}" stroke-width="2" hidden style="display:none" />
      <line class="detail-chart-hover-line" x1="${selectionStartX.toFixed(2)}" y1="${pad.top.toFixed(
        2
      )}" x2="${selectionStartX.toFixed(2)}" y2="${(pad.top + innerHeight).toFixed(
        2
      )}" stroke="${palette.label}" stroke-width="1.25" stroke-dasharray="3 4" hidden style="display:none" />
      <circle class="detail-chart-hover-dot" cx="${selectionStartX.toFixed(2)}" cy="${yCoord(
        selectionStartPoint.value
      ).toFixed(2)}" r="4.8" fill="${palette.line}" stroke="${palette.fillTop}" stroke-width="2" hidden style="display:none" />
      ${xAxisMarkup}
    </svg>
  `;
}

const engine = new CommodityWatchEngine(new CommodityApiClient());
engine.init();
