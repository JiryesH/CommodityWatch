import {
  ALWAYS_CALENDAR_SECTORS,
  ALWAYS_HEADLINE_CATEGORIES,
  DEFAULT_HOME_SERIES_KEYS,
  SECTOR_META,
  getCommodityById,
  getCommodityBySeriesKey,
  getGroupById,
  getGroupCalendarPattern,
  getGroupFeedPattern,
  getSectorById,
  getCommodityCalendarPattern,
} from "./config.js";
import {
  fetchCalendarEvents,
  fetchHeadlineFeed,
  fetchInventorySnapshot,
  fetchLatestSeriesMap,
  fetchRelatedHeadlines,
  fetchSeriesHistory,
} from "./data-client.js";
import {
  getFilterLabel,
  getSelectedCommodities,
  getSelectedGroup,
  getSelectedGroups,
  getSelectedSectors,
  hasPartialGroupSelection,
  isAllFilter,
  normalizeFilter,
} from "./filter-state.js";
import {
  canonicalCategoriesForArticle as canonicalHeadlineCategories,
  dotClass as headlineDotClass,
  dotLabel as headlineDotLabel,
} from "../shared/headline-taxonomy.js";
import {
  FRESHNESS_BADGES,
  commodityGroupForCode,
  formatSignedValue as formatInventorySignedValue,
  formatValue as formatInventoryValue,
} from "../inventory-watch/catalog.js";
import { buildInventoryDetailHref, buildInventorySnapshotHref } from "../inventory-watch/router.js";

const headlineExactFormatter = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "short",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const priceObservationFormatter = new Intl.DateTimeFormat("en-GB", {
  day: "2-digit",
  month: "short",
  year: "numeric",
  timeZone: "UTC",
});

const calendarDayFormatter = new Intl.DateTimeFormat("en-GB", {
  weekday: "short",
  day: "numeric",
  month: "short",
  timeZone: "UTC",
});

const calendarTimeFormatter = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function parseIsoDate(value) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}

function toIsoDate(value) {
  return new Date(value).toISOString().slice(0, 10);
}

function addUtcDays(value, days) {
  const next = new Date(value);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function exactHeadlineTime(value) {
  const parsed = parseIsoDate(value);
  return parsed ? headlineExactFormatter.format(parsed) : "";
}

function relativeHeadlineTime(value) {
  const parsed = parseIsoDate(value);
  if (!parsed) {
    return "—";
  }

  const deltaSeconds = Math.round((Date.now() - parsed.getTime()) / 1000);
  if (!Number.isFinite(deltaSeconds)) {
    return "—";
  }

  const future = deltaSeconds < 0;
  const absoluteSeconds = Math.abs(deltaSeconds);
  let unitLabel = "<1m";

  if (absoluteSeconds >= 86400) {
    unitLabel = `${Math.floor(absoluteSeconds / 86400)}d`;
  } else if (absoluteSeconds >= 3600) {
    unitLabel = `${Math.floor(absoluteSeconds / 3600)}h`;
  } else if (absoluteSeconds >= 60) {
    unitLabel = `${Math.floor(absoluteSeconds / 60)}m`;
  }

  return future ? `in ${unitLabel}` : `${unitLabel} ago`;
}

function renderHeadlineTimeMeta(value) {
  const exact = exactHeadlineTime(value);
  if (!exact) {
    return '<span class="feed-time feed-time-invalid">—</span>';
  }

  return `
    <span class="feed-time" title="${escapeHtml(exact)}">${escapeHtml(relativeHeadlineTime(value))}</span>
    <span class="feed-time-exact">(${escapeHtml(exact)})</span>
  `;
}

function sortNewest(first, second) {
  return new Date(second.published).getTime() - new Date(first.published).getTime();
}

function sortSoonest(first, second) {
  return new Date(first.event_date).getTime() - new Date(second.event_date).getTime();
}

function dedupeByKey(items, getKey) {
  const seen = new Set();
  return items.filter((item) => {
    const key = getKey(item);
    if (!key || seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function inferDecimals(value, unit) {
  if (!Number.isFinite(value)) {
    return 2;
  }

  if (/cents? per pound/i.test(unit || "")) {
    return 2;
  }

  if (Math.abs(value) >= 1000) {
    return 0;
  }
  if (Math.abs(value) >= 100) {
    return 1;
  }
  return 2;
}

function formatValue(value, unit) {
  const decimals = inferDecimals(value, unit);
  const formatted = Number(value).toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });

  switch (unit) {
    case "USD per barrel":
      return `$${formatted} / bbl`;
    case "USD per MMBtu":
      return `$${formatted} / MMBtu`;
    case "USD per metric ton":
      return `$${formatted} / t`;
    case "USD per dry metric ton":
      return `$${formatted} / dmt`;
    case "USD per troy ounce":
      return `$${formatted} / oz`;
    case "US cents per pound":
      return `${formatted} c / lb`;
    case "USD per kilogram":
      return `$${formatted} / kg`;
    case "USD per gallon":
      return `$${formatted} / gal`;
    default:
      return `${formatted}${unit ? ` ${unit}` : ""}`;
  }
}

function formatChange(value, percent, unit) {
  const decimals = inferDecimals(value, unit);
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  const absoluteValue = Math.abs(value);
  const absoluteFormatted = Number(absoluteValue).toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
  const percentFormatted = Number(Math.abs(percent || 0)).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  return {
    absolute: `${sign}${absoluteFormatted}`,
    percent: `${sign}${percentFormatted}%`,
  };
}

function formatObservationDate(value) {
  const parsed = parseIsoDate(value);
  return parsed ? priceObservationFormatter.format(parsed) : "Unknown date";
}

function renderLaunchIcon() {
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M14 4h6v6"></path>
      <path d="M10 14 20 4"></path>
      <path d="M20 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h5"></path>
    </svg>
  `;
}

function bindNavigateLinks(root, onNavigate) {
  root.querySelectorAll("[data-route]").forEach((button) => {
    button.addEventListener("click", () => {
      if (typeof onNavigate === "function") {
        onNavigate(button.dataset.route);
      }
    });
  });
}

function createModuleCard({ id, title, linkLabel, route, accentSectorId, onNavigate }) {
  const section = document.createElement("section");
  section.className = "dashboard-module";
  section.dataset.moduleId = id;
  section.style.setProperty("--module-accent", SECTOR_META[accentSectorId]?.accent || "var(--color-amber)");
  section.innerHTML = `
    <div class="module-card">
      <header class="module-header">
        <h2 class="module-title">${escapeHtml(title)}</h2>
        ${
          linkLabel
            ? `
              <button class="module-link" type="button" data-route="${escapeHtml(route)}">
                <span>${escapeHtml(linkLabel)}</span>
                ${renderLaunchIcon()}
              </button>
            `
            : ""
        }
      </header>
      <div class="module-body">
        <p class="module-loading">Loading…</p>
      </div>
    </div>
  `;

  bindNavigateLinks(section, onNavigate);
  return {
    section,
    body: section.querySelector(".module-body"),
  };
}

function renderEmptyState(title, copy) {
  return `
    <div class="module-empty">
      <p class="module-empty-title">${escapeHtml(title)}</p>
      <p class="module-empty-copy">${escapeHtml(copy)}</p>
    </div>
  `;
}

function renderErrorState(copy) {
  return renderEmptyState("Module unavailable", copy);
}

export function createModuleRenderGate(section) {
  const requestId = (section.__requestId || 0) + 1;
  section.__requestId = requestId;

  return {
    isCurrent() {
      return section.__requestId === requestId;
    },
    commit(body, html) {
      if (section.__requestId === requestId) {
        body.innerHTML = html;
      }
    },
  };
}

function renderHeadlineSentimentMeta(sentiment) {
  const tone = String(sentiment || "").toLowerCase();
  if (!tone || !["positive", "negative", "neutral"].includes(tone)) {
    return "";
  }

  const label = tone.charAt(0).toUpperCase() + tone.slice(1);
  return `
    <span class="feed-sent ${escapeHtml(tone)}">
      <span class="feed-sent-label">Sentiment:</span> ${escapeHtml(label)}
    </span>
  `;
}

function renderSparkline(values, direction = "neutral") {
  const points = values.length ? values : [0, 0];
  const minValue = Math.min(...points);
  const maxValue = Math.max(...points);
  const width = 132;
  const height = 42;
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
    <svg class="sparkline is-${escapeHtml(direction)}" viewBox="0 0 ${width} ${height}" aria-hidden="true">
      <path class="sparkline-area" d="${area}"></path>
      <path class="sparkline-line" d="${line}"></path>
    </svg>
  `;
}

function sparklineTrend(values = []) {
  if (!values.length || values.length < 2) {
    return "neutral";
  }

  const delta = values[values.length - 1] - values[0];
  if (delta > 0) {
    return "up";
  }
  if (delta < 0) {
    return "down";
  }
  return "neutral";
}

const INVENTORY_GROUP_RANK = new Map([
  ["Crude Oil", 0],
  ["Refined Products", 1],
  ["Natural Gas", 2],
  ["Base Metals", 3],
  ["Grains", 4],
  ["Softs", 5],
  ["Precious Metals", 6],
  ["Inventory", 7],
]);

const INVENTORY_COMMODITY_CODES_BY_GROUP = {
  "crude-oil": ["crude_oil"],
  "natural-gas": ["natural_gas"],
  precious: ["gold", "silver", "platinum", "palladium"],
  "base-metals": ["copper", "aluminum", "nickel", "zinc", "lead", "tin"],
  "grains-oilseeds": ["corn", "wheat", "soybeans", "oilseeds"],
  softs: ["coffee", "cocoa", "sugar", "cotton"],
};

const INVENTORY_COMMODITY_CODES_BY_COMMODITY = {
  brent: ["crude_oil"],
  wti: ["crude_oil"],
  "dubai-oman": ["crude_oil"],
  "henry-hub": ["natural_gas"],
  ttf: ["natural_gas"],
  gold: ["gold"],
  silver: ["silver"],
  platinum: ["platinum"],
  copper: ["copper"],
  aluminium: ["aluminum"],
  nickel: ["nickel"],
  zinc: ["zinc"],
  "iron-ore": [],
  wheat: ["wheat"],
  corn: ["corn"],
  soybeans: ["soybeans"],
  coffee: ["coffee"],
  sugar: ["sugar"],
  cocoa: ["cocoa"],
};

const INVENTORY_GROUPS_BY_SECTOR = {
  energy: ["energy", "natural-gas"],
  metals: ["base-metals", "precious-metals"],
  agriculture: ["grains", "softs"],
};

function buildSparklineValues(historyRows, latestRow) {
  const values = historyRows
    .map((row) => ({ observation_date: row.observation_date, value: row.value }))
    .filter((row) => Number.isFinite(row.value))
    .sort((left, right) => left.observation_date.localeCompare(right.observation_date))
    .slice(-30)
    .map((row) => row.value);

  if (values.length) {
    return values;
  }

  if (Number.isFinite(latestRow?.value)) {
    return [latestRow.value, latestRow.value];
  }

  return [0, 0];
}

function inventorySignalAccent(card) {
  if (card.freshness === "aged") {
    return "var(--color-amber)";
  }

  if (card.signal === "tightening" || card.signal === "contracting") {
    return "var(--color-up)";
  }

  if (card.signal === "loosening" || card.signal === "expanding") {
    return "var(--color-down)";
  }

  return "rgba(15, 25, 35, 0.16)";
}

function inventorySignalLabel(signal) {
  return String(signal || "neutral")
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function sortInventoryCards(first, second) {
  const firstRank = INVENTORY_GROUP_RANK.get(first.snapshotGroup) ?? 999;
  const secondRank = INVENTORY_GROUP_RANK.get(second.snapshotGroup) ?? 999;
  if (firstRank !== secondRank) {
    return firstRank - secondRank;
  }

  return first.name.localeCompare(second.name);
}

function getInventorySelection(filter, cards) {
  const normalized = normalizeFilter(filter);
  if (isAllFilter(normalized)) {
    return {
      items: [...cards].sort(sortInventoryCards),
      scopeLabel: "All Commodities",
      narrowedByGroup: false,
    };
  }

  const selectedGroup = getSelectedGroup(normalized);
  const narrowedByGroup = Boolean(selectedGroup || hasPartialGroupSelection(normalized));
  const mappedCommodityCodes = new Set();

  if (selectedGroup && !isAllCommoditySelection(normalized)) {
    getSelectedCommodities(normalized).forEach((commodity) => {
      (INVENTORY_COMMODITY_CODES_BY_COMMODITY[commodity.id] || []).forEach((code) => mappedCommodityCodes.add(code));
    });
  }

  if (!mappedCommodityCodes.size) {
    getSelectedGroups(normalized).forEach((group) => {
      (INVENTORY_COMMODITY_CODES_BY_GROUP[group.id] || []).forEach((code) => mappedCommodityCodes.add(code));
    });
  }

  if (mappedCommodityCodes.size) {
    return {
      items: cards.filter((card) => mappedCommodityCodes.has(card.commodityCode)).sort(sortInventoryCards),
      scopeLabel: getFilterLabel(filter),
      narrowedByGroup,
    };
  }

  if (narrowedByGroup) {
    return {
      items: [],
      scopeLabel: getFilterLabel(filter),
      narrowedByGroup,
    };
  }

  const sectorGroups = new Set(
    getSelectedSectors(normalized).flatMap((sector) => INVENTORY_GROUPS_BY_SECTOR[sector.id] || [])
  );

  return {
    items: cards.filter((card) => sectorGroups.has(commodityGroupForCode(card.commodityCode))).sort(sortInventoryCards),
    scopeLabel: getFilterLabel(filter),
    narrowedByGroup: false,
  };
}

function renderInventoryPreviewCard(card) {
  const groupSlug = commodityGroupForCode(card.commodityCode);
  const route = buildInventoryDetailHref(groupSlug === "all" ? "all" : groupSlug, card.indicatorId);
  const freshnessBadge = FRESHNESS_BADGES[card.freshness] || FRESHNESS_BADGES.current;

  return `
    <button
      class="inventory-preview-card"
      type="button"
      data-route="${escapeHtml(route)}"
      style="--inventory-accent:${escapeHtml(inventorySignalAccent(card))};"
      aria-label="Open ${escapeHtml(card.name)} in InventoryWatch"
    >
      <div class="inventory-preview-head">
        <p class="inventory-preview-group">${escapeHtml(card.snapshotGroup)}</p>
        <span class="inventory-preview-badge is-${escapeHtml(freshnessBadge.tone)}">${escapeHtml(freshnessBadge.label)}</span>
      </div>
      <h3 class="inventory-preview-title">${escapeHtml(card.name)}</h3>
      <p class="inventory-preview-value">${escapeHtml(formatInventoryValue(card.latestValue, card.unit))}</p>
      <p class="inventory-preview-change">
        ${escapeHtml(formatInventorySignedValue(card.changeAbs, card.unit))} vs prior
        <span class="sep">·</span>
        ${escapeHtml(inventorySignalLabel(card.signal))}
      </p>
      <div class="inventory-preview-foot">
        <div class="inventory-preview-source">
          <span>${escapeHtml(card.sourceLabel)}</span>
          <span>${escapeHtml(card.freshness)}</span>
        </div>
        ${renderSparkline(card.sparkline, sparklineTrend(card.sparkline))}
      </div>
    </button>
  `;
}

function bindPriceTileInteractions(body, onNavigate) {
  if (typeof body.__priceTileCleanup === "function") {
    body.__priceTileCleanup();
    body.__priceTileCleanup = null;
  }

  const handleClick = (event) => {
    const tile = event.target.closest("[data-price-route]");
    if (!tile || !body.contains(tile)) {
      return;
    }

    if (typeof onNavigate === "function") {
      onNavigate(tile.dataset.priceRoute);
    }
  };

  const handlePointerMove = (event) => {
    const tile = event.target.closest("[data-price-route]");
    if (!tile || !body.contains(tile)) {
      return;
    }

    const rect = tile.getBoundingClientRect();
    tile.style.setProperty("--pointer-x", `${event.clientX - rect.left}px`);
    tile.style.setProperty("--pointer-y", `${event.clientY - rect.top}px`);
  };

  body.addEventListener("click", handleClick);
  body.addEventListener("pointermove", handlePointerMove);

  body.__priceTileCleanup = () => {
    body.removeEventListener("click", handleClick);
    body.removeEventListener("pointermove", handlePointerMove);
  };
}

function bindPriceStripCarousel(body) {
  if (typeof body.__priceStripCleanup === "function") {
    body.__priceStripCleanup();
    body.__priceStripCleanup = null;
  }

  const strip = body.querySelector("[data-price-strip]");
  const track = body.querySelector("[data-price-track]");
  if (!strip || !track) {
    return;
  }

  let animationCleanup = null;
  let resizeRafId = 0;

  const removeClones = () => {
    track.querySelectorAll("[data-price-clone='true']").forEach((clone) => clone.remove());
    track.style.transform = "";
  };

  const teardownAnimation = () => {
    if (typeof animationCleanup === "function") {
      animationCleanup();
      animationCleanup = null;
    }
  };

  const mountCarousel = () => {
    teardownAnimation();
    removeClones();

    const baseCards = [...track.children];
    if (!baseCards.length) {
      strip.classList.remove("is-carousel");
      return;
    }

    const baseWidth = track.scrollWidth;
    const viewportWidth = strip.clientWidth;

    if (baseWidth <= viewportWidth + 8) {
      strip.classList.remove("is-carousel");
      return;
    }

    strip.classList.add("is-carousel");

    baseCards.forEach((card) => {
      const clone = card.cloneNode(true);
      clone.dataset.priceClone = "true";
      clone.setAttribute("aria-hidden", "true");
      clone.tabIndex = -1;
      track.appendChild(clone);
    });

    const firstClone = track.querySelector("[data-price-clone='true']");
    const loopWidth = firstClone ? firstClone.offsetLeft : baseWidth;
    if (loopWidth <= 0) {
      strip.classList.remove("is-carousel");
      removeClones();
      return;
    }

    let rafId = 0;
    let lastTimestamp = 0;
    let offset = 0;
    let paused = false;
    let resumeAt = 0;

    const wrapOffset = (value) => {
      if (!loopWidth) {
        return 0;
      }

      let nextValue = value;
      while (nextValue <= -loopWidth) {
        nextValue += loopWidth;
      }
      while (nextValue > 0) {
        nextValue -= loopWidth;
      }
      return nextValue;
    };

    const applyOffset = (value) => {
      offset = wrapOffset(value);
      track.style.transform = `translate3d(${offset}px, 0, 0)`;
    };

    const pause = () => {
      paused = true;
    };

    const resume = () => {
      paused = false;
    };

    const handleWheel = (event) => {
      const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
      if (!delta) {
        return;
      }

      event.preventDefault();
      paused = true;
      resumeAt = performance.now() + 1400;
      applyOffset(offset - delta * 0.75);
    };

    const handlePointerDown = (event) => {
      if (event.pointerType !== "touch") {
        return;
      }

      pause();
    };

    const handlePointerUp = () => {
      resumeAt = performance.now() + 900;
    };

    const step = (timestamp) => {
      if (!track.isConnected) {
        return;
      }

      if (!lastTimestamp) {
        lastTimestamp = timestamp;
      }

      const delta = timestamp - lastTimestamp;
      lastTimestamp = timestamp;

      if (resumeAt && timestamp >= resumeAt) {
        paused = false;
        resumeAt = 0;
      }

      if (!paused) {
        applyOffset(offset - delta * 0.028);
      }

      rafId = window.requestAnimationFrame(step);
    };

    strip.addEventListener("mouseenter", pause);
    strip.addEventListener("mouseleave", resume);
    strip.addEventListener("focusin", pause);
    strip.addEventListener("focusout", resume);
    strip.addEventListener("wheel", handleWheel, { passive: false });
    strip.addEventListener("pointerdown", handlePointerDown, { passive: true });
    strip.addEventListener("pointerup", handlePointerUp, { passive: true });
    strip.addEventListener("pointercancel", handlePointerUp, { passive: true });

    rafId = window.requestAnimationFrame(step);

    animationCleanup = () => {
      window.cancelAnimationFrame(rafId);
      strip.removeEventListener("mouseenter", pause);
      strip.removeEventListener("mouseleave", resume);
      strip.removeEventListener("focusin", pause);
      strip.removeEventListener("focusout", resume);
      strip.removeEventListener("wheel", handleWheel);
      strip.removeEventListener("pointerdown", handlePointerDown);
      strip.removeEventListener("pointerup", handlePointerUp);
      strip.removeEventListener("pointercancel", handlePointerUp);
      removeClones();
      strip.classList.remove("is-carousel");
    };
  };

  const handleResize = () => {
    window.cancelAnimationFrame(resizeRafId);
    resizeRafId = window.requestAnimationFrame(mountCarousel);
  };

  const resizeObserver =
    typeof window.ResizeObserver === "function" ? new window.ResizeObserver(handleResize) : null;

  resizeObserver?.observe(strip);
  window.addEventListener("resize", handleResize, { passive: true });

  mountCarousel();

  body.__priceStripCleanup = () => {
    window.cancelAnimationFrame(resizeRafId);
    teardownAnimation();
    resizeObserver?.disconnect();
    window.removeEventListener("resize", handleResize);
    removeClones();
  };
}

function getPriceSelection(filter) {
  const normalized = normalizeFilter(filter);

  if (isAllFilter(normalized)) {
    return DEFAULT_HOME_SERIES_KEYS
      .map((seriesKey) => getCommodityBySeriesKey(seriesKey))
      .filter(Boolean);
  }

  const selectedGroup = getSelectedGroup(normalized);
  if (selectedGroup) {
    return getSelectedCommodities(normalized).filter((commodity) => commodity?.seriesKey);
  }

  return getSelectedGroups(normalized)
    .flatMap((group) => group.commodities)
    .map((commodity) => getCommodityById(commodity.id))
    .filter((commodity) => commodity?.seriesKey);
}

function buildHeadlineText(article) {
  return `${article?.title || ""} ${article?.description || ""}`.trim();
}

function articleHasCategory(article, categories) {
  return article.categories.some((category) => categories.includes(category));
}

function matchesSectorFeed(article, sectorId) {
  const sector = getSectorById(sectorId);
  return Boolean(sector && articleHasCategory(article, sector.feedCategories));
}

function matchesGroupFeed(article, groupId) {
  const group = getGroupById(groupId);
  if (!group || !articleHasCategory(article, group.feedCategories)) {
    return false;
  }

  const pattern = getGroupFeedPattern(groupId);
  const requiresKeyword =
    group.feedCategories.every((category) => category === "Metals" || category === "Agriculture" || category === "Fertilizers") ||
    group.id === "power";

  if (!requiresKeyword || !pattern) {
    return true;
  }

  return pattern.test(buildHeadlineText(article));
}

function getAlwaysRelevantHeadlines(feedArticles) {
  return feedArticles.filter((article) => article.categories.some((category) => ALWAYS_HEADLINE_CATEGORIES.includes(category)));
}

function fallbackHeadlineLabel(commodityMetaList = []) {
  return commodityMetaList[0]?.label || "General";
}

function fallbackHeadlineDotClass(commodityMetaList = []) {
  const sectorId = commodityMetaList[0]?.sectorId;
  if (sectorId === "metals") {
    return "metals";
  }
  if (sectorId === "agriculture") {
    return "agri";
  }
  return "energy";
}

function createHeadlineItem(article, overrides = {}, commodityMetaList = []) {
  const categories = article ? canonicalHeadlineCategories(article) : [];
  const primaryCategory = categories[0] || "General";
  const headlineLabel = article ? headlineDotLabel(article) : fallbackHeadlineLabel(commodityMetaList);
  const dotClass = article ? headlineDotClass(article) : fallbackHeadlineDotClass(commodityMetaList);

  return {
    id: overrides.id || article?.id || article?.link || `${overrides.title || article?.title || "headline"}|${overrides.published || article?.published || ""}`,
    title: overrides.title || article?.title || "",
    url: overrides.url || article?.link || "",
    source: overrides.source || article?.source || "Unknown source",
    published: overrides.published || article?.published || "",
    sentiment: overrides.sentiment || article?.sentiment?.label || "",
    primaryCategory,
    metaCategoryLabel: primaryCategory !== "General" ? headlineLabel : "",
    dotLabel: headlineLabel,
    dotClass,
  };
}

function enrichRelatedHeadline(row, feedById, commodityMetaList) {
  const article = row.id ? feedById.get(row.id) : null;
  return createHeadlineItem(
    article,
    {
      id: row.id || row.link || `${row.title}|${row.published || ""}`,
      title: row.title,
      url: row.link || "",
      source: row.source || article?.source || "Unknown source",
      published: row.published || article?.published || "",
      sentiment: article?.sentiment?.label || "",
    },
    commodityMetaList
  );
}

function mergeRelatedHeadlineSets(relatedRows, feedById) {
  const merged = new Map();

  relatedRows.forEach(({ commodityMeta, rows }) => {
    rows.forEach((row) => {
      const key = row.id || row.link || `${row.title}|${row.published || ""}`;
      if (!merged.has(key)) {
        merged.set(key, {
          row,
          commodityMetaList: [commodityMeta],
        });
        return;
      }

      const existing = merged.get(key);
      const seenCommodity = existing.commodityMetaList.some((candidate) => candidate.id === commodityMeta.id);
      if (!seenCommodity) {
        existing.commodityMetaList.push(commodityMeta);
      }
    });
  });

  return [...merged.values()]
    .map(({ row, commodityMetaList }) => enrichRelatedHeadline(row, feedById, commodityMetaList))
    .sort(sortNewest);
}

function renderHeadlineItem(item) {
  const metaParts = [escapeHtml(item.source)];
  if (item.metaCategoryLabel) {
    metaParts.push(escapeHtml(item.metaCategoryLabel));
  }
  metaParts.push(renderHeadlineTimeMeta(item.published));

  const sentimentMeta = renderHeadlineSentimentMeta(item.sentiment);
  if (sentimentMeta) {
    metaParts.push(sentimentMeta);
  }

  const titleMarkup = item.url
    ? `
        <a class="headline-link" href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer noopener">
          ${escapeHtml(item.title)}
        </a>
      `
    : `<span class="headline-link">${escapeHtml(item.title)}</span>`;

  return `
    <article class="headline-item">
      <div class="feed-cat">
        <span class="feed-cat-dot ${escapeHtml(item.dotClass)}"></span>
        <span class="feed-cat-label">${escapeHtml(item.dotLabel)}</span>
      </div>
      <h3 class="feed-headline">
        ${titleMarkup}
      </h3>
      <div class="feed-meta">${metaParts.join('<span class="sep">·</span>')}</div>
    </article>
  `;
}

function isUndatedEvent(event) {
  return event.event_date.includes("T00:00:00") && /does not specify|no publication hour|does not specify a publication hour/i.test(event.notes || "");
}

function formatCalendarTime(event) {
  if (isUndatedEvent(event)) {
    return "Time TBC";
  }

  return `${calendarTimeFormatter.format(new Date(event.event_date))} UTC`;
}

function formatCalendarDay(isoDate) {
  return calendarDayFormatter.format(new Date(`${isoDate}T00:00:00Z`));
}

function renderCalendarItem(event) {
  const primarySector = event.commodity_sectors.find((sectorId) => SECTOR_META[sectorId]) || "cross-commodity";
  return `
    <article class="calendar-item">
      <div class="calendar-item-marker" style="--marker-accent:${SECTOR_META[primarySector]?.accent || "var(--color-cross)"};"></div>
      <div class="calendar-item-copy">
        <h3 class="calendar-item-title">${escapeHtml(event.name)}</h3>
        <p class="calendar-item-organiser">${escapeHtml(event.organiser)}</p>
        <div class="calendar-item-meta">
          <span>${escapeHtml(formatCalendarTime(event))}</span>
        </div>
      </div>
      <a class="calendar-item-link" href="${escapeHtml(event.calendar_url)}" target="_blank" rel="noreferrer noopener" aria-label="Open source for ${escapeHtml(event.name)}">
        ${renderLaunchIcon()}
      </a>
    </article>
  `;
}

function groupEventsByDay(events) {
  const grouped = new Map();
  events.forEach((event) => {
    const day = event.event_date.slice(0, 10);
    const bucket = grouped.get(day) || [];
    bucket.push(event);
    grouped.set(day, bucket);
  });
  return [...grouped.entries()].map(([day, items]) => ({ day, items: items.sort(sortSoonest) }));
}

function renderCalendarGroups(events) {
  const today = toIsoDate(new Date());
  return groupEventsByDay(events)
    .map(
      ({ day, items }) => `
        <section class="calendar-day-group">
          <div class="calendar-day-divider ${day === today ? "is-today" : ""}">
            <span>${escapeHtml(formatCalendarDay(day))}</span>
            ${day === today ? '<span class="calendar-day-badge">Today</span>' : ""}
          </div>
          <div class="calendar-day-items">
            ${items.map(renderCalendarItem).join("")}
          </div>
        </section>
      `
    )
    .join("");
}

function matchesCalendarPattern(event, pattern) {
  if (!pattern) {
    return true;
  }

  return pattern.test(`${event.name || ""} ${event.notes || ""} ${event.organiser || ""}`);
}

function getCalendarSelection(filter, events) {
  const normalized = normalizeFilter(filter);
  if (isAllFilter(normalized)) {
    return {
      items: events.sort(sortSoonest).slice(0, 10),
      directMatchCount: events.length,
    };
  }

  const selectedSectorIds = getSelectedSectors(normalized).map((sector) => sector.id);
  const sectorEvents = events.filter((event) =>
    event.commodity_sectors.some((sectorId) => selectedSectorIds.includes(sectorId))
  );
  const alwaysEvents = events.filter((event) =>
    event.commodity_sectors.some((sectorId) => ALWAYS_CALENDAR_SECTORS.includes(sectorId))
  );

  let directMatches = sectorEvents;
  const selectedGroup = getSelectedGroup(normalized);
  const selectedGroups = getSelectedGroups(normalized);

  if (hasPartialGroupSelection(normalized) && selectedGroups.length) {
    const selectedCommodities = getSelectedCommodities(normalized);
    const commodityPattern =
      selectedGroup && selectedCommodities.length === 1
        ? getCommodityCalendarPattern(selectedCommodities[0].id)
        : null;
    const groupPatterns = selectedGroups.map((group) => getGroupCalendarPattern(group.id)).filter(Boolean);

    directMatches = sectorEvents.filter((event) => {
      if (commodityPattern) {
        return matchesCalendarPattern(event, commodityPattern);
      }

      if (!groupPatterns.length) {
        return false;
      }

      return groupPatterns.some((pattern) => matchesCalendarPattern(event, pattern));
    });
  }

  const items = dedupeByKey([...directMatches, ...alwaysEvents].sort(sortSoonest), (event) => event.id).slice(0, 10);
  return {
    items,
    directMatchCount: directMatches.length,
  };
}

async function renderPriceModuleBody(body, filter, { onNavigate, commit, isCurrent } = {}) {
  const writeBody = typeof commit === "function" ? commit : (target, html) => {
    target.innerHTML = html;
  };
  const isRenderCurrent = typeof isCurrent === "function" ? isCurrent : () => true;

  if (typeof body.__priceTileCleanup === "function") {
    body.__priceTileCleanup();
    body.__priceTileCleanup = null;
  }

  if (typeof body.__priceStripCleanup === "function") {
    body.__priceStripCleanup();
    body.__priceStripCleanup = null;
  }

  const selection = getPriceSelection(filter);

  if (!selection.length) {
    writeBody(
      body,
      renderEmptyState("No benchmark prices available", `No live benchmark prices are configured for ${getFilterLabel(filter)}.`)
    );
    return;
  }

  const latestBySeriesKey = await fetchLatestSeriesMap();
  if (!isRenderCurrent()) {
    return;
  }
  const priceEntries = await Promise.all(
    selection.map(async (commodityMeta) => {
      const latestRow = latestBySeriesKey.get(commodityMeta.seriesKey);
      if (!latestRow) {
        return null;
      }

      const historyRows = await fetchSeriesHistory(commodityMeta.seriesKey);
      return {
        commodityMeta,
        latestRow,
        historyRows,
      };
    })
  );
  if (!isRenderCurrent()) {
    return;
  }

  const availableEntries = priceEntries.filter(Boolean);
  if (!availableEntries.length) {
    writeBody(
      body,
      renderEmptyState(
        "No benchmark prices available",
        `No live benchmark prices are currently available for ${getFilterLabel(filter)}.`
      )
    );
    return;
  }

  writeBody(body, `
    ${isAllFilter(filter) ? "" : `<p class="module-scope">Showing prices for ${escapeHtml(getFilterLabel(filter))}</p>`}
    <div class="price-strip" data-price-strip>
      <div class="price-strip-track" data-price-track>
        ${availableEntries
        .map(({ commodityMeta, latestRow, historyRows }) => {
          const change = formatChange(latestRow.delta_value || 0, latestRow.delta_pct || 0, latestRow.unit);
          const direction = (latestRow.delta_value || 0) >= 0 ? "up" : "down";
          const group = getGroupById(commodityMeta.groupId);
          const route = `/price-watch/?series=${encodeURIComponent(commodityMeta.seriesKey)}`;

          return `
            <button
              class="price-tile is-${escapeHtml(direction)}"
              data-sector="${escapeHtml(commodityMeta.sectorId)}"
              data-price-route="${escapeHtml(route)}"
              type="button"
              aria-label="Open ${escapeHtml(commodityMeta.label)} in PriceWatch"
            >
              <div class="price-tile-head">
                <p class="price-tile-label">${escapeHtml(group?.label || SECTOR_META[commodityMeta.sectorId]?.label || "Commodity")}</p>
                <h3 class="price-tile-title">${escapeHtml(commodityMeta.label)}</h3>
                <p class="price-as-of">As of ${escapeHtml(formatObservationDate(latestRow.observation_date))}</p>
              </div>
              <div class="price-tile-spacer" aria-hidden="true"></div>
              <div class="price-tile-body">
                <p class="price-value">${escapeHtml(formatValue(latestRow.value, latestRow.unit))}</p>
                <div class="price-move">
                  <span class="price-change">${escapeHtml(change.absolute)}</span>
                  <span class="price-change-pct">${escapeHtml(change.percent)}</span>
                </div>
                <div class="price-spark-wrap">
                  ${renderSparkline(buildSparklineValues(historyRows, latestRow), direction)}
                </div>
                <p class="price-source">${escapeHtml(latestRow.source_name)}</p>
              </div>
            </button>
          `;
        })
        .join("")}
      </div>
    </div>
  `);

  if (!isRenderCurrent()) {
    return;
  }

  bindPriceTileInteractions(body, onNavigate);
  bindPriceStripCarousel(body);
}

async function renderHeadlineModuleBody(body, filter, { commit, isCurrent } = {}) {
  const writeBody = typeof commit === "function" ? commit : (target, html) => {
    target.innerHTML = html;
  };
  const isRenderCurrent = typeof isCurrent === "function" ? isCurrent : () => true;

  const feed = await fetchHeadlineFeed();
  if (!isRenderCurrent()) {
    return;
  }
  const normalized = normalizeFilter(filter);
  const alwaysRelevant = getAlwaysRelevantHeadlines(feed.articles);
  const selectedSectorIds = getSelectedSectors(normalized).map((sector) => sector.id);

  if (isAllFilter(normalized)) {
    const items = feed.articles.slice(0, 10).map((article) => createHeadlineItem(article));

    writeBody(body, `<div class="headline-list">${items.map(renderHeadlineItem).join("")}</div>`);
    return;
  }

  const selectedGroups = getSelectedGroups(normalized);
  const selectedGroup = getSelectedGroup(normalized);
  const sectorScopeOnly = !hasPartialGroupSelection(normalized) && !selectedGroup;
  let directItems = [];
  if (selectedGroup) {
    const selectedCommodities = getSelectedCommodities(normalized).filter((commodity) => commodity.seriesKey);

    if (selectedCommodities.length) {
      const relatedRows = await Promise.all(
        selectedCommodities.map(async (commodityMeta) => ({
          commodityMeta,
          rows: await fetchRelatedHeadlines(commodityMeta.seriesKey, 6),
        }))
      );
      if (!isRenderCurrent()) {
        return;
      }
      directItems = mergeRelatedHeadlineSets(relatedRows, feed.byId);
    }
  }

  if (!directItems.length) {
    if (sectorScopeOnly) {
      directItems = feed.articles
        .filter((article) => selectedSectorIds.some((sectorId) => matchesSectorFeed(article, sectorId)))
        .slice(0, 10)
        .map((article) => createHeadlineItem(article));
    } else {
      directItems = feed.articles
        .filter((article) => selectedGroups.some((group) => matchesGroupFeed(article, group.id)))
        .slice(0, 10)
        .map((article) => createHeadlineItem(article));
    }
  }

  if (!directItems.length && selectedSectorIds.length) {
    directItems = feed.articles
      .filter((article) => selectedSectorIds.some((sectorId) => matchesSectorFeed(article, sectorId)))
      .slice(0, 10)
      .map((article) => createHeadlineItem(article));
  }

  const broaderContextItems = alwaysRelevant.slice(0, 10).map((article) => createHeadlineItem(article));

  const items = dedupeByKey([...directItems, ...broaderContextItems].sort(sortNewest), (item) => item.id).slice(0, 10);

  if (!items.length) {
    writeBody(
      body,
      renderEmptyState("No headlines found", `No headlines found for ${getFilterLabel(filter)} in the current feed window.`)
    );
    return;
  }

  writeBody(body, `
    <p class="module-scope">Showing headlines for: <strong>${escapeHtml(getFilterLabel(filter))}</strong></p>
    ${
      directItems.length
        ? ""
        : `<p class="module-note">No direct commodity matches were found. Showing broader commodity context instead.</p>`
    }
    <div class="headline-list">${items.map(renderHeadlineItem).join("")}</div>
  `);
}

async function renderCalendarModuleBody(body, filter, { commit, isCurrent } = {}) {
  const writeBody = typeof commit === "function" ? commit : (target, html) => {
    target.innerHTML = html;
  };
  const isRenderCurrent = typeof isCurrent === "function" ? isCurrent : () => true;

  const normalized = normalizeFilter(filter);
  const today = new Date();
  const from = toIsoDate(today);
  const to = toIsoDate(addUtcDays(today, 14));
  const sectors = isAllFilter(normalized)
    ? []
    : [...new Set([...getSelectedSectors(normalized).map((sector) => sector.id), ...ALWAYS_CALENDAR_SECTORS])];
  const events = await fetchCalendarEvents({ from, to, sectors });
  if (!isRenderCurrent()) {
    return;
  }
  const { items, directMatchCount } = getCalendarSelection(normalized, events);

  if (!items.length) {
    writeBody(
      body,
      renderEmptyState("No releases scheduled", `No upcoming releases were found for ${getFilterLabel(filter)} in the next two weeks.`)
    );
    return;
  }

  writeBody(body, `
    ${
      !isAllFilter(normalized) && directMatchCount === 0
        ? `<p class="module-note">No direct ${escapeHtml(getFilterLabel(filter))} releases are scheduled. Showing macro and cross-commodity events.</p>`
        : ""
    }
    <div class="calendar-agenda">${renderCalendarGroups(items)}</div>
  `);
}

async function renderInventoryModuleBody(body, filter, { commit, isCurrent } = {}) {
  const writeBody = typeof commit === "function" ? commit : (target, html) => {
    target.innerHTML = html;
  };
  const isRenderCurrent = typeof isCurrent === "function" ? isCurrent : () => true;

  const snapshot = await fetchInventorySnapshot({ includeSparklines: true, limit: 100 });
  if (!isRenderCurrent()) {
    return;
  }

  const { items, scopeLabel, narrowedByGroup } = getInventorySelection(filter, snapshot.cards);
  const visibleItems = items.slice(0, 6);

  if (!items.length) {
    writeBody(
      body,
      renderEmptyState(
        "Inventory coverage unavailable",
        narrowedByGroup
          ? `InventoryWatch does not currently map ${scopeLabel} to a live inventory series.`
          : `No live inventory indicators are currently available for ${scopeLabel}.`
      )
    );
    return;
  }

  writeBody(
    body,
    `
      ${
        isAllFilter(filter)
          ? ""
          : `<p class="module-scope">Showing inventory for: <strong>${escapeHtml(scopeLabel)}</strong></p>`
      }
      <div class="inventory-preview-grid">
        ${visibleItems.map((card) => renderInventoryPreviewCard(card)).join("")}
      </div>
      ${
        items.length > visibleItems.length
          ? `<p class="module-note">Showing ${visibleItems.length} of ${items.length} matching indicators. Open InventoryWatch for the full grid.</p>`
          : ""
      }
    `
  );
}

function attachAsyncRenderer(section, body, renderer, filter, context = {}) {
  const gate = createModuleRenderGate(section);

  Promise.resolve(renderer(body, filter, { ...context, ...gate }))
    .catch(() => {
      if (!gate.isCurrent()) {
        return;
      }
      body.innerHTML = renderErrorState("Live data could not be loaded for this module.");
    });
}

export function PriceModule({ filter, onNavigate }) {
  const primarySectorId = getSelectedSectors(filter)[0]?.id || "energy";
  const { section, body } = createModuleCard({
    id: "prices",
    title: "Benchmark Prices",
    linkLabel: "View PriceWatch",
    route: "/price-watch/",
    accentSectorId: primarySectorId,
    onNavigate,
  });

  attachAsyncRenderer(section, body, renderPriceModuleBody, filter, { onNavigate });
  return section;
}

export function HeadlineModule({ filter, onNavigate }) {
  const primarySectorId = getSelectedSectors(filter)[0]?.id || "cross-commodity";
  const { section, body } = createModuleCard({
    id: "headlines",
    title: "Latest Headlines",
    linkLabel: "View HeadlineWatch",
    route: "/headline-watch/",
    accentSectorId: primarySectorId,
    onNavigate,
  });

  attachAsyncRenderer(section, body, renderHeadlineModuleBody, filter);
  return section;
}

export function CalendarModule({ filter, onNavigate }) {
  const primarySectorId = getSelectedSectors(filter)[0]?.id || "macro";
  const { section, body } = createModuleCard({
    id: "calendar",
    title: "Upcoming Releases",
    linkLabel: "View CalendarWatch",
    route: "/calendar-watch/",
    accentSectorId: primarySectorId,
    onNavigate,
  });

  attachAsyncRenderer(section, body, renderCalendarModuleBody, filter);
  return section;
}

export function InventoryModule({ filter, onNavigate }) {
  const primarySectorId = getSelectedSectors(filter)[0]?.id || "energy";
  const { section, body } = createModuleCard({
    id: "inventory",
    title: "Inventory Snapshot",
    linkLabel: "View InventoryWatch",
    route: buildInventorySnapshotHref(),
    accentSectorId: primarySectorId,
    onNavigate,
  });

  attachAsyncRenderer(section, body, renderInventoryModuleBody, filter);
  return section;
}
