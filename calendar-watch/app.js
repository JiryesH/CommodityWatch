import { loadCalendarEvents } from "./calendar-data.js";
import {
  CADENCE_META,
  CADENCE_OPTIONS,
  SECTOR_META,
  SECTOR_OPTIONS,
  addUtcDays,
  addUtcMonths,
  areAllFiltersSelected,
  buildMonthGrid,
  buildRangeForView,
  buildWeekDays,
  clampAnchorDateForView,
  cloneDefaultFilters,
  countActiveFilters,
  endOfUtcYear,
  filterEvents,
  filterEventsByRange,
  getEventsForDay,
  getMaximumAnchorDate,
  getMinimumAnchorDate,
  groupEventsByDay,
  isSameUtcDay,
  isSameUtcMonth,
  searchEvents,
  sortEvents,
  startOfUtcDay,
  toIsoDay,
  toggleFilterSelection,
} from "./calendar-utils.js";

const TO_TOP_SCROLL_THRESHOLD = 360;
const UPCOMING_HORIZON_DAYS = 365;
const MAX_VISIBLE_DATE = endOfUtcYear(new Date());

const weekdayFormatter = new Intl.DateTimeFormat("en-GB", {
  weekday: "short",
  timeZone: "UTC",
});

const dayFormatter = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "short",
  timeZone: "UTC",
});

const fullDayFormatter = new Intl.DateTimeFormat("en-GB", {
  weekday: "long",
  day: "numeric",
  month: "long",
  year: "numeric",
  timeZone: "UTC",
});

const monthFormatter = new Intl.DateTimeFormat("en-GB", {
  month: "long",
  year: "numeric",
  timeZone: "UTC",
});

const monthDayFormatter = new Intl.DateTimeFormat("en-GB", {
  month: "short",
  day: "numeric",
  timeZone: "UTC",
});

const shortDateFormatter = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "short",
  year: "numeric",
  timeZone: "UTC",
});

const timeFormatter = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const ALL_SECTOR_IDS = SECTOR_OPTIONS.map((option) => option.id);
const ALL_CADENCE_IDS = CADENCE_OPTIONS.map((option) => option.id);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function parseUtcDate(value) {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date : null;
}

function formatUtcDateTime(value) {
  const date = parseUtcDate(value);
  if (!date) {
    return "Date unavailable";
  }

  return `${fullDayFormatter.format(date)}, ${timeFormatter.format(date)} UTC`;
}

function formatUtcTime(value) {
  const date = parseUtcDate(value);
  if (!date) {
    return "Time unavailable";
  }

  return `${timeFormatter.format(date)} UTC`;
}

function formatUtcDay(value) {
  const date = parseUtcDate(value);
  if (!date) {
    return "Unknown day";
  }

  return fullDayFormatter.format(date);
}

function getDateTimeAttribute(value) {
  const date = parseUtcDate(value);
  return date ? date.toISOString() : null;
}

function getEventDayIso(value) {
  const date = parseUtcDate(value);
  return date ? toIsoDay(date) : null;
}

function formatWeekRangeLabel(anchorDate) {
  const weekDays = buildWeekDays(anchorDate);
  const first = weekDays[0];
  const last = weekDays[weekDays.length - 1];
  const sameMonth =
    first.getUTCFullYear() === last.getUTCFullYear() && first.getUTCMonth() === last.getUTCMonth();

  if (sameMonth) {
    return `${weekdayFormatter.format(first)} ${first.getUTCDate()}-${last.getUTCDate()} ${monthFormatter.format(first)}`;
  }

  return `${monthDayFormatter.format(first)} - ${monthDayFormatter.format(last)}`;
}

function formatCountLabel(count, singular, plural = `${singular}s`) {
  return `${count} ${count === 1 ? singular : plural}`;
}

function formatCalendarLimitDate(value) {
  return shortDateFormatter.format(value);
}

function getConfirmedLabel(event) {
  return event.is_confirmed ? "Confirmed" : "Provisional";
}

function getSourceHostname(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
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

function iconLink() {
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M14 4h6v6"></path>
      <path d="M10 14 20 4"></path>
      <path d="M20 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h5"></path>
    </svg>
  `;
}

function iconLock(confirmed) {
  if (confirmed) {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <rect x="5" y="11" width="14" height="10" rx="2"></rect>
        <path d="M8 11V8a4 4 0 1 1 8 0v3"></path>
      </svg>
    `;
  }

  return `
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="5" y="11" width="14" height="10" rx="2"></rect>
      <path d="M16 11V8a4 4 0 0 0-7.4-2"></path>
      <path d="M4 4l16 16"></path>
    </svg>
  `;
}

function renderSectorTag(sector) {
  const meta = SECTOR_META[sector];
  if (!meta) {
    return `<span class="sector-tag" style="--sector-accent:#7B8894; --sector-bg:#E8EDF1; --sector-text:#5A6671;">${escapeHtml(
      sector ?? "Unknown"
    )}</span>`;
  }

  return `<span class="sector-tag" style="--sector-accent:${meta.accent}; --sector-bg:${meta.pillBackground}; --sector-text:${meta.pillText};">${escapeHtml(meta.label)}</span>`;
}

function renderCadenceBadge(cadence, { subtle = false } = {}) {
  const meta = CADENCE_META[cadence];
  const label = meta ? meta.label : String(cadence ?? "Unknown");
  return `<span class="cadence-badge${subtle ? " is-subtle" : ""}">${escapeHtml(label)}</span>`;
}

function renderSectorFilterPills(selectedValues) {
  const allSelected = areAllFiltersSelected(selectedValues, ALL_SECTOR_IDS);
  const selected = new Set(selectedValues);

  return `
    <button
      class="filter-pill calendar-sector-reset${allSelected ? " is-selected" : ""}"
      id="sector-reset"
      type="button"
      data-filter-sector-reset
      aria-pressed="${String(allSelected)}"
    >
      All
    </button>
    <div class="filter-divider" aria-hidden="true"></div>
    <div class="sector-pill-row">
      ${SECTOR_OPTIONS.map((option) => {
        const meta = SECTOR_META[option.id];
        const isSelected = selected.has(option.id);
        return `
          <button
            class="filter-pill calendar-sector-pill${isSelected ? " is-selected" : ""}"
            type="button"
            data-filter-sector="${option.id}"
            aria-pressed="${String(isSelected)}"
            style="--filter-pill-color:${meta.accent};"
          >
            ${escapeHtml(option.label)}
          </button>
        `;
      }).join("")}
    </div>
  `;
}

function renderEventCard(event, { inDrawer = false } = {}) {
  const sectors = Array.isArray(event.commodity_sectors) ? event.commodity_sectors : [];
  const dateTimeAttribute = getDateTimeAttribute(event.event_date);

  return `
    <article class="event-card${inDrawer ? " event-card-drawer" : ""}">
      <button class="event-card-main" type="button" data-event-open="${event.id}">
        <time class="event-time"${dateTimeAttribute ? ` datetime="${escapeHtml(dateTimeAttribute)}"` : ""}>${escapeHtml(
          formatUtcTime(event.event_date)
        )}</time>
        <h3 class="event-name">${escapeHtml(event.name)}</h3>
        <p class="event-organiser">${escapeHtml(event.organiser)}</p>
        <div class="event-tags">${sectors.map(renderSectorTag).join("")}</div>
      </button>
      <span class="event-status${event.is_confirmed ? " is-confirmed" : " is-provisional"}" title="${escapeHtml(getConfirmedLabel(event))}">
        ${iconLock(event.is_confirmed)}
        <span class="sr-only">${escapeHtml(getConfirmedLabel(event))}</span>
      </span>
      <a
        class="event-link"
        href="${escapeHtml(event.calendar_url)}"
        target="_blank"
        rel="noreferrer noopener"
        aria-label="Open source for ${escapeHtml(event.name)}"
        title="Open source"
      >
        ${iconLink()}
      </a>
    </article>
  `;
}

function renderDayListCard(event) {
  const sectors = Array.isArray(event.commodity_sectors) ? event.commodity_sectors : [];
  const dateTimeAttribute = getDateTimeAttribute(event.event_date);

  return `
    <article class="drawer-event-card">
      <button class="drawer-event-main" type="button" data-event-open="${event.id}">
        <div class="drawer-event-meta">
          <time${dateTimeAttribute ? ` datetime="${escapeHtml(dateTimeAttribute)}"` : ""}>${escapeHtml(
            formatUtcTime(event.event_date)
          )}</time>
          <span class="drawer-status-label">${escapeHtml(getConfirmedLabel(event))}</span>
        </div>
        <h3>${escapeHtml(event.name)}</h3>
        <p>${escapeHtml(event.organiser)}</p>
        <div class="event-tags">${sectors.map(renderSectorTag).join("")}</div>
      </button>
      <a
        class="drawer-event-link"
        href="${escapeHtml(event.calendar_url)}"
        target="_blank"
        rel="noreferrer noopener"
        aria-label="Open source for ${escapeHtml(event.name)}"
      >
        ${iconLink()}
      </a>
    </article>
  `;
}

function renderLoadingState(viewMode) {
  const title = viewMode === "month" ? "Loading month schedule..." : "Loading week schedule...";
  return `
    <section class="calendar-panel calendar-loading">
      <div class="calendar-loading-copy">${escapeHtml(title)}</div>
    </section>
  `;
}

function renderErrorState(message) {
  return `
    <section class="calendar-panel">
      <div class="calendar-empty">
        <h3>Calendar data is unavailable</h3>
        <p>${escapeHtml(message)}</p>
      </div>
    </section>
  `;
}

export class CalendarWatchApp {
  constructor({
    root,
    filterRoot,
    drawerOverlay,
    drawerPanel,
    toTopButton,
    navSearch,
    searchToggle,
    searchInput,
    loadEvents = loadCalendarEvents,
  }) {
    this.root = root;
    this.filterRoot = filterRoot;
    this.drawerOverlay = drawerOverlay;
    this.drawerPanel = drawerPanel;
    this.toTopButton = toTopButton;
    this.navSearch = navSearch;
    this.searchToggle = searchToggle;
    this.searchInput = searchInput;
    this.loadEvents = loadEvents;
    this.activeRefreshRequestId = 0;
    this.state = {
      viewMode: "week",
      anchorDate: getMinimumAnchorDate("week"),
      filters: cloneDefaultFilters(),
      searchQuery: "",
      searchOpen: false,
      cadenceLayerOpen: false,
      events: [],
      visibleEvents: [],
      loading: true,
      error: null,
      panelMode: null,
      panelDayIso: null,
      selectedEventId: null,
      panelOrigin: null,
    };
  }

  async init() {
    this.bindEvents();
    await this.refreshData();
  }

  bindEvents() {
    this.root.addEventListener("click", (event) => this.handleInteractionClick(event));
    this.filterRoot.addEventListener("click", (event) => this.handleInteractionClick(event));

    this.drawerOverlay.addEventListener("click", (event) => {
      if (event.target === this.drawerOverlay || event.target.closest("[data-drawer-close]")) {
        this.closePanel();
      }
    });

    this.drawerPanel.addEventListener("click", (event) => {
      const eventButton = event.target.closest("[data-event-open]");
      if (eventButton) {
        this.openEvent(eventButton.dataset.eventOpen, "day");
        return;
      }

      const backButton = event.target.closest("[data-drawer-back]");
      if (backButton) {
        this.openDayPanel(this.state.panelDayIso);
      }
    });

    this.searchToggle.addEventListener("click", (event) => {
      event.stopPropagation();
      this.state.searchOpen = !this.state.searchOpen;
      this.renderSearchUi();
      if (this.state.searchOpen) {
        window.requestAnimationFrame(() => this.searchInput.focus());
      }
    });

    this.searchInput.addEventListener("input", () => {
      this.state.searchQuery = this.searchInput.value;
      this.applyDerivedState();
      this.renderSearchUi();
    });

    document.addEventListener("click", (event) => {
      if (this.state.searchOpen && !eventPathIncludes(event, this.navSearch)) {
        this.state.searchOpen = false;
        this.renderSearchUi();
      }

      if (this.state.cadenceLayerOpen && !eventPathIncludes(event, this.filterRoot)) {
        this.state.cadenceLayerOpen = false;
        this.render();
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") {
        return;
      }

      if (this.state.panelMode) {
        this.closePanel();
        return;
      }

      if (this.state.searchOpen) {
        this.state.searchOpen = false;
        this.renderSearchUi();
        return;
      }

      if (this.state.cadenceLayerOpen) {
        this.state.cadenceLayerOpen = false;
        this.render();
      }
    });

    window.addEventListener("scroll", () => {
      this.toTopButton.classList.toggle("visible", window.scrollY > TO_TOP_SCROLL_THRESHOLD);
    });

    this.toTopButton.addEventListener("click", () => {
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  }

  hasActiveSearch() {
    return this.getSearchQuery().length > 0;
  }

  getSearchQuery() {
    return this.state.searchQuery.trim();
  }

  getCurrentRange() {
    const unclampedRange = buildRangeForView(this.state.viewMode, this.state.anchorDate);

    return {
      from: unclampedRange.from,
      to: new Date(Math.min(Date.parse(unclampedRange.to), MAX_VISIBLE_DATE.getTime())).toISOString(),
    };
  }

  getCurrentRangeEvents() {
    return filterEventsByRange(this.state.visibleEvents, this.getCurrentRange());
  }

  getMaximumAnchor(viewMode = this.state.viewMode) {
    return getMaximumAnchorDate(viewMode, MAX_VISIBLE_DATE);
  }

  getDataRequestRange() {
    const minimumMonthRange = buildRangeForView("month", getMinimumAnchorDate("month"));
    const currentRange = this.getCurrentRange();
    const horizonDate = addUtcDays(new Date(), UPCOMING_HORIZON_DAYS);
    const horizonEnd = new Date(`${toIsoDay(horizonDate)}T23:59:59Z`).toISOString();
    const requestEnd = new Date(
      Math.min(MAX_VISIBLE_DATE.getTime(), Math.max(Date.parse(currentRange.to), Date.parse(horizonEnd)))
    ).toISOString();

    return {
      from: minimumMonthRange.from,
      to: requestEnd,
    };
  }

  applyDerivedState() {
    const filtered = filterEvents(this.state.events, this.state.filters);
    const searched = searchEvents(filtered, this.state.searchQuery);

    this.state.visibleEvents = this.hasActiveSearch()
      ? searched.filter((event) => Date.parse(event.event_date) >= Date.now())
      : searched;

    this.synchronizePanelState();
    this.render();
  }

  async refreshData() {
    const requestId = ++this.activeRefreshRequestId;
    this.state.loading = true;
    this.state.error = null;
    this.render();

    try {
      const events = sortEvents(await this.loadEvents(this.getDataRequestRange())).filter(
        (event) => Date.parse(event.event_date) <= MAX_VISIBLE_DATE.getTime()
      );

      if (requestId !== this.activeRefreshRequestId) {
        return;
      }

      this.state.events = events;
      this.state.loading = false;
      this.state.error = null;
      this.applyDerivedState();
    } catch (error) {
      if (requestId !== this.activeRefreshRequestId) {
        return;
      }

      this.state.events = [];
      this.state.visibleEvents = [];
      this.state.loading = false;
      this.state.error = error instanceof Error ? error.message : "Unknown calendar API error";
      this.closePanel({ render: false });
      this.render();
    }
  }

  synchronizePanelState() {
    if (!this.state.panelMode) {
      return;
    }

    if (this.hasActiveSearch() && this.state.panelMode === "day") {
      this.closePanel({ render: false });
      return;
    }

    if (this.state.panelMode === "event") {
      const selectedEvent = this.state.visibleEvents.find((event) => event.id === this.state.selectedEventId);
      if (!selectedEvent) {
        this.closePanel({ render: false });
      }
      return;
    }

    if (this.state.panelMode === "day" && this.state.panelDayIso) {
      const grouped = groupEventsByDay(this.getCurrentRangeEvents());
      const dayEvents = grouped.get(this.state.panelDayIso) || [];
      if (!dayEvents.length) {
        this.closePanel({ render: false });
      }
    }
  }

  handleInteractionClick(event) {
    const viewButton = event.target.closest("[data-view-mode]");
    if (viewButton) {
      const nextViewMode = viewButton.dataset.viewMode;
      if (nextViewMode !== this.state.viewMode) {
        this.state.viewMode = nextViewMode;
        this.state.anchorDate = clampAnchorDateForView(nextViewMode, this.state.anchorDate, new Date(), MAX_VISIBLE_DATE);
        this.refreshData();
      }
      return;
    }

    const navigationButton = event.target.closest("[data-nav-direction]");
    if (navigationButton) {
      this.navigate(navigationButton.dataset.navDirection);
      return;
    }

    if (event.target.closest("[data-nav-reset]")) {
      this.resetAnchor();
      return;
    }

    if (event.target.closest("[data-filter-sector-reset]")) {
      this.state.filters.sectors = [...ALL_SECTOR_IDS];
      this.applyDerivedState();
      return;
    }

    const sectorButton = event.target.closest("[data-filter-sector]");
    if (sectorButton) {
      this.state.filters.sectors = toggleFilterSelection(
        this.state.filters.sectors,
        sectorButton.dataset.filterSector,
        ALL_SECTOR_IDS
      );
      this.applyDerivedState();
      return;
    }

    if (event.target.closest("[data-cadence-toggle]")) {
      this.state.cadenceLayerOpen = !this.state.cadenceLayerOpen;
      this.render();
      return;
    }

    if (event.target.closest("[data-filter-cadence-reset]")) {
      this.state.filters.cadences = [...ALL_CADENCE_IDS];
      this.state.cadenceLayerOpen = false;
      this.applyDerivedState();
      return;
    }

    const cadenceButton = event.target.closest("[data-filter-cadence]");
    if (cadenceButton) {
      this.state.filters.cadences = toggleFilterSelection(
        this.state.filters.cadences,
        cadenceButton.dataset.filterCadence,
        ALL_CADENCE_IDS
      );
      this.state.cadenceLayerOpen = true;
      this.applyDerivedState();
      return;
    }

    if (event.target.closest("[data-clear-filters]")) {
      this.state.filters = cloneDefaultFilters();
      this.state.cadenceLayerOpen = false;
      this.applyDerivedState();
      return;
    }

    if (event.target.closest("[data-clear-search]")) {
      this.state.searchQuery = "";
      this.searchInput.value = "";
      this.state.cadenceLayerOpen = false;
      this.applyDerivedState();
      this.renderSearchUi();
      return;
    }

    const dayButton = event.target.closest("[data-day-open]");
    if (dayButton && dayButton.dataset.dayOpen) {
      this.openDayPanel(dayButton.dataset.dayOpen);
      return;
    }

    const eventButton = event.target.closest("[data-event-open]");
    if (eventButton) {
      this.openEvent(eventButton.dataset.eventOpen, "calendar");
    }
  }

  navigate(direction) {
    const candidateAnchor =
      this.state.viewMode === "month"
        ? addUtcMonths(this.state.anchorDate, direction === "next" ? 1 : -1)
        : addUtcDays(this.state.anchorDate, direction === "next" ? 7 : -7);
    const nextAnchor = clampAnchorDateForView(this.state.viewMode, candidateAnchor, new Date(), MAX_VISIBLE_DATE);

    if (nextAnchor.getTime() === this.state.anchorDate.getTime()) {
      return;
    }

    this.state.anchorDate = nextAnchor;
    this.refreshData();
  }

  resetAnchor() {
    const nextAnchor = getMinimumAnchorDate(this.state.viewMode);
    if (nextAnchor.getTime() === this.state.anchorDate.getTime()) {
      return;
    }

    this.state.anchorDate = nextAnchor;
    this.refreshData();
  }

  openDayPanel(dayIso) {
    const grouped = groupEventsByDay(this.getCurrentRangeEvents());
    const dayEvents = grouped.get(dayIso) || [];

    if (!dayEvents.length) {
      return;
    }

    this.state.panelMode = "day";
    this.state.panelDayIso = dayIso;
    this.state.selectedEventId = null;
    this.state.panelOrigin = null;
    this.renderDrawer();
  }

  openEvent(eventId, origin) {
    const selectedEvent = this.state.visibleEvents.find((event) => event.id === eventId);

    if (!selectedEvent) {
      return;
    }

    this.state.panelMode = "event";
    this.state.selectedEventId = eventId;
    this.state.panelDayIso = origin === "day" && this.state.panelDayIso ? this.state.panelDayIso : getEventDayIso(selectedEvent.event_date);
    this.state.panelOrigin = origin;
    this.renderDrawer();
  }

  closePanel({ render = true } = {}) {
    this.state.panelMode = null;
    this.state.selectedEventId = null;
    this.state.panelDayIso = null;
    this.state.panelOrigin = null;

    if (render) {
      this.renderDrawer();
    }
  }

  render() {
    this.renderFilterBar();

    this.root.innerHTML = `
      <section class="calendar-page">
        <header class="calendar-header">
          <div class="calendar-header-top">
            <div class="page-copy">
              <h1 class="page-title">CalendarWatch</h1>
            </div>
            <div class="header-actions">
              <div class="view-toggle" role="tablist" aria-label="Calendar view mode">
                <button
                  class="view-toggle-button${this.state.viewMode === "week" ? " is-selected" : ""}"
                  type="button"
                  data-view-mode="week"
                  role="tab"
                  aria-selected="${String(this.state.viewMode === "week")}"
                >
                  Week view
                </button>
                <button
                  class="view-toggle-button${this.state.viewMode === "month" ? " is-selected" : ""}"
                  type="button"
                  data-view-mode="month"
                  role="tab"
                  aria-selected="${String(this.state.viewMode === "month")}"
                >
                  Month view
                </button>
              </div>
            </div>
          </div>
        </header>

        <section class="calendar-body">
          ${this.renderCalendarPanel()}
        </section>
      </section>
    `;

    this.renderSearchUi();
    this.renderDrawer();
  }

  renderFilterBar() {
    const activeFilterCount = countActiveFilters(this.state.filters);
    const summaryEventCount = this.hasActiveSearch()
      ? this.state.visibleEvents.length
      : this.getCurrentRangeEvents().length;
    const allCadencesSelected = areAllFiltersSelected(this.state.filters.cadences, ALL_CADENCE_IDS);
    const cadenceSelectionCount = this.state.filters.cadences.length;
    const selectedCadences = new Set(this.state.filters.cadences);

    this.filterRoot.innerHTML = `
      <div class="filter-wrap calendar-filter-wrap">
        <div class="filter-bar" role="toolbar" aria-label="Calendar sector filter">
          ${renderSectorFilterPills(this.state.filters.sectors)}
          <div class="filter-divider" aria-hidden="true"></div>
          <button
            class="filter-pill cadence-toggle${this.state.cadenceLayerOpen || !allCadencesSelected ? " is-selected" : ""}"
            type="button"
            data-cadence-toggle
            aria-expanded="${String(this.state.cadenceLayerOpen)}"
            aria-controls="cadence-layer"
          >
            Cadence${allCadencesSelected ? "" : ` <span class="cadence-toggle-count">${cadenceSelectionCount}</span>`}
          </button>
        </div>
        <div
          class="filter-layer calendar-cadence-layer${this.state.cadenceLayerOpen ? " open" : ""}"
          id="cadence-layer"
          aria-hidden="${String(!this.state.cadenceLayerOpen)}"
          ${this.state.cadenceLayerOpen ? "" : "hidden"}
        >
          <div class="filter-chip-row" role="group" aria-label="Cadence filter">
            ${CADENCE_OPTIONS.map((option) => {
              const isSelected = selectedCadences.has(option.id);
              return `
                <button
                  class="filter-chip${isSelected ? " is-selected" : ""}"
                  type="button"
                  data-filter-cadence="${option.id}"
                >
                  ${escapeHtml(option.label)}
                </button>
              `;
            }).join("")}
            <button class="filter-layer-clear" type="button" data-filter-cadence-reset ${
              allCadencesSelected ? "hidden" : ""
            }>clear</button>
          </div>
        </div>
        <div class="calendar-filter-meta">
          <div class="calendar-filter-status">
            <div class="filter-summary">
              <span>
                ${formatCountLabel(summaryEventCount, "event")} ${
                  this.hasActiveSearch() ? "matching search" : "in view"
                }
              </span>
              <span class="filter-summary-divider" aria-hidden="true"></span>
              <span>${formatCountLabel(this.state.filters.sectors.length, "sector")} selected</span>
              <span class="filter-summary-divider" aria-hidden="true"></span>
              <span>${
                allCadencesSelected
                  ? "All cadences"
                  : formatCountLabel(this.state.filters.cadences.length, "cadence")
              }</span>
            </div>

            <div class="calendar-filter-actions">
              ${
                this.hasActiveSearch()
                  ? `
                    <span class="search-status">Search: ${escapeHtml(this.getSearchQuery())}</span>
                    <button class="clear-filters" type="button" data-clear-search>clear search</button>
                  `
                  : ""
              }
              ${
                activeFilterCount
                  ? `
                    <button class="clear-filters" type="button" data-clear-filters>
                      ${activeFilterCount} filter${activeFilterCount === 1 ? "" : "s"} active - clear all
                    </button>
                  `
                  : `<span class="filter-summary-muted">Default coverage: all sectors and all cadences.</span>`
              }
            </div>
          </div>
        </div>
      </div>
    `;
  }

  renderSearchUi() {
    this.navSearch.classList.toggle("open", this.state.searchOpen);
    this.navSearch.classList.toggle("has-query", this.hasActiveSearch());
    this.searchToggle.setAttribute("aria-expanded", String(this.state.searchOpen));
    if (this.searchInput.value !== this.state.searchQuery) {
      this.searchInput.value = this.state.searchQuery;
    }
  }

  renderCalendarPanel() {
    if (this.state.loading) {
      return renderLoadingState(this.state.viewMode);
    }

    if (this.state.error) {
      return renderErrorState(this.state.error);
    }

    if (this.hasActiveSearch()) {
      return this.renderSearchResults();
    }

    return this.state.viewMode === "month" ? this.renderMonthView() : this.renderWeekView();
  }

  renderSearchResults() {
    const results = this.state.visibleEvents;
    const grouped = groupEventsByDay(results);

    return `
      <section class="calendar-panel">
        <div class="calendar-toolbar">
          <div class="calendar-toolbar-copy">
            <p class="toolbar-label">Release search</p>
            <h2>${escapeHtml(this.getSearchQuery())}</h2>
          </div>
          <div class="calendar-toolbar-actions">
            <button class="nav-button" type="button" data-clear-search>Clear search</button>
          </div>
        </div>

        ${
          !results.length
            ? `
              <div class="calendar-empty">
                <h3>No upcoming releases match the current search</h3>
                <p>Try a broader query, or clear search and refine by sector or cadence instead.</p>
              </div>
            `
            : `
              <div class="search-results">
                ${Array.from(grouped.entries())
                  .map(([dayIso, dayEvents]) => {
                    const dayDate = new Date(`${dayIso}T00:00:00Z`);
                    return `
                      <section class="search-day-group">
                        <header class="search-day-head">
                          <p class="search-day-label">${escapeHtml(fullDayFormatter.format(dayDate))}</p>
                          <span class="search-day-count">${formatCountLabel(dayEvents.length, "release")}</span>
                        </header>
                        <div class="search-day-list">
                          ${dayEvents.map((event) => renderEventCard(event)).join("")}
                        </div>
                      </section>
                    `;
                  })
                  .join("")}
              </div>
            `
        }
      </section>
    `;
  }

  renderWeekView() {
    const today = startOfUtcDay(new Date());
    const weekDays = buildWeekDays(this.state.anchorDate);
    const currentRangeEvents = this.getCurrentRangeEvents();
    const grouped = groupEventsByDay(currentRangeEvents);
    const hasAnyEvents = currentRangeEvents.length > 0;
    const atMinimumAnchor = this.state.anchorDate.getTime() === getMinimumAnchorDate("week").getTime();
    const atMaximumAnchor = this.state.anchorDate.getTime() === this.getMaximumAnchor("week").getTime();
    const maxVisibleDay = startOfUtcDay(MAX_VISIBLE_DATE);
    const calendarLimitLabel = formatCalendarLimitDate(MAX_VISIBLE_DATE);

    return `
      <section class="calendar-panel">
        <div class="calendar-toolbar">
          <div class="calendar-toolbar-copy">
            <p class="toolbar-label">Week schedule</p>
            <h2>${escapeHtml(formatWeekRangeLabel(this.state.anchorDate))}</h2>
          </div>
          <div class="calendar-toolbar-actions">
            <button class="nav-button" type="button" data-nav-direction="prev" aria-label="Previous week" ${
              atMinimumAnchor ? "disabled" : ""
            }>Previous</button>
            <button class="nav-button" type="button" data-nav-reset ${atMinimumAnchor ? "disabled" : ""}>This week</button>
            <button class="nav-button" type="button" data-nav-direction="next" aria-label="Next week" ${
              atMaximumAnchor ? "disabled" : ""
            }>Next</button>
          </div>
        </div>

        ${
          !hasAnyEvents
            ? `
              <div class="calendar-empty">
                <h3>No scheduled events match the current filters</h3>
                <p>Clear filters or move to a different week to restore the schedule.</p>
              </div>
            `
            : `
              <div class="week-grid">
                ${weekDays
                  .map((day) => {
                    if (day.getTime() > maxVisibleDay.getTime()) {
                      return `
                        <section class="day-column day-column-unavailable" aria-label="Out of range">
                          <header class="day-column-head">
                            <div>
                              <p class="day-label">Calendar limit</p>
                              <h3>Unavailable</h3>
                            </div>
                          </header>
                          <div class="day-column-body">
                            <div class="day-empty day-empty-unavailable">
                              <span>Calendar ends on ${escapeHtml(calendarLimitLabel)}</span>
                            </div>
                          </div>
                        </section>
                      `;
                    }

                    const events = getEventsForDay(grouped, day);
                    const isToday = isSameUtcDay(day, today);
                    return `
                      <section class="day-column${isToday ? " is-today" : ""}" aria-label="${escapeHtml(fullDayFormatter.format(day))}">
                        <header class="day-column-head">
                          <div>
                            <p class="day-label">${escapeHtml(weekdayFormatter.format(day))}</p>
                            <h3>${escapeHtml(dayFormatter.format(day))}</h3>
                          </div>
                          <div class="day-head-meta">
                            ${isToday ? '<span class="today-pill">Today</span>' : ""}
                            <span class="day-count">${formatCountLabel(events.length, "event")}</span>
                          </div>
                        </header>
                        <div class="day-column-body">
                          ${
                            events.length
                              ? events.map((event) => renderEventCard(event)).join("")
                              : `
                                <div class="day-empty">
                                  <span>No scheduled releases</span>
                                </div>
                              `
                          }
                        </div>
                      </section>
                    `;
                  })
                  .join("")}
              </div>
            `
        }
      </section>
    `;
  }

  renderMonthView() {
    const today = startOfUtcDay(new Date());
    const rangeEvents = this.getCurrentRangeEvents();
    const grouped = groupEventsByDay(rangeEvents);
    const gridDays = buildMonthGrid(this.state.anchorDate);

    const currentMonthLabel = monthFormatter.format(this.state.anchorDate);
    const atMinimumAnchor = this.state.anchorDate.getTime() === getMinimumAnchorDate("month").getTime();
    const atMaximumAnchor = this.state.anchorDate.getTime() === this.getMaximumAnchor("month").getTime();
    const maxVisibleDay = startOfUtcDay(MAX_VISIBLE_DATE);

    return `
      <section class="calendar-panel">
        <div class="calendar-toolbar">
          <div class="calendar-toolbar-copy">
            <p class="toolbar-label">Month schedule</p>
            <h2>${escapeHtml(currentMonthLabel)}</h2>
          </div>
          <div class="calendar-toolbar-actions">
            <button class="nav-button" type="button" data-nav-direction="prev" aria-label="Previous month" ${
              atMinimumAnchor ? "disabled" : ""
            }>Previous</button>
            <button class="nav-button" type="button" data-nav-reset ${atMinimumAnchor ? "disabled" : ""}>This month</button>
            <button class="nav-button" type="button" data-nav-direction="next" aria-label="Next month" ${
              atMaximumAnchor ? "disabled" : ""
            }>Next</button>
          </div>
        </div>

        <div class="month-grid">
          ${["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            .map((label) => `<div class="month-grid-head">${label}</div>`)
            .join("")}

          ${gridDays
            .map((day) => {
              if (day.getTime() > maxVisibleDay.getTime()) {
                return `
                  <div class="month-cell month-cell-unavailable" aria-hidden="true"></div>
                `;
              }

              const dayIso = toIsoDay(day);
              const events = grouped.get(dayIso) || [];
              const inCurrentMonth = isSameUtcMonth(day, this.state.anchorDate);
              const hasEvents = events.length > 0;
              const previewEvents = events.slice(0, 2);
              const sectorMix = [...new Set(events.flatMap((event) => (Array.isArray(event.commodity_sectors) ? event.commodity_sectors : [])))].slice(
                0,
                4
              );

              return `
                <button
                  class="month-cell${inCurrentMonth ? "" : " is-outside"}${isSameUtcDay(day, today) ? " is-today" : ""}${hasEvents ? " has-events" : ""}"
                  type="button"
                  data-day-open="${hasEvents ? dayIso : ""}"
                  ${hasEvents ? "" : "disabled"}
                  aria-label="${escapeHtml(fullDayFormatter.format(day))}${hasEvents ? `, ${events.length} scheduled` : ", no events"}"
                >
                  <div class="month-cell-top">
                    <span class="month-cell-day">${day.getUTCDate()}</span>
                    ${hasEvents ? `<span class="month-count-badge">${events.length}</span>` : ""}
                  </div>
                  <div class="month-sector-indicators">
                    ${sectorMix
                      .map(
                        (sector) =>
                          `<span class="month-sector-indicator" style="--indicator-color:${SECTOR_META[sector].indicator}" aria-hidden="true"></span>`
                      )
                      .join("")}
                  </div>
                  <div class="month-preview-list">
                    ${previewEvents
                      .map((eventItem) => {
                        const firstSector = Array.isArray(eventItem.commodity_sectors) && eventItem.commodity_sectors[0];
                        const previewAccent = firstSector && SECTOR_META[firstSector] ? SECTOR_META[firstSector].accent : null;
                        return `<span class="month-preview-item"${previewAccent ? ` style="--preview-accent:${escapeHtml(previewAccent)}"` : ""}>${escapeHtml(eventItem.name)}</span>`;
                      })
                      .join("")}
                  </div>
                </button>
              `;
            })
            .join("")}
        </div>
      </section>
    `;
  }

  renderDrawer() {
    const isOpen = Boolean(this.state.panelMode);
    this.drawerOverlay.hidden = !isOpen;
    this.drawerOverlay.classList.toggle("is-open", isOpen);
    document.body.classList.toggle("drawer-open", isOpen);

    if (!isOpen) {
      this.drawerPanel.innerHTML = "";
      return;
    }

    if (this.state.panelMode === "day") {
      const grouped = groupEventsByDay(this.getCurrentRangeEvents());
      const dayEvents = grouped.get(this.state.panelDayIso) || [];
      const dayLabel = this.state.panelDayIso ? formatUtcDay(this.state.panelDayIso) : "Unknown day";

      this.drawerPanel.innerHTML = `
        <div class="drawer-shell">
          <header class="drawer-header">
            <div>
              <p class="drawer-kicker">${formatCountLabel(dayEvents.length, "event")} scheduled</p>
              <h2 id="drawer-title">${escapeHtml(dayLabel)}</h2>
            </div>
            <button class="drawer-close" type="button" data-drawer-close aria-label="Close drawer">Close</button>
          </header>
          <div class="drawer-body">
            <div class="drawer-card-list">
              ${dayEvents.map(renderDayListCard).join("")}
            </div>
          </div>
        </div>
      `;
      return;
    }

    const selectedEvent = this.state.visibleEvents.find((event) => event.id === this.state.selectedEventId);

    if (!selectedEvent) {
      this.closePanel();
      return;
    }

    this.drawerPanel.innerHTML = `
      <div class="drawer-shell">
        <header class="drawer-header">
          <div class="drawer-header-actions">
            ${
              this.state.panelOrigin === "day"
                ? `<button class="drawer-back" type="button" data-drawer-back>Back</button>`
                : ""
            }
          </div>
          <button class="drawer-close" type="button" data-drawer-close aria-label="Close drawer">Close</button>
        </header>
        <div class="drawer-body drawer-body-detail">
          <div class="detail-topline">
            <div class="detail-meta-strip">
              <span class="detail-status${selectedEvent.is_confirmed ? " is-confirmed" : " is-provisional"}">
                ${iconLock(selectedEvent.is_confirmed)}
                ${escapeHtml(getConfirmedLabel(selectedEvent))}
              </span>
              ${renderCadenceBadge(selectedEvent.cadence)}
            </div>
            <span class="detail-source-host">${escapeHtml(getSourceHostname(selectedEvent.calendar_url))}</span>
          </div>
          <h2 id="drawer-title" class="detail-title">${escapeHtml(selectedEvent.name)}</h2>
          <p class="detail-organiser">${escapeHtml(selectedEvent.organiser)}</p>

          <div class="detail-grid">
            <div class="detail-row">
              <span class="detail-label">Date and time</span>
              <span class="detail-value">${escapeHtml(formatUtcDateTime(selectedEvent.event_date))}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Sectors</span>
              <div class="detail-value detail-tags">${selectedEvent.commodity_sectors.map(renderSectorTag).join("")}</div>
            </div>
          </div>

          ${
            selectedEvent.notes
              ? `
                <section class="detail-notes">
                  <h3>Notes</h3>
                  <p>${escapeHtml(selectedEvent.notes)}</p>
                </section>
              `
              : ""
          }

          <a
            class="source-button"
            href="${escapeHtml(selectedEvent.calendar_url)}"
            target="_blank"
            rel="noreferrer noopener"
          >
            View Source
          </a>
          <p class="detail-attribution">
            Source:
            <a href="${escapeHtml(selectedEvent.calendar_url)}" target="_blank" rel="noreferrer noopener">
              ${escapeHtml(selectedEvent.source_label)}
            </a>
          </p>
        </div>
      </div>
    `;
  }
}

function getMountElements(documentRef) {
  if (!documentRef) {
    return null;
  }

  const root = documentRef.getElementById("calendar-root");
  const filterRoot = documentRef.getElementById("calendar-filter-root");
  const drawerOverlay = documentRef.getElementById("drawer-overlay");
  const drawerPanel = documentRef.getElementById("drawer-panel");
  const toTopButton = documentRef.getElementById("to-top-btn");
  const navSearch = documentRef.getElementById("nav-search");
  const searchToggle = documentRef.getElementById("search-toggle");
  const searchInput = documentRef.getElementById("calendar-search");

  if (
    !root ||
    !filterRoot ||
    !drawerOverlay ||
    !drawerPanel ||
    !toTopButton ||
    !navSearch ||
    !searchToggle ||
    !searchInput
  ) {
    return null;
  }

  return {
    root,
    filterRoot,
    drawerOverlay,
    drawerPanel,
    toTopButton,
    navSearch,
    searchToggle,
    searchInput,
  };
}

export function mountCalendarWatchApp(documentRef = globalThis.document) {
  const mountElements = getMountElements(documentRef);
  if (!mountElements) {
    return null;
  }

  const app = new CalendarWatchApp(mountElements);
  void app.init();
  return app;
}

if (typeof document !== "undefined") {
  mountCalendarWatchApp(document);
}
