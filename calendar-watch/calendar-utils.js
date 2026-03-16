export const SECTOR_OPTIONS = [
  { id: "energy", label: "Energy" },
  { id: "metals", label: "Metals" },
  { id: "agriculture", label: "Agriculture" },
  { id: "macro", label: "Macro" },
  { id: "cross-commodity", label: "Cross-Commodity" },
];

export const CADENCE_OPTIONS = [
  { id: "weekly", label: "Weekly" },
  { id: "monthly", label: "Monthly" },
  { id: "quarterly", label: "Quarterly" },
  { id: "annual", label: "Annual" },
  { id: "ad_hoc", label: "Ad Hoc" },
];

export const SECTOR_META = {
  energy: {
    label: "Energy",
    accent: "#E8A020",
    pillBackground: "#F7E9C5",
    pillText: "#A66E0E",
    indicator: "#E8A020",
  },
  metals: {
    label: "Metals",
    accent: "#4A90D9",
    pillBackground: "#E3EFFB",
    pillText: "#2F6FAE",
    indicator: "#4A90D9",
  },
  agriculture: {
    label: "Agriculture",
    accent: "#5BA85C",
    pillBackground: "#E4F2E2",
    pillText: "#3F7D41",
    indicator: "#5BA85C",
  },
  macro: {
    label: "Macro",
    accent: "#8662D6",
    pillBackground: "#EEE8FB",
    pillText: "#6246A5",
    indicator: "#8662D6",
  },
  "cross-commodity": {
    label: "Cross-Commodity",
    accent: "#7B8894",
    pillBackground: "#E8EDF1",
    pillText: "#5A6671",
    indicator: "#7B8894",
  },
};

export const CADENCE_META = {
  weekly: { label: "Weekly" },
  monthly: { label: "Monthly" },
  quarterly: { label: "Quarterly" },
  annual: { label: "Annual" },
  ad_hoc: { label: "Ad Hoc" },
};

export const DEFAULT_FILTERS = Object.freeze({
  sectors: SECTOR_OPTIONS.map((option) => option.id),
  cadences: CADENCE_OPTIONS.map((option) => option.id),
  confirmedOnly: false,
});

export function cloneDefaultFilters() {
  return {
    sectors: [...DEFAULT_FILTERS.sectors],
    cadences: [...DEFAULT_FILTERS.cadences],
    confirmedOnly: DEFAULT_FILTERS.confirmedOnly,
  };
}

export function toUtcDate(value) {
  return value instanceof Date ? new Date(value.getTime()) : new Date(value);
}

export function startOfUtcDay(value) {
  const date = toUtcDate(value);
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

export function addUtcDays(value, amount) {
  const date = startOfUtcDay(value);
  date.setUTCDate(date.getUTCDate() + amount);
  return date;
}

export function addUtcMonths(value, amount) {
  const date = toUtcDate(value);
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + amount, 1));
}

export function startOfUtcWeek(value) {
  const date = startOfUtcDay(value);
  const weekday = date.getUTCDay();
  const offset = weekday === 0 ? -6 : 1 - weekday;
  return addUtcDays(date, offset);
}

export function startOfUtcMonth(value) {
  const date = toUtcDate(value);
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

export function endOfUtcMonth(value) {
  const date = toUtcDate(value);
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0));
}

export function endOfUtcYear(value = new Date()) {
  const date = toUtcDate(value);
  return new Date(Date.UTC(date.getUTCFullYear(), 11, 31, 23, 59, 59));
}

export function buildWeekDays(anchorDate) {
  const weekStart = startOfUtcWeek(anchorDate);
  return Array.from({ length: 5 }, (_, index) => addUtcDays(weekStart, index));
}

export function buildMonthGrid(anchorDate) {
  const monthStart = startOfUtcMonth(anchorDate);
  const monthEnd = endOfUtcMonth(anchorDate);
  const gridStart = startOfUtcWeek(monthStart);
  const afterMonthEnd = addUtcDays(monthEnd, 1);
  const gridEnd = addUtcDays(startOfUtcWeek(afterMonthEnd), 6);
  const days = [];
  let cursor = gridStart;

  while (cursor.getTime() <= gridEnd.getTime()) {
    days.push(cursor);
    cursor = addUtcDays(cursor, 1);
  }

  return days;
}

export function buildRangeForView(viewMode, anchorDate) {
  if (viewMode === "month") {
    const grid = buildMonthGrid(anchorDate);
    return {
      from: new Date(`${toIsoDay(grid[0])}T00:00:00Z`).toISOString(),
      to: new Date(`${toIsoDay(grid[grid.length - 1])}T23:59:59Z`).toISOString(),
    };
  }

  const days = buildWeekDays(anchorDate);
  return {
    from: new Date(`${toIsoDay(days[0])}T00:00:00Z`).toISOString(),
    to: new Date(`${toIsoDay(days[days.length - 1])}T23:59:59Z`).toISOString(),
  };
}

export function filterEventsByRange(events, range) {
  const fromTime = range?.from ? Date.parse(range.from) : Number.NEGATIVE_INFINITY;
  const toTime = range?.to ? Date.parse(range.to) : Number.POSITIVE_INFINITY;

  return events.filter((event) => {
    const eventTime = Date.parse(event.event_date);
    return eventTime >= fromTime && eventTime <= toTime;
  });
}

export function toIsoDay(value) {
  const date = startOfUtcDay(value);
  return date.toISOString().slice(0, 10);
}

export function isSameUtcDay(left, right) {
  return toIsoDay(left) === toIsoDay(right);
}

export function isSameUtcMonth(left, right) {
  const first = toUtcDate(left);
  const second = toUtcDate(right);
  return first.getUTCFullYear() === second.getUTCFullYear() && first.getUTCMonth() === second.getUTCMonth();
}

export function sortEvents(events) {
  return [...events].sort((left, right) => {
    const dateDifference = Date.parse(left.event_date) - Date.parse(right.event_date);
    if (dateDifference !== 0) {
      return dateDifference;
    }
    return left.name.localeCompare(right.name, "en");
  });
}

export function normalizeFilters(filters) {
  return {
    sectors: Array.isArray(filters?.sectors) ? [...filters.sectors] : [...DEFAULT_FILTERS.sectors],
    cadences: Array.isArray(filters?.cadences) ? [...filters.cadences] : [...DEFAULT_FILTERS.cadences],
    confirmedOnly: Boolean(filters?.confirmedOnly),
  };
}

export function filterEvents(events, rawFilters) {
  const filters = normalizeFilters(rawFilters);
  const selectedSectors = new Set(filters.sectors);
  const selectedCadences = new Set(filters.cadences);

  return events.filter((event) => {
    if (!selectedCadences.has(event.cadence)) {
      return false;
    }

    if (filters.confirmedOnly && !event.is_confirmed) {
      return false;
    }

    if (!event.commodity_sectors.some((sector) => selectedSectors.has(sector))) {
      return false;
    }

    return true;
  });
}

export function groupEventsByDay(events) {
  return events.reduce((groups, event) => {
    const key = toIsoDay(event.event_date);
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(event);
    return groups;
  }, new Map());
}

export function getEventsForDay(groupedEvents, day) {
  return groupedEvents.get(toIsoDay(day)) || [];
}

export function countActiveFilters(rawFilters) {
  const filters = normalizeFilters(rawFilters);
  let count = 0;

  if (filters.sectors.length !== DEFAULT_FILTERS.sectors.length) {
    count += 1;
  }

  if (filters.cadences.length !== DEFAULT_FILTERS.cadences.length) {
    count += 1;
  }

  if (filters.confirmedOnly) {
    count += 1;
  }

  return count;
}

export function areAllFiltersSelected(values, allValues) {
  const selected = new Set(values);
  return allValues.every((value) => selected.has(value));
}

export function toggleFilterSelection(values, value, allValues = null) {
  if (!Array.isArray(allValues) || allValues.length === 0) {
    const nextValues = new Set(values);

    if (nextValues.has(value)) {
      nextValues.delete(value);
    } else {
      nextValues.add(value);
    }

    return [...nextValues];
  }

  const currentValues = new Set(values);
  const allSelected = areAllFiltersSelected(values, allValues);

  if (allSelected) {
    return [value];
  }

  if (currentValues.has(value)) {
    currentValues.delete(value);
  } else {
    currentValues.add(value);
  }

  if (currentValues.size === 0 || currentValues.size === allValues.length) {
    return [...allValues];
  }

  return allValues.filter((option) => currentValues.has(option));
}

export function getSectorMix(events) {
  const counts = new Map();

  events.forEach((event) => {
    event.commodity_sectors.forEach((sector) => {
      counts.set(sector, (counts.get(sector) || 0) + 1);
    });
  });

  return [...counts.entries()]
    .sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }
      return left[0].localeCompare(right[0], "en");
    })
    .map(([sector]) => sector);
}

export function getInitialAnchorDate(events) {
  const today = startOfUtcDay(new Date());

  if (!events.length) {
    return today;
  }

  const first = startOfUtcDay(events[0].event_date);
  const last = startOfUtcDay(events[events.length - 1].event_date);

  if (today.getTime() < addUtcDays(first, -7).getTime() || today.getTime() > addUtcDays(last, 7).getTime()) {
    return first;
  }

  return today;
}

export function normalizeAnchorDateForView(viewMode, value) {
  return viewMode === "month" ? startOfUtcMonth(value) : startOfUtcWeek(value);
}

export function getMinimumAnchorDate(viewMode, referenceDate = new Date()) {
  return normalizeAnchorDateForView(viewMode, referenceDate);
}

export function getMaximumAnchorDate(viewMode, limitDate) {
  if (!limitDate) {
    return null;
  }

  return normalizeAnchorDateForView(viewMode, limitDate);
}

export function clampAnchorDateForView(viewMode, value, referenceDate = new Date(), limitDate = null) {
  const minimumAnchor = getMinimumAnchorDate(viewMode, referenceDate);
  const candidate = normalizeAnchorDateForView(viewMode, value);
  const maximumAnchor = getMaximumAnchorDate(viewMode, limitDate);

  if (candidate.getTime() < minimumAnchor.getTime()) {
    return minimumAnchor;
  }

  if (maximumAnchor && candidate.getTime() > maximumAnchor.getTime()) {
    return maximumAnchor;
  }

  return candidate;
}

export function searchEvents(events, query) {
  const normalizedQuery = String(query ?? "").trim().toLowerCase();
  if (!normalizedQuery) {
    return [...events];
  }

  const searchTerms = normalizedQuery.split(/\s+/).filter(Boolean);

  return events.filter((event) => {
    const haystack = [
      event.name,
      event.organiser,
      event.source_label,
      event.notes,
      ...(event.commodity_sectors || []),
      event.cadence,
    ]
      .join(" ")
      .toLowerCase();

    return searchTerms.every((term) => haystack.includes(term));
  });
}
