export const CALENDAR_ROUTE_PREFIX = "/calendar-watch";

export function buildCalendarEventHref(eventId) {
  const search = new URLSearchParams();
  search.set("event", String(eventId || "").trim());
  return `${CALENDAR_ROUTE_PREFIX}/?${search.toString()}`;
}

export function parseCalendarLocation(locationRef = globalThis.location) {
  const search = new URLSearchParams(locationRef?.search || "");
  const eventId = String(search.get("event") || "").trim();

  return {
    eventId: eventId || null,
  };
}
