const absoluteFormatter = new Intl.DateTimeFormat("en-GB", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

export function formatUtcTimestamp(timestamp: string | null | undefined) {
  if (!timestamp) {
    return "Awaiting update";
  }

  return `${absoluteFormatter.format(new Date(timestamp)).replace(",", "")} UTC`;
}

export function ageInHours(timestamp: string | null | undefined) {
  if (!timestamp) {
    return Number.POSITIVE_INFINITY;
  }

  const diff = Date.now() - new Date(timestamp).getTime();
  return diff / (1000 * 60 * 60);
}

export function ageInDays(timestamp: string | null | undefined) {
  return ageInHours(timestamp) / 24;
}

export function isoDate(timestamp: string | null | undefined) {
  if (!timestamp) {
    return "N/A";
  }

  return new Date(timestamp).toISOString().slice(0, 10);
}
