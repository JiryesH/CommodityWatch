from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone

from ..types import CandidateEvent, utc_now
from .base import StructuredFeedAdapter


EUROSTAT_ICAL_URL = "https://ec.europa.eu/eurostat/o/calendars/eventsIcal?theme=0&category=0"
EUROSTAT_CALENDAR_URL = "https://ec.europa.eu/eurostat/news/release-calendar"
EUROSTAT_ORGANISER = "Eurostat"
EUROSTAT_SOURCE_LABEL = "Eurostat"
RELEVANT_THEMES = {
    "agriculture",
    "economy",
    "environment",
    "industry",
    "international",
    "population",
    "transport",
}
MACRO_KEYWORDS = (
    "balance of payments",
    "building permits",
    "construction",
    "earnings",
    "exchange rates",
    "gdp",
    "government debt",
    "government deficit",
    "hicp",
    "house price",
    "house sales",
    "industrial",
    "inflation",
    "interest rates",
    "job vacancy",
    "labour",
    "minimum wages",
    "monthly energy",
    "production",
    "retail",
    "services",
    "trade",
    "turnover",
    "unemployment",
)
AGRICULTURE_KEYWORDS = (
    "agricultural",
    "chicks",
    "cows",
    "eggs",
    "livestock",
    "milk",
    "slaughter",
)
ENERGY_KEYWORDS = (
    "carbon",
    "climate",
    "coal",
    "electric",
    "emission",
    "energy",
    "gas",
    "oil",
    "petroleum",
)


class EurostatReleaseCalendarAdapter(StructuredFeedAdapter):
    slug = "eurostat"
    primary_url = EUROSTAT_ICAL_URL

    def __init__(self, *, horizon_days: int = 365):
        self.horizon_days = horizon_days

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        horizon = current + timedelta(days=self.horizon_days)
        body = client.get(EUROSTAT_ICAL_URL).text
        raw_events = self._parse_ical(body)
        cadence_by_summary = self._infer_cadence_map(raw_events)

        events: list[CandidateEvent] = []
        for raw_event in raw_events:
            event_day = raw_event.get("date")
            summary = raw_event.get("summary")
            theme = raw_event.get("theme")
            categories = raw_event.get("categories", "")
            if not isinstance(event_day, date) or not isinstance(summary, str) or not summary:
                continue
            if not isinstance(theme, str) or theme not in RELEVANT_THEMES:
                continue
            if "data release" not in categories.lower():
                continue
            if not self._is_relevant_release(summary, theme):
                continue

            event_date = datetime.combine(event_day, time(0, 0), tzinfo=timezone.utc)
            if event_date < current - timedelta(days=7) or event_date > horizon:
                continue

            notes = (
                "Eurostat's iCal feed confirms the release date but does not specify a publication hour."
            )
            events.append(
                CandidateEvent(
                    name=f"Eurostat {summary}",
                    organiser=EUROSTAT_ORGANISER,
                    cadence=cadence_by_summary[summary],
                    commodity_sectors=self._commodity_sectors(summary, theme),
                    event_date=event_date,
                    calendar_url=EUROSTAT_CALENDAR_URL,
                    redistribution_ok=True,
                    source_label=EUROSTAT_SOURCE_LABEL,
                    notes=notes,
                    is_confirmed=True,
                    source_item_key=str(raw_event.get("uid") or f"{summary}-{event_day.isoformat()}"),
                    raw_payload={
                        "summary": summary,
                        "theme": theme,
                        "categories": categories,
                        "date_precision": "date_only",
                    },
                )
            )

        if not events:
            raise ValueError("Eurostat iCal feed did not yield any publishable data releases")
        return events

    @classmethod
    def _infer_cadence_map(cls, raw_events: list[dict[str, object]]) -> dict[str, str]:
        grouped_dates: dict[str, list[date]] = defaultdict(list)
        for raw_event in raw_events:
            summary = raw_event.get("summary")
            event_day = raw_event.get("date")
            if isinstance(summary, str) and isinstance(event_day, date):
                grouped_dates[summary].append(event_day)

        cadence_by_summary: dict[str, str] = {}
        for summary, days in grouped_dates.items():
            cadence_by_summary[summary] = cls._infer_cadence(days)
        return cadence_by_summary

    @staticmethod
    def _infer_cadence(days: list[date]) -> str:
        unique_days = sorted(set(days))
        if len(unique_days) == 1:
            return "ad_hoc"

        day_gaps = [(current - previous).days for previous, current in zip(unique_days, unique_days[1:])]
        if day_gaps and all(25 <= gap <= 38 for gap in day_gaps):
            return "monthly"
        if day_gaps and all(75 <= gap <= 105 for gap in day_gaps):
            return "quarterly"
        if day_gaps and all(330 <= gap <= 400 for gap in day_gaps):
            return "annual"
        if day_gaps and all(5 <= gap <= 10 for gap in day_gaps):
            return "weekly"
        return "ad_hoc"

    @staticmethod
    def _commodity_sectors(summary: str, theme: str) -> tuple[str, ...]:
        normalized = summary.lower()
        if theme == "agriculture":
            return ("agriculture",)
        if any(keyword in normalized for keyword in ("energy", "oil", "gas", "electric", "coal", "petroleum")):
            return ("energy", "macro")
        if theme == "environment":
            if any(keyword in normalized for keyword in ("emission", "climate", "carbon")):
                return ("energy", "cross-commodity")
            return ("cross-commodity",)
        if theme == "transport":
            return ("energy", "macro")
        return ("macro",)

    @staticmethod
    def _is_relevant_release(summary: str, theme: str) -> bool:
        normalized = summary.lower()
        if theme == "agriculture":
            return any(keyword in normalized for keyword in AGRICULTURE_KEYWORDS)
        if theme == "environment":
            return any(keyword in normalized for keyword in ENERGY_KEYWORDS)
        if theme == "population":
            return any(keyword in normalized for keyword in ("labour", "unemployment", "earnings", "minimum wages"))
        if theme in {"economy", "industry", "international", "transport"}:
            return any(keyword in normalized for keyword in MACRO_KEYWORDS + ENERGY_KEYWORDS + AGRICULTURE_KEYWORDS)
        return False

    @staticmethod
    def _parse_ical(payload: str) -> list[dict[str, object]]:
        unfolded_lines: list[str] = []
        for raw_line in payload.splitlines():
            line = raw_line.rstrip("\r")
            if line.startswith((" ", "\t")) and unfolded_lines:
                unfolded_lines[-1] += line[1:]
            else:
                unfolded_lines.append(line)

        events: list[dict[str, object]] = []
        current_event: dict[str, object] | None = None
        for line in unfolded_lines:
            if line == "BEGIN:VEVENT":
                current_event = {}
                continue
            if line == "END:VEVENT":
                if current_event is not None:
                    events.append(current_event)
                current_event = None
                continue
            if current_event is None or ":" not in line:
                continue

            key, value = line.split(":", 1)
            key_name = key.split(";", 1)[0]
            decoded_value = (
                value.replace("\\,", ",")
                .replace("\\;", ";")
                .replace("\\n", " ")
                .replace("\\N", " ")
                .replace("\\\\", "\\")
            )
            if key_name == "SUMMARY":
                current_event["summary"] = decoded_value.strip()
            elif key_name == "UID":
                current_event["uid"] = decoded_value.strip()
            elif key_name == "X-THEME":
                current_event["theme"] = decoded_value.strip().lower()
            elif key_name == "X-CATEGORY":
                current_event["categories"] = decoded_value.strip()
            elif key_name == "DTSTART":
                current_event["date"] = datetime.strptime(decoded_value.strip(), "%Y%m%d").date()

        return events
