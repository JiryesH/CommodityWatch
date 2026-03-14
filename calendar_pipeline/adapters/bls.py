from __future__ import annotations

import re
from datetime import datetime, timedelta
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..http import BROWSER_USER_AGENT, HttpFetchError
from ..time import eastern_to_utc, parse_us_time, slugify
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


BLS_SCHEDULE_URL_TEMPLATE = "https://www.bls.gov/schedule/{year}/"

TRACKED_RELEASES = {
    "consumer price index": {"name": "BLS Consumer Price Index", "cadence": "monthly"},
    "producer price index": {"name": "BLS Producer Price Index", "cadence": "monthly"},
    "employment situation": {"name": "BLS Employment Situation", "cadence": "monthly"},
    "state employment and unemployment": {
        "name": "BLS State Employment and Unemployment",
        "cadence": "monthly",
    },
    "u.s. import and export price indexes": {
        "name": "BLS Import and Export Price Indexes",
        "cadence": "monthly",
    },
    "employment cost index": {"name": "BLS Employment Cost Index", "cadence": "quarterly"},
}


class BlsScheduleAdapter(HtmlCalendarAdapter):
    slug = "bls"
    primary_url = BLS_SCHEDULE_URL_TEMPLATE.format(year=datetime.utcnow().year)

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        events: list[CandidateEvent] = []
        years = [current.year]
        if (current + timedelta(days=90)).year > current.year:
            years.append(current.year + 1)

        for year in years:
            try:
                html = client.get(
                    BLS_SCHEDULE_URL_TEMPLATE.format(year=year),
                    user_agent=BROWSER_USER_AGENT,
                ).text
            except HttpFetchError as exc:
                if year > current.year and "HTTP 404" in str(exc):
                    continue
                raise
            events.extend(self._parse_year_page(html, year=year, current=current))
        return events

    def _parse_year_page(self, html: str, *, year: int, current: datetime) -> list[CandidateEvent]:
        soup = BeautifulSoup(html, "html.parser")
        parsed = self._parse_tabular_rows(soup, current=current)
        if parsed:
            return parsed
        return self._parse_text_rows(soup.get_text("\n"), current=current, year=year)

    def _parse_tabular_rows(self, soup: BeautifulSoup, *, current: datetime) -> list[CandidateEvent]:
        events: list[CandidateEvent] = []
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            date_text = cells[0].get_text(" ", strip=True)
            time_text = cells[1].get_text(" ", strip=True)
            title_text = cells[2].get_text(" ", strip=True)
            if not date_text or not time_text or not title_text:
                continue

            event_day = self._parse_day(date_text)
            if event_day is None:
                continue
            release = self._match_release(title_text)
            if release is None:
                continue

            event_date = eastern_to_utc(event_day, parse_us_time(time_text))
            if event_date < current - timedelta(days=7):
                continue
            link = cells[2].find("a")
            detail = cells[3].get_text(" ", strip=True) if len(cells) > 3 else None
            calendar_url = urljoin(BLS_SCHEDULE_URL_TEMPLATE.format(year=event_day.year), link.get("href", "")) if link else BLS_SCHEDULE_URL_TEMPLATE.format(year=event_day.year)
            events.append(
                self._build_event(
                    release=release,
                    event_date=event_date,
                    calendar_url=calendar_url,
                    raw_title=title_text,
                    detail=detail,
                )
            )
        return events

    def _parse_text_rows(self, page_text: str, *, current: datetime, year: int) -> list[CandidateEvent]:
        events: list[CandidateEvent] = []
        pattern = re.compile(
            r"([A-Z][a-z]+,\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4})\s+(\d{2}:\d{2}\s+[AP]M)\s+([^\n]+)",
            re.MULTILINE,
        )
        for match in pattern.finditer(page_text):
            event_day = self._parse_day(match.group(1))
            if event_day is None:
                continue
            release = self._match_release(match.group(3))
            if release is None:
                continue
            event_date = eastern_to_utc(event_day, parse_us_time(match.group(2)))
            if event_date < current - timedelta(days=7):
                continue
            events.append(
                self._build_event(
                    release=release,
                    event_date=event_date,
                    calendar_url=BLS_SCHEDULE_URL_TEMPLATE.format(year=year),
                    raw_title=match.group(3),
                    detail=None,
                )
            )
        return events

    @staticmethod
    def _parse_day(value: str):
        for fmt in ("%A, %B %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _match_release(value: str) -> dict[str, str] | None:
        normalized = value.strip().lower()
        for key, config in TRACKED_RELEASES.items():
            if key in normalized:
                return config
        return None

    @staticmethod
    def _build_event(
        *,
        release: dict[str, str],
        event_date: datetime,
        calendar_url: str,
        raw_title: str,
        detail: str | None,
    ) -> CandidateEvent:
        notes = raw_title if raw_title != release["name"] else None
        if detail:
            notes = f"{detail}. {notes}".strip() if notes else detail
        reference = detail or event_date.date().isoformat()
        return CandidateEvent(
            name=release["name"],
            organiser="U.S. Bureau of Labor Statistics",
            cadence=release["cadence"],
            commodity_sectors=("macro",),
            event_date=event_date,
            calendar_url=calendar_url,
            redistribution_ok=True,
            source_label="BLS",
            notes=notes,
            is_confirmed=True,
            source_item_key=f"{slugify(release['name'])}-{slugify(reference)}",
            raw_payload={
                "raw_title": raw_title,
                "detail": detail,
            },
        )
