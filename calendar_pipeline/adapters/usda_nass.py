from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from ..time import add_months, eastern_to_utc, month_start, parse_us_time, slugify
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


NASS_BASE_URL = "https://www.nass.usda.gov/Publications/Calendar/reports_by_date.php"
NASS_ORGANISER = "USDA National Agricultural Statistics Service"
NASS_SOURCE_LABEL = "USDA NASS"


@dataclass(frozen=True)
class ParsedNassRelease:
    title: str
    release_day: date
    release_time_text: str
    status: str
    href: str
    report_id: str | None


class UsdaNassCalendarAdapter(HtmlCalendarAdapter):
    slug = "usda_nass"
    primary_url = NASS_BASE_URL

    def __init__(self, *, months_ahead: int = 12):
        self.months_ahead = months_ahead

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        first_month = month_start(current.date())
        releases: list[ParsedNassRelease] = []

        for offset in range(self.months_ahead + 1):
            target_month = add_months(first_month, offset)
            url = (
                f"{NASS_BASE_URL}?view=l&js=1&month={target_month.month:02d}&year={target_month.year}"
            )
            html = client.get(url).text
            releases.extend(
                self._parse_month_page(
                    html,
                    source_url=url,
                    current=current,
                    target_month=target_month,
                )
            )

        cadence_by_report = self._infer_cadence_map(releases)
        events: list[CandidateEvent] = []
        for release in releases:
            event_date = eastern_to_utc(release.release_day, parse_us_time(release.release_time_text))
            if event_date < current - timedelta(days=7):
                continue

            display_name = self._display_name(release.title)
            report_key = release.report_id or slugify(release.title)
            notes = release.status or None
            events.append(
                CandidateEvent(
                    name=display_name,
                    organiser=NASS_ORGANISER,
                    cadence=cadence_by_report[report_key],
                    commodity_sectors=("agriculture",),
                    event_date=event_date,
                    calendar_url=release.href,
                    redistribution_ok=True,
                    source_label=NASS_SOURCE_LABEL,
                    notes=notes,
                    is_confirmed=True,
                    source_item_key=f"nass-{report_key}-{release.release_day.isoformat()}",
                    raw_payload={
                        "raw_title": release.title,
                        "status": release.status,
                        "report_id": release.report_id,
                    },
                )
            )

        return events

    def _parse_month_page(
        self,
        html: str,
        *,
        source_url: str,
        current: datetime,
        target_month: date,
    ) -> list[ParsedNassRelease]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="calendar")
        if table is None:
            current_month = month_start(current.date())
            calendar_container = soup.find(id="calendar-container")
            if calendar_container is not None and target_month > current_month:
                return []
            raise ValueError("USDA NASS calendar table not found")

        releases: list[ParsedNassRelease] = []
        current_day: date | None = None

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            day_cell = cells[0].get_text(" ", strip=True)
            if day_cell:
                current_day = datetime.strptime(day_cell, "%a, %m/%d/%y").date()
            if current_day is None:
                continue

            link = cells[2].find("a")
            if link is None:
                continue

            raw_title = self._normalize_title(link.get_text(" ", strip=True))
            if not raw_title:
                continue

            href = urljoin(source_url, link.get("href", ""))
            releases.append(
                ParsedNassRelease(
                    title=raw_title,
                    release_day=current_day,
                    release_time_text=cells[1].get_text(" ", strip=True),
                    status=cells[3].get_text(" ", strip=True),
                    href=href,
                    report_id=self._report_id_from_url(href),
                )
            )

        return releases

    @classmethod
    def _infer_cadence_map(cls, releases: list[ParsedNassRelease]) -> dict[str, str]:
        grouped_dates: dict[str, list[date]] = defaultdict(list)
        grouped_titles: dict[str, str] = {}

        for release in releases:
            report_key = release.report_id or slugify(release.title)
            grouped_dates[report_key].append(release.release_day)
            grouped_titles.setdefault(report_key, release.title)

        cadence_by_report: dict[str, str] = {}
        for report_key, days in grouped_dates.items():
            cadence_by_report[report_key] = cls._infer_cadence(days, grouped_titles[report_key])
        return cadence_by_report

    @staticmethod
    def _infer_cadence(days: list[date], title: str) -> str:
        normalized = title.lower()
        unique_days = sorted(set(days))
        if len(unique_days) >= 4:
            day_gaps = [(current - previous).days for previous, current in zip(unique_days, unique_days[1:])]
            if all(abs(gap - 7) <= 2 for gap in day_gaps):
                return "weekly"
            if all(25 <= gap <= 38 for gap in day_gaps):
                return "monthly"
            if all(75 <= gap <= 105 for gap in day_gaps):
                return "quarterly"
            if all(330 <= gap <= 400 for gap in day_gaps):
                return "annual"

        if len(unique_days) == 1:
            if "annual" in normalized:
                return "annual"
            return "ad_hoc"

        if len(unique_days) >= 2:
            months = [
                (current.year - previous.year) * 12 + (current.month - previous.month)
                for previous, current in zip(unique_days, unique_days[1:])
            ]
            if all(month == 1 for month in months):
                return "monthly"
            if all(month == 3 for month in months):
                return "quarterly"
            if all(month >= 11 for month in months):
                return "annual"

        if any(keyword in normalized for keyword in ("weekly", "progress", "condition")):
            return "weekly"
        if any(keyword in normalized for keyword in ("monthly", "prices", "inventory", "production", "stocks")):
            return "monthly"
        return "ad_hoc"

    @staticmethod
    def _display_name(title: str) -> str:
        return title if title.lower().startswith("usda") else f"USDA {title}"

    @staticmethod
    def _normalize_title(value: str) -> str:
        return " ".join(value.split())

    @staticmethod
    def _report_id_from_url(url: str) -> str | None:
        query = parse_qs(urlparse(url).query)
        values = query.get("report_id")
        return values[0] if values else None
