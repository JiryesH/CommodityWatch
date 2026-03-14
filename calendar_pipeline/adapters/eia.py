from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..time import eastern_to_utc, nearest_weekday, parse_slash_date, parse_us_date, parse_us_time, slugify
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


WPSR_URL = "https://www.eia.gov/petroleum/supply/weekly/schedule.php"
NGS_URL = "http://ir.eia.gov/ngs/schedule.html"
GASDIESEL_SCHEDULE_URL = "https://www.eia.gov/petroleum/gasdiesel/schedule.php"
HEATING_OIL_SCHEDULE_URL = "https://www.eia.gov/petroleum/heatingoilpropane/schedule.php"
STEO_SCHEDULE_URL = "https://www.eia.gov/outlooks/steo/release_schedule.php"
UPCOMING_REPORTS_URL = "https://www.eia.gov/reports/upcoming.php"
EIA_ORGANISER = "U.S. Energy Information Administration"
EIA_SOURCE_LABEL = "EIA"

DEDICATED_REPORT_NAMES = {
    "Gasoline and Diesel Fuel Update",
    "Heating Oil & Propane Update",
    "Short-Term Energy Outlook",
    "Weekly Natural Gas Storage Report",
    "Weekly Petroleum Status Report",
}


@dataclass(frozen=True)
class UpcomingEiaReport:
    name: str
    url: str
    cadence: str
    schedule_text: str


class EiaScheduleAdapter(HtmlCalendarAdapter):
    slug = "eia"
    primary_url = WPSR_URL

    def __init__(self, *, horizon_days: int = 370):
        self.horizon_days = horizon_days

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        horizon = current + timedelta(days=self.horizon_days)

        pages = {
            WPSR_URL: client.get(WPSR_URL).text,
            NGS_URL: client.get(NGS_URL).text,
            GASDIESEL_SCHEDULE_URL: client.get(GASDIESEL_SCHEDULE_URL).text,
            HEATING_OIL_SCHEDULE_URL: client.get(HEATING_OIL_SCHEDULE_URL).text,
            STEO_SCHEDULE_URL: client.get(STEO_SCHEDULE_URL).text,
            UPCOMING_REPORTS_URL: client.get(UPCOMING_REPORTS_URL).text,
        }

        events: list[CandidateEvent] = []
        events.extend(self._collect_wpsr(pages[WPSR_URL], current=current, horizon=horizon))
        events.extend(self._collect_ngs(pages[NGS_URL], current=current, horizon=horizon))
        events.extend(self._collect_gasdiesel(pages[GASDIESEL_SCHEDULE_URL], current=current, horizon=horizon))
        events.extend(
            self._collect_heating_oil(pages[HEATING_OIL_SCHEDULE_URL], current=current, horizon=horizon)
        )
        events.extend(self._collect_steo(pages[STEO_SCHEDULE_URL], current=current, horizon=horizon))

        generic_reports = self._parse_upcoming_reports(pages[UPCOMING_REPORTS_URL])
        for report in generic_reports:
            if report.name in DEDICATED_REPORT_NAMES:
                continue

            if report.cadence == "weekly":
                events.extend(self._collect_generic_weekly_report(report, current=current, horizon=horizon))
                continue

            detail_html = client.get(report.url).text
            event = self._collect_detail_page_report(
                detail_html,
                report=report,
                current=current,
                horizon=horizon,
            )
            if event is not None:
                events.append(event)

        deduped: dict[str, CandidateEvent] = {}
        for event in events:
            deduped[event.source_item_key or event.natural_key_hash()] = event
        return sorted(deduped.values(), key=lambda event: event.event_date)

    def _collect_wpsr(self, html: str, *, current: datetime, horizon: datetime) -> list[CandidateEvent]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="schedule")
        if table is None:
            raise ValueError("EIA WPSR schedule table not found")

        exceptions: dict[date, tuple[date, time, str]] = {}
        for row in table.select("tbody tr"):
            header = row.find("th")
            cells = row.find_all("td")
            if header is None or len(cells) < 4:
                continue
            week_ending = parse_us_date(header.get_text(" ", strip=True))
            alternate_date = parse_us_date(cells[0].get_text(" ", strip=True))
            release_time = self._parse_eia_time(cells[2].get_text(" ", strip=True))
            holiday = cells[3].get_text(" ", strip=True)
            exceptions[week_ending] = (alternate_date, release_time, holiday)

        start_week_ending = self._previous_or_same_weekday(current.date() - timedelta(days=7), 4)
        end_week_ending = self._previous_or_same_weekday(horizon.date(), 4)
        week_ending = start_week_ending
        events: list[CandidateEvent] = []

        while week_ending <= end_week_ending:
            release_date = week_ending + timedelta(days=5)
            release_time = time(10, 30)
            note = (
                "Official EIA schedule publishes WPSR summary files after 10:30 a.m. Eastern on Wednesday. "
                "Some holiday weeks shift by one day."
            )
            if week_ending in exceptions:
                release_date, release_time, holiday = exceptions[week_ending]
                note = f"Holiday-adjusted release schedule for {holiday}. Standard release is 10:30 a.m. Eastern on Wednesday."

            event_date = eastern_to_utc(release_date, release_time)
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name="EIA Weekly Petroleum Status Report",
                        organiser=EIA_ORGANISER,
                        cadence="weekly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=WPSR_URL,
                        redistribution_ok=True,
                        source_label=EIA_SOURCE_LABEL,
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"wpsr-{week_ending.isoformat()}",
                        raw_payload={
                            "week_ending": week_ending.isoformat(),
                            "release_date": release_date.isoformat(),
                            "release_time_local": release_time.strftime("%H:%M"),
                        },
                    )
                )
            week_ending += timedelta(days=7)

        return events

    def _collect_ngs(self, html: str, *, current: datetime, horizon: datetime) -> list[CandidateEvent]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if table is None:
            raise ValueError("EIA natural gas storage schedule table not found")

        exceptions: dict[date, tuple[date, time, str]] = {}
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            alternate_date = parse_us_date(cells[0].get_text(" ", strip=True).split("-")[0].strip())
            nominal_release_date = nearest_weekday(alternate_date, 3, max_distance=3)
            if nominal_release_date is None:
                continue
            release_time = self._parse_eia_time(cells[2].get_text(" ", strip=True))
            holiday = cells[3].get_text(" ", strip=True)
            exceptions[nominal_release_date] = (alternate_date, release_time, holiday)

        release_date = self._previous_or_same_weekday(current.date() - timedelta(days=7), 3)
        final_release_date = self._previous_or_same_weekday(horizon.date(), 3)
        events: list[CandidateEvent] = []

        while release_date <= final_release_date:
            actual_release_date = release_date
            release_time = time(10, 30)
            note = "Official EIA schedule publishes the natural gas storage report at 10:30 a.m. Eastern on Thursday."
            if release_date in exceptions:
                actual_release_date, release_time, holiday = exceptions[release_date]
                note = f"Holiday-adjusted release schedule for {holiday}. Standard release is 10:30 a.m. Eastern on Thursday."

            event_date = eastern_to_utc(actual_release_date, release_time)
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name="EIA Weekly Natural Gas Storage Report",
                        organiser=EIA_ORGANISER,
                        cadence="weekly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=NGS_URL,
                        redistribution_ok=True,
                        source_label=EIA_SOURCE_LABEL,
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"ngs-{release_date.isoformat()}",
                        raw_payload={
                            "nominal_release_date": release_date.isoformat(),
                            "release_date": actual_release_date.isoformat(),
                            "release_time_local": release_time.strftime("%H:%M"),
                        },
                    )
                )
            release_date += timedelta(days=7)

        return events

    def _collect_gasdiesel(self, html: str, *, current: datetime, horizon: datetime) -> list[CandidateEvent]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="schedule")
        if table is None:
            raise ValueError("EIA gasoline and diesel schedule table not found")

        exceptions: dict[date, tuple[date, str]] = {}
        for row in table.select("tbody tr"):
            header = row.find("th")
            cells = row.find_all("td")
            if header is None or len(cells) < 3:
                continue
            data_for_day = parse_us_date(header.get_text(" ", strip=True))
            alternate_release_date = parse_us_date(cells[0].get_text(" ", strip=True))
            holiday = cells[2].get_text(" ", strip=True)
            exceptions[data_for_day] = (alternate_release_date, holiday)

        data_for_day = self._previous_or_same_weekday(current.date() - timedelta(days=7), 0)
        final_data_day = self._previous_or_same_weekday(horizon.date(), 0)
        events: list[CandidateEvent] = []

        while data_for_day <= final_data_day:
            release_date = data_for_day + timedelta(days=1)
            note = "Official EIA schedule publishes the Gasoline and Diesel Fuel Update around 10:00 a.m. Eastern on Tuesday."
            if data_for_day in exceptions:
                release_date, holiday = exceptions[data_for_day]
                note = f"Holiday-adjusted release schedule for {holiday}. Standard release is around 10:00 a.m. Eastern on Tuesday."

            event_date = eastern_to_utc(release_date, time(10, 0))
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name="EIA Gasoline and Diesel Fuel Update",
                        organiser=EIA_ORGANISER,
                        cadence="weekly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=GASDIESEL_SCHEDULE_URL,
                        redistribution_ok=True,
                        source_label=EIA_SOURCE_LABEL,
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"gasdiesel-{data_for_day.isoformat()}",
                        raw_payload={
                            "data_for_day": data_for_day.isoformat(),
                            "release_date": release_date.isoformat(),
                        },
                    )
                )
            data_for_day += timedelta(days=7)

        return events

    def _collect_heating_oil(self, html: str, *, current: datetime, horizon: datetime) -> list[CandidateEvent]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="schedule")
        if table is None:
            raise ValueError("EIA heating oil and propane schedule table not found")

        exceptions: dict[date, tuple[date, time, str]] = {}
        for row in table.select("tbody tr"):
            header = row.find("th")
            cells = row.find_all("td")
            if header is None or len(cells) < 4:
                continue
            data_for_day = parse_us_date(header.get_text(" ", strip=True))
            alternate_release_date = parse_us_date(cells[0].get_text(" ", strip=True))
            release_time = self._parse_eia_time(cells[2].get_text(" ", strip=True))
            holiday = cells[3].get_text(" ", strip=True)
            exceptions[data_for_day] = (alternate_release_date, release_time, holiday)

        data_for_day = self._previous_or_same_weekday(current.date() - timedelta(days=7), 0)
        final_data_day = self._previous_or_same_weekday(horizon.date(), 0)
        events: list[CandidateEvent] = []

        while data_for_day <= final_data_day:
            if data_for_day.month not in {1, 2, 3, 10, 11, 12}:
                data_for_day += timedelta(days=7)
                continue

            release_date = data_for_day + timedelta(days=2)
            release_time = time(13, 0)
            note = (
                "Official EIA schedule publishes the Heating Oil and Propane Update at 1:00 p.m. Eastern on Wednesday "
                "during the heating season."
            )
            if data_for_day in exceptions:
                release_date, release_time, holiday = exceptions[data_for_day]
                note = f"Holiday-adjusted release schedule for {holiday}. Standard release is 1:00 p.m. Eastern on Wednesday."

            event_date = eastern_to_utc(release_date, release_time)
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name="EIA Heating Oil and Propane Update",
                        organiser=EIA_ORGANISER,
                        cadence="weekly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=HEATING_OIL_SCHEDULE_URL,
                        redistribution_ok=True,
                        source_label=EIA_SOURCE_LABEL,
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"heating-oil-propane-{data_for_day.isoformat()}",
                        raw_payload={
                            "data_for_day": data_for_day.isoformat(),
                            "release_date": release_date.isoformat(),
                            "release_time_local": release_time.strftime("%H:%M"),
                        },
                    )
                )
            data_for_day += timedelta(days=7)

        return events

    def _collect_steo(self, html: str, *, current: datetime, horizon: datetime) -> list[CandidateEvent]:
        soup = BeautifulSoup(html, "html.parser")
        events: list[CandidateEvent] = []

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            edition = cells[0].get_text(" ", strip=True)
            release_text = cells[1].get_text(" ", strip=True).replace("(Wednesday)", "").strip()
            if "202" not in edition:
                continue
            try:
                release_date = parse_slash_date(release_text)
            except ValueError:
                continue
            event_date = eastern_to_utc(release_date, time(12, 0))
            if event_date < current - timedelta(days=7) or event_date > horizon:
                continue
            events.append(
                CandidateEvent(
                    name="EIA Short-Term Energy Outlook",
                    organiser=EIA_ORGANISER,
                    cadence="monthly",
                    commodity_sectors=("energy",),
                    event_date=event_date,
                    calendar_url=STEO_SCHEDULE_URL,
                    redistribution_ok=True,
                    source_label=EIA_SOURCE_LABEL,
                    notes="EIA release schedule lists this edition for noon Eastern, with occasional holiday or conference adjustments noted on the source page.",
                    is_confirmed=True,
                    source_item_key=f"steo-{edition.lower().replace(' ', '-')}",
                    raw_payload={
                        "edition": edition,
                        "release_date": release_date.isoformat(),
                    },
                )
            )
        if not events:
            raise ValueError("EIA STEO release schedule rows not found")
        return events

    def _collect_generic_weekly_report(
        self,
        report: UpcomingEiaReport,
        *,
        current: datetime,
        horizon: datetime,
    ) -> list[CandidateEvent]:
        schedule_match = re.search(
            r"([A-Za-z]+)(?:\s+approx\.)?\s+(?:at|by)\s+(\d{1,2}:\d{2}\s+[ap]\.m\.)",
            report.schedule_text,
            re.IGNORECASE,
        )
        if schedule_match is None:
            return []

        weekday = self._weekday_from_name(schedule_match.group(1))
        release_time = self._parse_eia_time(schedule_match.group(2))
        release_date = self._previous_or_same_weekday(current.date() - timedelta(days=7), weekday)
        final_release_date = self._previous_or_same_weekday(horizon.date(), weekday)
        events: list[CandidateEvent] = []

        while release_date <= final_release_date:
            event_date = eastern_to_utc(release_date, release_time)
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name=f"EIA {report.name}",
                        organiser=EIA_ORGANISER,
                        cadence="weekly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=report.url,
                        redistribution_ok=True,
                        source_label=EIA_SOURCE_LABEL,
                        notes=f"EIA upcoming reports page lists the release schedule as: {report.schedule_text}",
                        is_confirmed=True,
                        source_item_key=f"eia-{slugify(report.name)}-{release_date.isoformat()}",
                        raw_payload={
                            "schedule_text": report.schedule_text,
                            "release_date": release_date.isoformat(),
                        },
                    )
                )
            release_date += timedelta(days=7)

        return events

    def _collect_detail_page_report(
        self,
        html: str,
        *,
        report: UpcomingEiaReport,
        current: datetime,
        horizon: datetime,
    ) -> CandidateEvent | None:
        release_date = self._parse_detail_page_release_date(html)
        if release_date is None:
            return None

        release_time = self._parse_time_from_schedule_text(report.schedule_text)
        if release_time is None:
            event_date = datetime.combine(release_date, time(0, 0), tzinfo=timezone.utc)
            timing_note = "The source page confirms the release date but does not specify a publication hour."
        else:
            event_date = eastern_to_utc(release_date, release_time)
            timing_note = f"Scheduled for {release_time.strftime('%H:%M')} Eastern."

        if event_date < current - timedelta(days=7) or event_date > horizon:
            return None

        return CandidateEvent(
            name=f"EIA {report.name}",
            organiser=EIA_ORGANISER,
            cadence=report.cadence,
            commodity_sectors=("energy",),
            event_date=event_date,
            calendar_url=report.url,
            redistribution_ok=True,
            source_label=EIA_SOURCE_LABEL,
            notes=f"{timing_note} EIA upcoming reports page lists the schedule as: {report.schedule_text}",
            is_confirmed=True,
            source_item_key=f"eia-{slugify(report.name)}-{release_date.isoformat()}",
            raw_payload={
                "schedule_text": report.schedule_text,
                "release_date": release_date.isoformat(),
            },
        )

    @classmethod
    def _parse_upcoming_reports(cls, html: str) -> list[UpcomingEiaReport]:
        soup = BeautifulSoup(html, "html.parser")
        reports: list[UpcomingEiaReport] = []

        for section in soup.select("ul.l-padding-medium-bottom"):
            heading = section.find("h2")
            if heading is None:
                continue
            section_name = heading.get_text(" ", strip=True).lower()
            if section_name not in {"weekly", "monthly", "quarterly"}:
                continue

            for item in section.find_all("li", class_="list-item"):
                inner = item.find("ul")
                if inner is None:
                    continue
                link = inner.find("a", href=True)
                if link is None:
                    continue
                name = cls._normalize_whitespace(link.get_text(" ", strip=True))
                if not name:
                    continue
                schedule_node = item.find("span", class_="date")
                schedule_text = cls._normalize_whitespace(
                    schedule_node.get_text(" ", strip=True) if schedule_node is not None else ""
                )
                reports.append(
                    UpcomingEiaReport(
                        name=name,
                        url=urljoin(UPCOMING_REPORTS_URL, link.get("href", "").strip()),
                        cadence=section_name,
                        schedule_text=schedule_text,
                    )
                )

        annual_heading = soup.find("h1", string=re.compile(r"Annual", re.IGNORECASE))
        if annual_heading is not None:
            for section in annual_heading.find_all_next("ul", class_="l-padding-medium-bottom"):
                heading = section.find("h2")
                if heading is None:
                    continue
                month_label = cls._normalize_whitespace(heading.get_text(" ", strip=True))
                if not re.fullmatch(r"[A-Za-z]+ \d{4}", month_label):
                    continue
                for link in section.select("li a[href]"):
                    name = cls._normalize_whitespace(link.get_text(" ", strip=True))
                    if not name:
                        continue
                    reports.append(
                        UpcomingEiaReport(
                            name=name,
                            url=urljoin(UPCOMING_REPORTS_URL, link.get("href", "").strip()),
                            cadence="annual",
                            schedule_text=month_label,
                        )
                    )

        if not reports:
            raise ValueError("EIA upcoming reports page did not yield any release definitions")
        return reports

    @staticmethod
    def _parse_detail_page_release_date(html: str) -> date | None:
        patterns = [
            r"Next Release Date:</span>\s*<span class=\"date\">\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            r"Next Release:</strong>\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            r"Next Release Date:</strong>\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            r"Release Date:</span>\s*<span class=\"date\">\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            r"Release Date:</strong>\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match is None:
                continue
            return parse_us_date(match.group(1))
        return None

    @staticmethod
    def _parse_time_from_schedule_text(value: str) -> time | None:
        match = re.search(r"(\d{1,2}:\d{2}\s+[ap]\.m\.)", value, re.IGNORECASE)
        if match is None:
            return None
        return EiaScheduleAdapter._parse_eia_time(match.group(1))

    @staticmethod
    def _parse_eia_time(value: str) -> time:
        normalized = (
            value.strip()
            .replace("after ", "")
            .replace("around ", "")
            .replace("approx. ", "")
        )
        return parse_us_time(normalized)

    @staticmethod
    def _previous_or_same_weekday(day: date, weekday: int) -> date:
        offset = (day.weekday() - weekday) % 7
        return day - timedelta(days=offset)

    @staticmethod
    def _weekday_from_name(name: str) -> int:
        return {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
        }[name.strip().lower()]

    @staticmethod
    def _normalize_whitespace(value: str) -> str:
        return " ".join(value.split())
