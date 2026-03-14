from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta

from bs4 import BeautifulSoup

from ..time import central_to_utc, nearest_weekday
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


BAKER_HUGHES_URL = "https://rigcount.bakerhughes.com/rig-count-overview"


class BakerHughesRigCountAdapter(HtmlCalendarAdapter):
    slug = "baker_hughes"
    primary_url = BAKER_HUGHES_URL

    def __init__(self, *, horizon_days: int = 370):
        self.horizon_days = horizon_days

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        html = client.get(BAKER_HUGHES_URL, user_agent="").text
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        exceptions = self._parse_exceptions(text)
        events: list[CandidateEvent] = []

        start_nominal = self._previous_or_same_friday(current.date() - timedelta(days=7))
        final_nominal = self._previous_or_same_friday((current + timedelta(days=self.horizon_days)).date())
        nominal_date = start_nominal

        while nominal_date <= final_nominal:
            release_date = nominal_date
            note = "Baker Hughes states the North America rig count is released weekly at noon U.S. Central on the last workday of the week."
            if nominal_date in exceptions:
                release_date, holiday_note = exceptions[nominal_date]
                note = holiday_note

            event_date = central_to_utc(release_date, time(12, 0))
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name="Baker Hughes North America Rig Count",
                        organiser="Baker Hughes",
                        cadence="weekly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=BAKER_HUGHES_URL,
                        redistribution_ok=True,
                        source_label="Baker Hughes",
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"baker-hughes-rig-count-{nominal_date.isoformat()}",
                        raw_payload={
                            "nominal_release_date": nominal_date.isoformat(),
                            "actual_release_date": release_date.isoformat(),
                        },
                    )
                )
            nominal_date += timedelta(days=7)

        first_month = self._month_start(current.date() - timedelta(days=31))
        final_month = self._month_start((current + timedelta(days=self.horizon_days)).date())
        month_cursor = first_month
        while month_cursor <= final_month:
            nominal_date = self._international_release_day(month_cursor)
            release_date = nominal_date
            note = (
                "Baker Hughes states the international rig count is released monthly at noon U.S. Central "
                "on the last working day of the first week of the month."
            )
            if nominal_date in exceptions:
                release_date, holiday_note = exceptions[nominal_date]
                note = holiday_note.replace("rig count", "international rig count")

            event_date = central_to_utc(release_date, time(12, 0))
            if event_date >= current - timedelta(days=7):
                events.append(
                    CandidateEvent(
                        name="Baker Hughes International Rig Count",
                        organiser="Baker Hughes",
                        cadence="monthly",
                        commodity_sectors=("energy",),
                        event_date=event_date,
                        calendar_url=BAKER_HUGHES_URL,
                        redistribution_ok=True,
                        source_label="Baker Hughes",
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"baker-hughes-international-rig-count-{month_cursor.year}-{month_cursor.month:02d}",
                        raw_payload={
                            "nominal_release_date": nominal_date.isoformat(),
                            "actual_release_date": release_date.isoformat(),
                        },
                    )
                )
            month_cursor = self._next_month(month_cursor)

        return events

    @staticmethod
    def _parse_exceptions(page_text: str) -> dict[date, tuple[date, str]]:
        exceptions: dict[date, tuple[date, str]] = {}
        pattern = re.compile(
            r"updated publication date is ([A-Za-z]+), ([A-Za-z]+) (\d{1,2})(?:st|nd|rd|th)? (\d{4})",
            re.IGNORECASE,
        )
        for match in pattern.finditer(page_text):
            weekday_name, month_name, day_number, year = match.groups()
            release_date = datetime.strptime(
                f"{month_name} {day_number} {year}",
                "%B %d %Y",
            ).date()
            nominal_date = nearest_weekday(release_date, 4, max_distance=1)
            if nominal_date is None:
                continue
            note = (
                f"Holiday-adjusted Baker Hughes rig count. The source page notes an updated publication date of "
                f"{weekday_name}, {month_name} {int(day_number)}, {year}."
            )
            exceptions[nominal_date] = (release_date, note)
        return exceptions

    @staticmethod
    def _previous_or_same_friday(day: date) -> date:
        return day - timedelta(days=(day.weekday() - 4) % 7)

    @staticmethod
    def _international_release_day(day: date) -> date:
        candidates = [
            date(day.year, day.month, day_number)
            for day_number in range(1, 8)
            if date(day.year, day.month, day_number).weekday() < 5
        ]
        return candidates[-1]

    @staticmethod
    def _month_start(day: date) -> date:
        return day.replace(day=1)

    @staticmethod
    def _next_month(day: date) -> date:
        if day.month == 12:
            return date(day.year + 1, 1, 1)
        return date(day.year, day.month + 1, 1)
