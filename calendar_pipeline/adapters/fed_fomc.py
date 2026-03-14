from __future__ import annotations

import re
from datetime import datetime, time, timedelta

from bs4 import BeautifulSoup

from ..time import eastern_to_utc
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"


class FedFomcCalendarAdapter(HtmlCalendarAdapter):
    slug = "fed_fomc"
    primary_url = FOMC_URL

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        html = client.get(FOMC_URL).text
        soup = BeautifulSoup(html, "html.parser")

        events: list[CandidateEvent] = []
        for panel in soup.select("div.panel.panel-default"):
            heading = panel.get_text(" ", strip=True)
            match = re.search(r"(\d{4}) FOMC Meetings", heading)
            if match is None:
                continue

            year = int(match.group(1))
            for row in panel.select("div.row"):
                month_node = row.select_one(".fomc-meeting__month")
                date_node = row.select_one(".fomc-meeting__date")
                if month_node is None or date_node is None:
                    continue
                month_name = month_node.get_text(" ", strip=True)
                raw_date_range = date_node.get_text(" ", strip=True)
                event_day = self._second_day_of_meeting(year, month_name, raw_date_range)
                if event_day is None:
                    continue
                event_date = eastern_to_utc(event_day, time(14, 0))
                if event_date < current - timedelta(days=7):
                    continue
                press_conference = "*" in raw_date_range
                note = "FOMC statement is customarily released at 2:00 p.m. Eastern on the second day of the scheduled meeting."
                if press_conference:
                    note = f"{note} Fed calendar marks this meeting with an asterisk for the scheduled press conference cycle."
                events.append(
                    CandidateEvent(
                        name="Federal Reserve FOMC Decision",
                        organiser="Board of Governors of the Federal Reserve System",
                        cadence="ad_hoc",
                        commodity_sectors=("macro",),
                        event_date=event_date,
                        calendar_url=FOMC_URL,
                        redistribution_ok=True,
                        source_label="Federal Reserve",
                        notes=note,
                        is_confirmed=True,
                        source_item_key=f"fomc-{year}-{event_day.month:02d}",
                        raw_payload={
                            "meeting_year": year,
                            "meeting_month": month_name,
                            "raw_date_range": raw_date_range,
                        },
                    )
                )
        if not events:
            raise ValueError("Fed FOMC meetings were not found")
        return events

    @staticmethod
    def _second_day_of_meeting(year: int, month_name: str, raw_date_range: str):
        cleaned = raw_date_range.replace("*", "").strip()
        if "notation vote" in cleaned.lower():
            return None

        day_values = [int(value) for value in re.findall(r"\d+", cleaned)]
        if not day_values:
            return None

        month_value = month_name.strip()
        if "/" in month_value and len(day_values) >= 2:
            month_value = month_value.split("/", 1)[1].strip()

        month_number = FedFomcCalendarAdapter._parse_month_number(month_value)
        if month_number is None:
            return None

        return datetime(year=year, month=month_number, day=day_values[-1]).date()

    @staticmethod
    def _parse_month_number(value: str) -> int | None:
        normalized = value.replace(".", "").strip()
        for fmt in ("%B", "%b"):
            try:
                return datetime.strptime(normalized, fmt).month
            except ValueError:
                continue
        return None
