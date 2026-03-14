from __future__ import annotations

import re
from datetime import datetime, time, timedelta

from bs4 import BeautifulSoup

from ..time import frankfurt_to_utc, parse_day_month_year
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


ECB_URL = "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"


class EcbMeetingCalendarAdapter(HtmlCalendarAdapter):
    slug = "ecb"
    primary_url = ECB_URL

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        html = client.get(ECB_URL).text
        soup = BeautifulSoup(html, "html.parser")
        lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]

        events: list[CandidateEvent] = []
        for index, line in enumerate(lines[:-1]):
            if not re.fullmatch(r"\d{2}/\d{2}/\d{4}", line):
                continue
            description = lines[index + 1]
            if "monetary policy meeting" not in description.lower() or "day 2" not in description.lower():
                continue
            event_day = parse_day_month_year(line)
            event_date = frankfurt_to_utc(event_day, time(14, 15))
            if event_date < current - timedelta(days=7):
                continue
            events.append(
                CandidateEvent(
                    name="ECB Monetary Policy Decision",
                    organiser="European Central Bank Governing Council",
                    cadence="ad_hoc",
                    commodity_sectors=("macro",),
                    event_date=event_date,
                    calendar_url=ECB_URL,
                    redistribution_ok=True,
                    source_label="ECB",
                    notes="ECB monetary policy decisions are normally published at 14:15 local Frankfurt time on Day 2 of the meeting.",
                    is_confirmed=True,
                    source_item_key=f"ecb-mopo-{event_day.isoformat()}",
                    raw_payload={
                        "description": description,
                        "raw_date": line,
                    },
                )
            )
        if not events:
            raise ValueError("ECB monetary policy meetings were not found")
        return events
