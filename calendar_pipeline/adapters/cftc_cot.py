from __future__ import annotations

import re
from datetime import datetime, time, timedelta

from bs4 import BeautifulSoup

from ..time import eastern_to_utc
from ..types import CandidateEvent, utc_now
from .base import HtmlCalendarAdapter


CFTC_URL = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm"


class CftcCotScheduleAdapter(HtmlCalendarAdapter):
    slug = "cftc_cot"
    primary_url = CFTC_URL

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        html = client.get(CFTC_URL).text
        soup = BeautifulSoup(html, "html.parser")
        events: list[CandidateEvent] = []

        for heading in soup.find_all(["h2", "h3", "h4"]):
            match = re.search(r"(\d{4}) Release Schedule", heading.get_text(" ", strip=True))
            if match is None:
                continue
            year = int(match.group(1))
            table = heading.find_next("table")
            if table is None:
                continue
            for row in table.find_all("tr"):
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
                if len(cells) < 2:
                    continue
                month_name = cells[0]
                if not month_name or month_name.lower() == "month":
                    continue
                for raw_day in cells[1:]:
                    delayed = "*" in raw_day
                    cleaned = raw_day.replace("*", "").strip()
                    if not cleaned or cleaned == "\xa0":
                        continue
                    event_day = datetime.strptime(f"{month_name} {int(cleaned)} {year}", "%B %d %Y").date()
                    event_date = eastern_to_utc(event_day, time(15, 30))
                    if event_date < current - timedelta(days=7):
                        continue
                    events.append(
                        CandidateEvent(
                            name="CFTC Commitments of Traders Report",
                            organiser="U.S. Commodity Futures Trading Commission",
                            cadence="weekly",
                            commodity_sectors=("cross-commodity",),
                            event_date=event_date,
                            calendar_url=CFTC_URL,
                            redistribution_ok=True,
                            source_label="CFTC",
                            notes=(
                                "CFTC states the Commitments of Traders report is released at 3:30 p.m. Eastern "
                                "and usually contains data from the previous Tuesday."
                                if not delayed
                                else "Delayed CFTC Commitments of Traders release due to a federal holiday."
                            ),
                            is_confirmed=True,
                            source_item_key=f"cftc-cot-{event_day.isoformat()}",
                            raw_payload={
                                "release_year": year,
                                "release_month": month_name,
                                "release_day": int(cleaned),
                                "delayed": delayed,
                            },
                        )
                    )

        if not events:
            raise ValueError("CFTC release schedule rows were not found")
        return events
