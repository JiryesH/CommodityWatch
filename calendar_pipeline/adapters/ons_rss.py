from __future__ import annotations

from datetime import datetime, timedelta

import feedparser

from ..time import parse_email_datetime
from ..types import CandidateEvent, utc_now
from .base import StructuredFeedAdapter


ONS_UPCOMING_RSS_URL = (
    "https://www.ons.gov.uk/releasecalendar"
    "?rss&highlight=true&limit=100&page={page}&release-type=type-upcoming&sort=date-newest"
)

TRACKED_RELEASES = [
    ("consumer price inflation", "ONS UK CPI Release", "monthly"),
    ("retail sales", "ONS UK Retail Sales", "monthly"),
    ("uk trade", "ONS UK Trade", "monthly"),
    ("gdp first quarterly estimate", "ONS UK GDP First Estimate", "quarterly"),
    ("gdp quarterly national accounts", "ONS UK GDP Quarterly National Accounts", "quarterly"),
    ("gdp monthly estimate", "ONS UK GDP Monthly Estimate", "monthly"),
    ("gross domestic product", "ONS UK GDP Release", "monthly"),
    ("uk labour market", "ONS UK Labour Market", "monthly"),
    ("labour market overview", "ONS UK Labour Market", "monthly"),
    ("producer price inflation", "ONS UK Producer Price Inflation", "monthly"),
    ("index of production", "ONS UK Industrial Production", "monthly"),
    ("index of services", "ONS UK Services Index", "monthly"),
    ("construction output", "ONS UK Construction Output", "monthly"),
    ("public sector finances", "ONS UK Public Sector Finances", "monthly"),
    ("balance of payments", "ONS UK Balance of Payments", "quarterly"),
    ("business investment", "ONS UK Business Investment", "quarterly"),
]


class OnsReleaseCalendarAdapter(StructuredFeedAdapter):
    slug = "ons_rss"
    primary_url = ONS_UPCOMING_RSS_URL.format(page=1)

    def __init__(self, *, max_pages: int = 6):
        self.max_pages = max_pages

    def collect(self, client, *, as_of: datetime | None = None) -> list[CandidateEvent]:
        current = as_of or utc_now()
        seen_links: set[str] = set()
        events: list[CandidateEvent] = []

        for page in range(1, self.max_pages + 1):
            feed = feedparser.parse(client.get(ONS_UPCOMING_RSS_URL.format(page=page)).body)
            if not feed.entries:
                break

            matched_on_page = 0
            for entry in feed.entries:
                link = str(entry.get("link", "")).strip()
                title = str(entry.get("title", "")).strip()
                if not link or not title or link in seen_links:
                    continue
                seen_links.add(link)

                match = self._match_release(title)
                if match is None or "time series" in title.lower():
                    continue

                event_date = parse_email_datetime(str(entry.get("published", "")))
                if event_date < current - timedelta(days=7):
                    continue
                matched_on_page += 1
                display_name, cadence = match
                notes = title if title != display_name else None
                events.append(
                    CandidateEvent(
                        name=display_name,
                        organiser="Office for National Statistics",
                        cadence=cadence,
                        commodity_sectors=("macro",),
                        event_date=event_date,
                        calendar_url=link,
                        redistribution_ok=True,
                        source_label="ONS",
                        notes=notes,
                        is_confirmed=True,
                        source_item_key=str(entry.get("guid", link)),
                        raw_payload={
                            "title": title,
                            "summary": str(entry.get("summary", "")),
                        },
                    )
                )
            if matched_on_page == 0:
                break

        return events

    @staticmethod
    def _match_release(title: str) -> tuple[str, str] | None:
        normalized = title.lower()
        for keyword, display_name, cadence in TRACKED_RELEASES:
            if keyword in normalized:
                return display_name, cadence
        return None
