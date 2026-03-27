from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from calendar_pipeline.adapters.baker_hughes import BAKER_HUGHES_URL, BakerHughesRigCountAdapter
from calendar_pipeline.adapters.bls import BLS_SCHEDULE_URL_TEMPLATE, BlsScheduleAdapter
from calendar_pipeline.adapters.cftc_cot import CFTC_URL, CftcCotScheduleAdapter
from calendar_pipeline.adapters.ecb import ECB_URL, EcbMeetingCalendarAdapter
from calendar_pipeline.adapters.eia import (
    GASDIESEL_SCHEDULE_URL,
    HEATING_OIL_SCHEDULE_URL,
    NGS_URL,
    STEO_SCHEDULE_URL,
    UPCOMING_REPORTS_URL,
    WPSR_URL,
    EiaScheduleAdapter,
)
from calendar_pipeline.adapters.eurostat import EUROSTAT_ICAL_URL, EurostatReleaseCalendarAdapter
from calendar_pipeline.adapters.fed_fomc import FOMC_URL, FedFomcCalendarAdapter
from calendar_pipeline.adapters.ons_rss import ONS_UPCOMING_RSS_URL, OnsReleaseCalendarAdapter
from calendar_pipeline.adapters.usda_nass import NASS_BASE_URL, UsdaNassCalendarAdapter
import calendar_pipeline.http as http
from calendar_pipeline.http import BROWSER_USER_AGENT, HttpFetchError
from calendar_pipeline.storage import CalendarRepository, calendar_events, create_calendar_engine
from calendar_pipeline.types import CandidateEvent


@dataclass(frozen=True)
class FakeResponse:
    url: str
    body: bytes
    status_code: int = 200

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


class FakeClient:
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping

    def get(self, url: str, **_: object):
        if url not in self.mapping:
            raise AssertionError(f"Unexpected URL requested in test: {url}")
        payload = self.mapping[url]
        if isinstance(payload, Exception):
            raise payload
        return FakeResponse(url=url, body=payload.encode("utf-8"))


class RecordingClient:
    def __init__(self, mapping: dict[str, str | Exception]):
        self.mapping = mapping
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs: object):
        self.calls.append((url, kwargs))
        if url not in self.mapping:
            raise AssertionError(f"Unexpected URL requested in test: {url}")
        payload = self.mapping[url]
        if isinstance(payload, Exception):
            raise payload
        return FakeResponse(url=url, body=payload.encode("utf-8"))


def test_repository_blocks_unconfirmed_redistribution(tmp_path: Path) -> None:
    database_path = tmp_path / "calendarwatch.db"
    repository = CalendarRepository(create_calendar_engine(f"sqlite:///{database_path}"))
    repository.ensure_schema()

    candidate = CandidateEvent(
        name="ONS UK CPI Release",
        organiser="Office for National Statistics",
        cadence="monthly",
        commodity_sectors=("macro",),
        event_date=datetime(2026, 3, 25, 7, 0, tzinfo=timezone.utc),
        calendar_url="https://www.ons.gov.uk/releases/consumerpriceinflationukfebruary2026",
        redistribution_ok=False,
        source_label="ONS",
        notes=None,
        is_confirmed=True,
        source_item_key="ons-cpi-2026-03",
        raw_payload={"fixture": True},
    )

    repository.upsert_events(
        source_slug="ons_rss",
        ingestion_pattern="structured_feed",
        candidates=[candidate],
        detected_at=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc),
    )

    assert repository.list_events(from_date=None, to_date=None) == []

    with repository.engine.begin() as connection:
        row = connection.execute(select(calendar_events)).mappings().first()

    assert row is not None
    assert row["publish_status"] == "pending_review"
    assert "redistribution_unconfirmed" in row["review_reasons"]


def test_repository_flags_date_change_for_previously_published_event(tmp_path: Path) -> None:
    database_path = tmp_path / "calendarwatch.db"
    repository = CalendarRepository(create_calendar_engine(f"sqlite:///{database_path}"))
    repository.ensure_schema()

    first_candidate = CandidateEvent(
        name="Federal Reserve FOMC Decision",
        organiser="Board of Governors of the Federal Reserve System",
        cadence="ad_hoc",
        commodity_sectors=("macro",),
        event_date=datetime(2026, 3, 18, 18, 0, tzinfo=timezone.utc),
        calendar_url=FOMC_URL,
        redistribution_ok=True,
        source_label="Federal Reserve",
        notes=None,
        is_confirmed=True,
        source_item_key="fomc-2026-03",
        raw_payload={"raw_date_range": "17-18"},
    )
    repository.upsert_events(
        source_slug="fed_fomc",
        ingestion_pattern="html",
        candidates=[first_candidate],
        detected_at=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc),
    )

    moved_candidate = CandidateEvent(
        name="Federal Reserve FOMC Decision",
        organiser="Board of Governors of the Federal Reserve System",
        cadence="ad_hoc",
        commodity_sectors=("macro",),
        event_date=datetime(2026, 3, 19, 18, 0, tzinfo=timezone.utc),
        calendar_url=FOMC_URL,
        redistribution_ok=True,
        source_label="Federal Reserve",
        notes=None,
        is_confirmed=True,
        source_item_key="fomc-2026-03",
        raw_payload={"raw_date_range": "18-19"},
    )
    stats = repository.upsert_events(
        source_slug="fed_fomc",
        ingestion_pattern="html",
        candidates=[moved_candidate],
        detected_at=datetime(2026, 3, 13, 0, 0, tzinfo=timezone.utc),
    )

    assert stats["flagged"] == 1
    assert repository.list_events(from_date=None, to_date=None) == []

    with repository.engine.begin() as connection:
        row = connection.execute(select(calendar_events)).mappings().first()

    assert row is not None
    assert row["publish_status"] == "pending_review"
    assert "date_changed" in row["review_reasons"]


def test_repository_keeps_same_day_time_correction_published(tmp_path: Path) -> None:
    database_path = tmp_path / "calendarwatch.db"
    repository = CalendarRepository(create_calendar_engine(f"sqlite:///{database_path}"))
    repository.ensure_schema()

    first_candidate = CandidateEvent(
        name="ECB Monetary Policy Decision",
        organiser="European Central Bank Governing Council",
        cadence="ad_hoc",
        commodity_sectors=("macro",),
        event_date=datetime(2026, 3, 19, 9, 15, tzinfo=timezone.utc),
        calendar_url=ECB_URL,
        redistribution_ok=True,
        source_label="ECB",
        notes=None,
        is_confirmed=True,
        source_item_key="ecb-mopo-2026-03-19",
        raw_payload={"raw_date": "19/03/2026"},
    )
    repository.upsert_events(
        source_slug="ecb",
        ingestion_pattern="html",
        candidates=[first_candidate],
        detected_at=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc),
    )

    corrected_candidate = CandidateEvent(
        name="ECB Monetary Policy Decision",
        organiser="European Central Bank Governing Council",
        cadence="ad_hoc",
        commodity_sectors=("macro",),
        event_date=datetime(2026, 3, 19, 13, 15, tzinfo=timezone.utc),
        calendar_url=ECB_URL,
        redistribution_ok=True,
        source_label="ECB",
        notes=None,
        is_confirmed=True,
        source_item_key="ecb-mopo-2026-03-19",
        raw_payload={"raw_date": "19/03/2026"},
    )
    stats = repository.upsert_events(
        source_slug="ecb",
        ingestion_pattern="html",
        candidates=[corrected_candidate],
        detected_at=datetime(2026, 3, 13, 0, 0, tzinfo=timezone.utc),
    )

    assert stats["flagged"] == 0
    events = repository.list_events(from_date=None, to_date=None)
    assert len(events) == 1
    assert events[0]["event_date"] == "2026-03-19T13:15:00+00:00"


def test_eia_adapter_extracts_petroleum_storage_and_steo() -> None:
    adapter = EiaScheduleAdapter(horizon_days=45)
    client = FakeClient(
        {
            WPSR_URL: """
                <table class="basic-table schedule">
                  <tbody>
                    <tr>
                      <th>March 13, 2026</th>
                      <td>March 19, 2026</td>
                      <td>Thursday</td>
                      <td>12:00 p.m.</td>
                      <td>Holiday shift</td>
                    </tr>
                  </tbody>
                </table>
            """,
            NGS_URL: """
                <table>
                  <tr>
                    <td>November 13, 2026</td>
                    <td>Friday</td>
                    <td>10:30 a.m.</td>
                    <td>Veterans Day</td>
                  </tr>
                </table>
            """,
            GASDIESEL_SCHEDULE_URL: """
                <table class="basic-table schedule">
                  <tbody>
                    <tr>
                      <th>May 25, 2026</th>
                      <td>May 27, 2026</td>
                      <td>Wednesday</td>
                      <td>Memorial Day</td>
                    </tr>
                  </tbody>
                </table>
            """,
            HEATING_OIL_SCHEDULE_URL: """
                <table class="basic-table schedule">
                  <tbody>
                    <tr>
                      <th>January 19, 2026</th>
                      <td>January 22, 2026</td>
                      <td>Thursday</td>
                      <td>2:00 p.m.</td>
                      <td>Martin Luther King Jr. Day</td>
                    </tr>
                  </tbody>
                </table>
            """,
            STEO_SCHEDULE_URL: """
                <table>
                  <tr><td>April 2026</td><td>04/07/2026</td></tr>
                </table>
            """,
            UPCOMING_REPORTS_URL: """
                <ul class="l-padding-medium-bottom">
                  <li class="head"><h2 id="weekly">Weekly</h2></li>
                  <li class="list-item l-padding-bottom">
                    <ul>
                      <li class="sub-head"><a href="/coal/production/weekly/">Weekly Coal Production</a></li>
                      <li class="hide-bullet"><span class="release-date">Release schedule:</span> <span class="date">Thursday by 5:00 p.m. EST</span></li>
                    </ul>
                  </li>
                </ul>
                <ul class="l-padding-medium-bottom">
                  <li class="head"><h2 id="monthly">Monthly</h2></li>
                  <li class="list-item l-padding-bottom">
                    <ul>
                      <li class="sub-head"><a href="/electricity/monthly/">Electric Power Monthly</a></li>
                      <li class="hide-bullet"><span class="release-date">Release schedule:</span> <span class="date">the last week of the month</span></li>
                    </ul>
                  </li>
                </ul>
            """,
            "https://www.eia.gov/coal/production/weekly/": """
                <html><body><h1>Weekly Coal Production</h1></body></html>
            """,
            "https://www.eia.gov/electricity/monthly/": """
                <div class="release-dates">
                  <span class="label">Release Date:</span> <span class="date">February 24, 2026</span>
                  <span class="label">Next Release Date:</span> <span class="date">March 24, 2026</span>
                </div>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc))
    names = {event.name for event in events}

    assert "EIA Weekly Petroleum Status Report" in names
    assert "EIA Weekly Natural Gas Storage Report" in names
    assert "EIA Short-Term Energy Outlook" in names
    assert "EIA Weekly Coal Production" in names
    assert "EIA Electric Power Monthly" in names


def test_usda_nass_adapter_collects_all_calendar_releases() -> None:
    adapter = UsdaNassCalendarAdapter(months_ahead=0)
    client = FakeClient(
        {
            f"{NASS_BASE_URL}?view=l&js=1&month=03&year=2026": """
                <table class="calendar">
                  <tr><th>Date</th><th>Time</th><th>Release</th><th>Status</th></tr>
                  <tr>
                    <td>Tue, 03/31/26</td>
                    <td>12:00 pm ET</td>
                    <td><a href="./calendar-landing.php?report_id=11004">Grain Stocks</a></td>
                    <td>Report Pending ...</td>
                  </tr>
                  <tr>
                    <td></td>
                    <td>12:00 pm ET</td>
                    <td><a href="./calendar-landing.php?report_id=11003">Prospective Plantings</a></td>
                    <td>Report Pending ...</td>
                  </tr>
                  <tr>
                    <td>Fri, 03/27/26</td>
                    <td>3:00 pm ET</td>
                    <td><a href="./calendar-landing.php?report_id=99999">Untracked Release</a></td>
                    <td>Report Pending ...</td>
                  </tr>
                </table>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == [
        "USDA Grain Stocks",
        "USDA Prospective Plantings",
        "USDA Untracked Release",
    ]


def test_usda_nass_adapter_skips_unpublished_future_months() -> None:
    adapter = UsdaNassCalendarAdapter(months_ahead=1)
    client = FakeClient(
        {
            f"{NASS_BASE_URL}?view=l&js=1&month=03&year=2026": """
                <table class="calendar">
                  <tr><th>Date</th><th>Time</th><th>Release</th><th>Status</th></tr>
                  <tr>
                    <td>Tue, 03/31/26</td>
                    <td>12:00 pm ET</td>
                    <td><a href="./calendar-landing.php?report_id=11004">Grain Stocks</a></td>
                    <td>Report Pending ...</td>
                  </tr>
                </table>
            """,
            f"{NASS_BASE_URL}?view=l&js=1&month=04&year=2026": """
                <div id="calendar-container" class="col-xs-12 clearfix"></div>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == ["USDA Grain Stocks"]


def test_fed_fomc_adapter_uses_second_day_of_meeting() -> None:
    adapter = FedFomcCalendarAdapter()
    client = FakeClient(
        {
            FOMC_URL: """
                <div class="panel panel-default">
                  <div class="panel-heading"><h4>2026 FOMC Meetings</h4></div>
                  <div class="row fomc-meeting">
                    <div class="fomc-meeting__month"><strong>March</strong></div>
                    <div class="fomc-meeting__date">17-18*</div>
                  </div>
                </div>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert len(events) == 1
    assert events[0].event_date.isoformat() == "2026-03-18T18:00:00+00:00"


def test_fed_fomc_adapter_skips_notation_votes_and_handles_cross_month_meetings() -> None:
    adapter = FedFomcCalendarAdapter()
    client = FakeClient(
        {
            FOMC_URL: """
                <div class="panel panel-default">
                  <div class="panel-heading"><h4>2024 FOMC Meetings</h4></div>
                  <div class="row fomc-meeting">
                    <div class="fomc-meeting__month"><strong>Apr/May</strong></div>
                    <div class="fomc-meeting__date">30-1</div>
                  </div>
                  <div class="row fomc-meeting">
                    <div class="fomc-meeting__month"><strong>August</strong></div>
                    <div class="fomc-meeting__date">22 (notation vote)</div>
                  </div>
                </div>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc))

    assert len(events) == 1
    assert events[0].event_date.isoformat() == "2024-05-01T18:00:00+00:00"


def test_ecb_adapter_extracts_day_two_monetary_policy_meeting() -> None:
    adapter = EcbMeetingCalendarAdapter()
    client = FakeClient(
        {
            ECB_URL: """
                <html>
                  <body>
                    <div>18/03/2026</div>
                    <div>Governing Council of the ECB: monetary policy meeting in Frankfurt (Day 1)</div>
                    <div>19/03/2026</div>
                    <div>Governing Council of the ECB: monetary policy meeting in Frankfurt (Day 2), followed by press conference</div>
                  </body>
                </html>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == ["ECB Monetary Policy Decision"]


def test_ons_adapter_filters_to_macro_releases() -> None:
    adapter = OnsReleaseCalendarAdapter(max_pages=1)
    client = FakeClient(
        {
            ONS_UPCOMING_RSS_URL.format(page=1): """
                <?xml version="1.0" encoding="UTF-8"?>
                <rss version="2.0">
                  <channel>
                    <item>
                      <title>Consumer price inflation, UK: February 2026</title>
                      <link>https://www.ons.gov.uk/releases/consumerpriceinflationukfebruary2026</link>
                      <guid>ons-cpi-2026-03</guid>
                      <pubDate>Wed, 25 Mar 2026 07:00:00 +0000</pubDate>
                    </item>
                    <item>
                      <title>Health survey technical note</title>
                      <link>https://www.ons.gov.uk/releases/healthsurvey</link>
                      <guid>health-note</guid>
                      <pubDate>Wed, 25 Mar 2026 09:30:00 +0000</pubDate>
                    </item>
                  </channel>
                </rss>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == ["ONS UK CPI Release"]


def test_ons_adapter_matches_gdp_and_labour_market_titles() -> None:
    adapter = OnsReleaseCalendarAdapter(max_pages=1)
    client = FakeClient(
        {
            ONS_UPCOMING_RSS_URL.format(page=1): """
                <?xml version="1.0" encoding="UTF-8"?>
                <rss version="2.0">
                  <channel>
                    <item>
                      <title>GDP monthly estimate, UK: February 2026</title>
                      <link>https://www.ons.gov.uk/releases/gdpmonthlyestimateukfebruary2026</link>
                      <guid>ons-gdp-2026-04</guid>
                      <pubDate>Thu, 16 Apr 2026 06:00:00 +0000</pubDate>
                    </item>
                    <item>
                      <title>UK Labour Market April 2026</title>
                      <link>https://www.ons.gov.uk/releases/uklabourmarketapril2026</link>
                      <guid>ons-labour-2026-04</guid>
                      <pubDate>Tue, 21 Apr 2026 06:00:00 +0000</pubDate>
                    </item>
                  </channel>
                </rss>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == [
        "ONS UK GDP Monthly Estimate",
        "ONS UK Labour Market",
    ]


def test_eurostat_adapter_extracts_data_release_events() -> None:
    adapter = EurostatReleaseCalendarAdapter(horizon_days=30)
    client = FakeClient(
        {
            EUROSTAT_ICAL_URL: """
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260316
SUMMARY:Monthly energy
X-THEME:environment
X-CATEGORY:Data release
UID:eurostat-monthly-energy-20260316
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260318
SUMMARY:Unemployment
X-THEME:population
X-CATEGORY:Data release,Euro indicators release
UID:eurostat-unemployment-20260318
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260320
SUMMARY:Update of release calendar
X-THEME:general
X-CATEGORY:News article
UID:eurostat-ignore-20260320
END:VEVENT
END:VCALENDAR
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == [
        "Eurostat Monthly energy",
        "Eurostat Unemployment",
    ]


def test_baker_hughes_adapter_applies_exception_notice() -> None:
    adapter = BakerHughesRigCountAdapter(horizon_days=30)
    client = FakeClient(
        {
            BAKER_HUGHES_URL: """
                <html>
                  <body>
                    <p>Please be advised that, due to the Good Friday holiday occurring on April 3rd 2026, this weeks North America and International Rig Counts will be issued ahead of schedule. The updated publication date is Thursday, April 2nd 2026.</p>
                  </body>
                </html>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 25, 0, 0, tzinfo=timezone.utc))
    assert any(event.event_date.isoformat() == "2026-04-02T17:00:00+00:00" for event in events)
    assert any(event.name == "Baker Hughes International Rig Count" for event in events)


def test_baker_hughes_adapter_requests_without_custom_user_agent() -> None:
    adapter = BakerHughesRigCountAdapter(horizon_days=0)
    client = RecordingClient(
        {
            BAKER_HUGHES_URL: """
                <html>
                  <body>
                    <p>The North American rig count is released weekly at noon Central Time on the last day of the work week.</p>
                  </body>
                </html>
            """,
        }
    )

    adapter.collect(client, as_of=datetime(2026, 3, 25, 0, 0, tzinfo=timezone.utc))

    assert client.calls[0][1]["user_agent"] == ""


def test_bls_adapter_uses_browser_user_agent_and_stays_on_current_year_when_not_needed() -> None:
    adapter = BlsScheduleAdapter()
    client = RecordingClient(
        {
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2026): """
                <table>
                  <tr>
                    <td>Friday, April 10, 2026</td>
                    <td>08:30 AM</td>
                    <td><strong>Consumer Price Index</strong> for March 2026</td>
                  </tr>
                </table>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc))

    assert [event.name for event in events] == ["BLS Consumer Price Index"]
    assert client.calls == [
        (
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2026),
            {"user_agent": BROWSER_USER_AGENT},
        )
    ]


def test_bls_adapter_ignores_missing_future_year_page_when_cross_year_fetch_is_needed() -> None:
    adapter = BlsScheduleAdapter()
    client = RecordingClient(
        {
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2026): """
                <table>
                  <tr>
                    <td>Friday, December 4, 2026</td>
                    <td>08:30 AM</td>
                    <td><strong>Employment Situation</strong> for November 2026</td>
                  </tr>
                </table>
            """,
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2027): HttpFetchError(
                f"HTTP 404 for {BLS_SCHEDULE_URL_TEMPLATE.format(year=2027)}"
            ),
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 11, 15, 0, 0, tzinfo=timezone.utc))

    assert [event.name for event in events] == ["BLS Employment Situation"]
    assert client.calls == [
        (
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2026),
            {"user_agent": BROWSER_USER_AGENT},
        ),
        (
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2027),
            {"user_agent": BROWSER_USER_AGENT},
        ),
    ]


def test_cftc_adapter_parses_release_schedule() -> None:
    adapter = CftcCotScheduleAdapter()
    client = FakeClient(
        {
            CFTC_URL: """
                <h3><strong>2026 Release Schedule</strong></h3>
                <table>
                  <tr><td>Month</td><td>Dates</td><td>Dates</td></tr>
                  <tr><td>March</td><td>13</td><td>20</td></tr>
                </table>
            """,
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.event_date.isoformat() for event in events] == [
        "2026-03-13T19:30:00+00:00",
        "2026-03-20T19:30:00+00:00",
    ]


def test_bls_adapter_parses_tabular_schedule() -> None:
    adapter = BlsScheduleAdapter()
    client = FakeClient(
        {
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2026): """
                <table>
                  <tr><th>Date</th><th>Time</th><th>Release</th><th>Reference</th></tr>
                  <tr>
                    <td>Wednesday, March 11, 2026</td>
                    <td>08:30 AM</td>
                    <td><a href="/schedule/news_release/cpi.htm">Consumer Price Index</a></td>
                    <td>February 2026</td>
                  </tr>
                </table>
            """,
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2027): "<html><body></body></html>",
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == ["BLS Consumer Price Index"]


def test_bls_adapter_includes_state_employment_and_unemployment() -> None:
    adapter = BlsScheduleAdapter()
    client = FakeClient(
        {
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2026): """
                <table>
                  <tr>
                    <td>Friday, March 27, 2026</td>
                    <td>10:00 AM</td>
                    <td><a href="/schedule/news_release/laus.htm">State Employment and Unemployment (Monthly)</a></td>
                    <td>February 2026</td>
                  </tr>
                </table>
            """,
            BLS_SCHEDULE_URL_TEMPLATE.format(year=2027): "<html><body></body></html>",
        }
    )

    events = adapter.collect(client, as_of=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))
    assert [event.name for event in events] == ["BLS State Employment and Unemployment"]


def test_curl_http_client_post_json_rejects_malformed_status(monkeypatch: pytest.MonkeyPatch) -> None:
    completed = SimpleNamespace(
        returncode=0,
        stdout=b"response body\n__CALENDARWATCH_STATUS__:not-a-number",
        stderr=b"",
    )

    monkeypatch.setattr(http.subprocess, "run", lambda *args, **kwargs: completed)

    client = http.CurlHttpClient(timeout_seconds=1, user_agent="test-agent")

    with pytest.raises(http.HttpFetchError, match="invalid status code"):
        client.post_json("https://example.com/hook", {"ok": True})
