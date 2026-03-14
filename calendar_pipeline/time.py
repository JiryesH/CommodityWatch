from __future__ import annotations

import calendar
import re
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo


EASTERN_ZONE = ZoneInfo("America/New_York")
CENTRAL_ZONE = ZoneInfo("America/Chicago")
UK_ZONE = ZoneInfo("Europe/London")
FRANKFURT_ZONE = ZoneInfo("Europe/Berlin")


def parse_email_datetime(value: str) -> datetime:
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_us_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%B %d, %Y").date()


def parse_slash_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%m/%d/%Y").date()


def parse_day_month_year(value: str) -> date:
    return datetime.strptime(value.strip(), "%d/%m/%Y").date()


def parse_us_time(value: str) -> time:
    normalized = (
        value.strip()
        .replace("a.m.", "AM")
        .replace("p.m.", "PM")
        .replace("a.m", "AM")
        .replace("p.m", "PM")
        .replace("am", "AM")
        .replace("pm", "PM")
    )
    normalized = re.sub(r"\s+(?:ET|EDT|EST|CT|CDT|CST|MT|MDT|MST|PT|PDT|PST|UTC)$", "", normalized)
    return datetime.strptime(normalized, "%I:%M %p").time()


def local_date_time_to_utc(day: date, local_time: time, zone: ZoneInfo) -> datetime:
    local_dt = datetime.combine(day, local_time, tzinfo=zone)
    return local_dt.astimezone(timezone.utc)


def eastern_to_utc(day: date, local_time: time) -> datetime:
    return local_date_time_to_utc(day, local_time, EASTERN_ZONE)


def central_to_utc(day: date, local_time: time) -> datetime:
    return local_date_time_to_utc(day, local_time, CENTRAL_ZONE)


def uk_to_utc(day: date, local_time: time) -> datetime:
    return local_date_time_to_utc(day, local_time, UK_ZONE)


def frankfurt_to_utc(day: date, local_time: time) -> datetime:
    return local_date_time_to_utc(day, local_time, FRANKFURT_ZONE)


def start_of_day_utc(value: datetime) -> datetime:
    normalized = value.astimezone(timezone.utc)
    return datetime(
        normalized.year,
        normalized.month,
        normalized.day,
        tzinfo=timezone.utc,
    )


def end_of_day_utc(value: datetime) -> datetime:
    return start_of_day_utc(value) + timedelta(days=1) - timedelta(seconds=1)


def add_months(day: date, amount: int) -> date:
    year = day.year + ((day.month - 1 + amount) // 12)
    month = ((day.month - 1 + amount) % 12) + 1
    day_of_month = min(day.day, calendar.monthrange(year, month)[1])
    return date(year, month, day_of_month)


def month_start(day: date) -> date:
    return day.replace(day=1)


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def nearest_weekday(target: date, weekday: int, max_distance: int = 3) -> date | None:
    for offset in range(max_distance + 1):
        for direction in (-1, 1):
            candidate = target + timedelta(days=offset * direction)
            if candidate.weekday() == weekday:
                return candidate
        if target.weekday() == weekday:
            return target
    return None
