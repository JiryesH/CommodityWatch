from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime


class BaseCalendarAdapter(ABC):
    slug: str
    pattern: str
    primary_url: str

    @abstractmethod
    def collect(self, client, *, as_of: datetime | None = None):
        raise NotImplementedError


class StructuredFeedAdapter(BaseCalendarAdapter):
    pattern = "structured_feed"


class HtmlCalendarAdapter(BaseCalendarAdapter):
    pattern = "html"


class PdfCalendarAdapter(BaseCalendarAdapter):
    pattern = "pdf"


class PressReleaseMonitorAdapter(BaseCalendarAdapter):
    pattern = "press_release"
