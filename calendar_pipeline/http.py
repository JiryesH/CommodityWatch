from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any


DEFAULT_USER_AGENT = "CalendarWatchBot/1.0 (calendarwatch@commoditywatch.co)"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
STATUS_MARKER = b"\n__CALENDARWATCH_STATUS__:"


class HttpFetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status_code: int
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


class CurlHttpClient:
    def __init__(self, *, timeout_seconds: int = 30, user_agent: str = DEFAULT_USER_AGENT):
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        user_agent: str | None = None,
    ) -> HttpResponse:
        command = [
            "curl",
            "-sS",
            "-L",
            "--compressed",
            "--max-time",
            str(self.timeout_seconds),
            "-w",
            "\n__CALENDARWATCH_STATUS__:%{http_code}",
        ]
        effective_user_agent = self.user_agent if user_agent is None else user_agent
        if effective_user_agent:
            command.extend(["-A", effective_user_agent])
        for key, value in (headers or {}).items():
            command.extend(["-H", f"{key}: {value}"])
        command.append(url)

        completed = subprocess.run(command, capture_output=True, check=False)
        if completed.returncode != 0:
            message = completed.stderr.decode("utf-8", errors="replace").strip() or "curl failed"
            raise HttpFetchError(f"curl failed for {url}: {message}")

        if STATUS_MARKER not in completed.stdout:
            raise HttpFetchError(f"curl returned an unparseable response for {url}")

        body, _, status_bytes = completed.stdout.rpartition(STATUS_MARKER)
        try:
            status_code = int(status_bytes.decode("utf-8").strip())
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise HttpFetchError(f"curl returned an invalid status code for {url}") from exc

        if status_code < 200 or status_code >= 300:
            raise HttpFetchError(f"HTTP {status_code} for {url}")

        return HttpResponse(url=url, status_code=status_code, body=body)

    def post_json(self, url: str, payload: dict[str, Any]) -> HttpResponse:
        command = [
            "curl",
            "-sS",
            "-L",
            "--compressed",
            "--max-time",
            str(self.timeout_seconds),
            "-A",
            self.user_agent,
            "-H",
            "Content-Type: application/json",
            "-X",
            "POST",
            "-d",
            json.dumps(payload),
            "-w",
            "\n__CALENDARWATCH_STATUS__:%{http_code}",
            url,
        ]
        completed = subprocess.run(command, capture_output=True, check=False)
        if completed.returncode != 0:
            message = completed.stderr.decode("utf-8", errors="replace").strip() or "curl failed"
            raise HttpFetchError(f"curl failed for {url}: {message}")

        if STATUS_MARKER not in completed.stdout:
            raise HttpFetchError(f"curl returned an unparseable response for {url}")

        body, _, status_bytes = completed.stdout.rpartition(STATUS_MARKER)
        status_code = int(status_bytes.decode("utf-8").strip())
        if status_code < 200 or status_code >= 300:
            raise HttpFetchError(f"HTTP {status_code} for {url}")
        return HttpResponse(url=url, status_code=status_code, body=body)
