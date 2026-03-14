from __future__ import annotations

from .http import CurlHttpClient
from .storage import CalendarRepository


class FailureDigestService:
    def __init__(self, repository: CalendarRepository, client: CurlHttpClient):
        self.repository = repository
        self.client = client

    def send(self, endpoint_url: str, *, since_hours: int = 24) -> dict[str, int | bool]:
        failures = self.repository.list_pending_failures(since_hours=since_hours)
        if not failures:
            return {"sent": False, "count": 0}

        payload = {
            "summary": {
                "failure_count": len(failures),
                "window_hours": since_hours,
            },
            "failures": [
                {
                    "id": failure["id"],
                    "source_slug": failure["source_slug"],
                    "failed_at": failure["failed_at"].isoformat(),
                    "error_message": failure["error_message"],
                    "details": failure["details"],
                }
                for failure in failures
            ],
        }
        self.client.post_json(endpoint_url, payload)
        self.repository.mark_failures_digested([int(failure["id"]) for failure in failures])
        return {"sent": True, "count": len(failures)}
