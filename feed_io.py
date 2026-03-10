"""Shared JSON feed load/save helpers."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class FeedPersistenceError(RuntimeError):
    """Raised when a feed file exists but cannot be safely read or written."""


def empty_feed_payload() -> dict[str, Any]:
    return {"articles": [], "metadata": {}}


def load_feed_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_feed_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = handle.name
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception as exc:
        if temp_path is not None:
            try:
                Path(temp_path).unlink()
            except OSError:
                pass
        raise FeedPersistenceError(f"Unable to save feed file: {path}") from exc


def ensure_feed_metadata(data: dict[str, Any]) -> dict[str, Any]:
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata
    return metadata


def load_feed_payload(path: Path) -> dict[str, Any]:
    try:
        data = load_feed_json(path)
    except FileNotFoundError:
        return empty_feed_payload()
    except json.JSONDecodeError as exc:
        raise FeedPersistenceError(f"Feed file is not valid JSON: {path}") from exc
    except OSError as exc:
        raise FeedPersistenceError(f"Unable to read feed file: {path}") from exc

    if not isinstance(data, dict):
        raise FeedPersistenceError(f"Feed payload must be a JSON object: {path}")

    payload = dict(data)
    articles = payload.get("articles")
    if articles is None:
        payload["articles"] = []
    elif not isinstance(articles, list):
        raise FeedPersistenceError(f"Feed payload has invalid 'articles': {path}")

    metadata = payload.get("metadata")
    if metadata is None:
        payload["metadata"] = {}
    elif not isinstance(metadata, dict):
        raise FeedPersistenceError(f"Feed payload has invalid 'metadata': {path}")

    return payload


def save_feed_payload(
    path: Path,
    *,
    articles: list[Any],
    metadata: dict[str, Any],
) -> None:
    save_feed_json(path, {"metadata": metadata, "articles": articles})
