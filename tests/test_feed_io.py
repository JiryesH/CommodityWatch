from __future__ import annotations

import json
from pathlib import Path

import pytest

from feed_io import (
    FeedPersistenceError,
    empty_feed_payload,
    load_feed_json,
    load_feed_payload,
    save_feed_json,
    save_feed_payload,
)


def test_load_feed_payload_returns_empty_payload_when_missing(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"

    assert load_feed_payload(path) == empty_feed_payload()


def test_load_feed_payload_raises_for_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"
    path.write_text("{invalid", encoding="utf-8")

    with pytest.raises(FeedPersistenceError, match="not valid JSON"):
        load_feed_payload(path)


def test_load_feed_payload_raises_for_invalid_payload_shape(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"
    path.write_text(json.dumps({"articles": {}, "metadata": []}), encoding="utf-8")

    with pytest.raises(FeedPersistenceError, match="invalid 'articles'"):
        load_feed_payload(path)


def test_save_feed_json_preserves_existing_file_on_failed_write(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"
    original = {"articles": [{"id": "kept"}], "metadata": {"version": 1}}
    path.write_text(json.dumps(original), encoding="utf-8")

    with pytest.raises(FeedPersistenceError, match="Unable to save feed file"):
        save_feed_json(path, {"bad": {1, 2, 3}})

    assert load_feed_json(path) == original


def test_save_feed_payload_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"
    articles = [{"id": "1", "title": "Copper steady"}]
    metadata = {"total_articles": 1}

    save_feed_payload(path, articles=articles, metadata=metadata)

    assert load_feed_payload(path) == {"articles": articles, "metadata": metadata}
