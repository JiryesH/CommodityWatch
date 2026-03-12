from __future__ import annotations

import json
from pathlib import Path

import pytest

from feed_io import (
    FeedPersistenceError,
    default_headline_feed_output_path,
    empty_feed_payload,
    local_headline_feed_path,
    load_feed_json,
    load_feed_payload,
    preferred_headline_feed_path,
    resolve_repo_path,
    save_feed_json,
    save_feed_payload,
    tracked_headline_feed_path,
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


def test_preferred_headline_feed_path_falls_back_to_tracked_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CONTANGO_HEADLINE_FEED_PATH", raising=False)

    assert preferred_headline_feed_path(tmp_path) == tracked_headline_feed_path(tmp_path)


def test_preferred_headline_feed_path_prefers_local_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CONTANGO_HEADLINE_FEED_PATH", raising=False)
    local_path = local_headline_feed_path(tmp_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text("{}", encoding="utf-8")

    assert preferred_headline_feed_path(tmp_path) == local_path


def test_preferred_headline_feed_path_honors_explicit_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONTANGO_HEADLINE_FEED_PATH", "data/custom-feed.json")

    assert preferred_headline_feed_path(tmp_path) == resolve_repo_path("data/custom-feed.json", tmp_path)


def test_default_headline_feed_output_path_defaults_to_local_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CONTANGO_FEED_OUTPUT", raising=False)

    assert default_headline_feed_output_path(tmp_path) == local_headline_feed_path(tmp_path)


def test_default_headline_feed_output_path_honors_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONTANGO_FEED_OUTPUT", "data/custom-output.json")

    assert default_headline_feed_output_path(tmp_path) == resolve_repo_path("data/custom-output.json", tmp_path)
