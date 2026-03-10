"""Shared helpers for article enrichment pipelines (sentiment, NER)."""

from __future__ import annotations

import hashlib
from typing import Any


def build_enrichment_text(article: dict[str, Any], use_description: bool) -> str:
    """Build the input text for an enrichment model from an article."""
    title = " ".join((article.get("title") or "").split())
    if not use_description:
        return title

    description = " ".join((article.get("description") or "").split())
    if not description:
        return title
    return f"{title}. {description}"


def enrichment_text_hash(text: str) -> str:
    """Stable SHA-256 hash for incremental re-processing checks."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
