"""
Shared deduplication helpers for ingestion pipelines.

Canonical strategy:
1. Use a normalized URL as the primary dedupe key.
2. Fall back to normalized title when link is missing.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_MULTI_SLASH_RE = re.compile(r"/+")
_TRACKING_QUERY_KEYS = frozenset(
    {
        "fbclid",
        "gclid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "mkt_tok",
        "spm",
        "utm_content",
        "utm_id",
        "utm_medium",
        "utm_name",
        "utm_campaign",
        "utm_reader",
        "utm_pubreferrer",
        "utm_source",
        "utm_swu",
        "utm_term",
        "utm_viz_id",
    }
)


def normalize_title_for_dedupe(raw_title: Any) -> str:
    """Normalize title text for stable fallback dedupe keys."""
    return " ".join(str(raw_title or "").split()).casefold()


def _looks_like_domain_path(raw: str) -> bool:
    """
    Heuristic for URLs missing scheme, e.g. "www.example.com/path".
    """
    if not raw or raw.startswith("/"):
        return False
    first = raw.split("/", 1)[0]
    return "." in first and " " not in first


def normalize_url_for_dedupe(raw_url: Any) -> str:
    """
    Normalize URLs for deterministic dedupe keys.

    - Lowercase scheme + host
    - Remove default ports (:80 for http, :443 for https)
    - Collapse duplicate slashes in path
    - Remove non-root trailing slash
    - Strip fragment
    - Drop common tracking query params (utm_*, fbclid, gclid, ...)
    - Sort remaining query params for stable ordering
    """
    raw = " ".join(str(raw_url or "").split())
    if not raw:
        return ""

    parts = urlsplit(raw)
    if not parts.scheme and raw.startswith("//"):
        parts = urlsplit(f"https:{raw}")
    elif not parts.scheme and not parts.netloc and _looks_like_domain_path(raw):
        parts = urlsplit(f"https://{raw}")

    scheme = (parts.scheme or "https").lower()
    netloc = (parts.netloc or "").lower()

    if netloc:
        userinfo = ""
        hostport = netloc
        if "@" in hostport:
            userinfo, hostport = hostport.rsplit("@", 1)
        if ":" in hostport:
            host, port = hostport.rsplit(":", 1)
            if (
                (scheme == "http" and port == "80")
                or (scheme == "https" and port == "443")
            ):
                hostport = host
        netloc = f"{userinfo}@{hostport}" if userinfo else hostport

    path = _MULTI_SLASH_RE.sub("/", parts.path or "/")
    if path != "/":
        path = path.rstrip("/") or "/"

    filtered_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_QUERY_KEYS:
            continue
        filtered_query.append((key, value))
    filtered_query.sort(key=lambda item: (item[0].lower(), item[1]))
    query = urlencode(filtered_query, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def canonical_dedupe_key(link: Any, title: Any) -> str:
    """
    Canonical dedupe key:
      1) normalized URL when available
      2) normalized title fallback
    """
    normalized_url = normalize_url_for_dedupe(link)
    if normalized_url:
        return normalized_url

    normalized_title = normalize_title_for_dedupe(title)
    if normalized_title:
        return f"title:{normalized_title}"
    return ""


def canonical_article_dedupe_key(article: dict[str, Any]) -> str:
    return canonical_dedupe_key(article.get("link", ""), article.get("title", ""))


def canonical_article_id(link: Any, title: Any) -> str:
    """
    Stable short ID derived from canonical dedupe key.
    """
    key = canonical_dedupe_key(link, title) or "title:"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

