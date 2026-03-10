"""
Commodity News Feed - RSS Scraper
==================================
Scrapes and normalizes RSS feeds from ICIS, S&P Global Commodity Insights,
and Fastmarkets,
joining them into a single unified JSON data file for the frontend to consume.

Usage:
    python rss_scraper.py                # Run once, output to data/feed.json
    python rss_scraper.py --daemon       # Run continuously, polling every 10 minutes
    python rss_scraper.py --daemon --interval 300  # Custom poll interval (seconds)
    python rss_scraper.py --sentiment    # Also score headlines with FinBERT
    python rss_scraper.py --ner          # Also extract entities/countries with spaCy
    python sentiment_finbert.py --input data/feed.json --output data/feed.json
    python ner_spacy.py --input data/feed.json --output data/feed.json
    python app.py --host 127.0.0.1 --port 8081  # Control API for job orchestration

Requirements:
    pip install feedparser requests curl_cffi beautifulsoup4
    (curl_cffi is needed to bypass Akamai bot detection on S&P Global feeds)
    (beautifulsoup4 is needed for Argus HTML parsing)
    Optional: pip install cloudscraper  (additional fallback)
    Optional: pip install transformers torch  (for sentiment_finbert.py / --sentiment)
    Optional: pip install spacy pycountry  (for ner_spacy.py / --ner)
              python -m spacy download en_core_web_lg
"""

import feedparser
import requests
import os
import sys
import time
import re
import argparse
import logging
import threading
import subprocess
import shutil
import http.server
import socketserver
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional, Any
from article_processing import (
    deduplicate_articles as shared_deduplicate_articles,
    merge_duplicate_articles as shared_merge_duplicate_articles,
    normalize_article_published as shared_normalize_article_published,
    sort_articles_by_date as shared_sort_articles_by_date,
    to_utc_iso,
)
from classifier import (
    CANONICAL_CATEGORIES,
    MAX_CATEGORIES_PER_ARTICLE,
    normalize_article_categories,
)
from dedupe_utils import canonical_article_dedupe_key, canonical_article_id
from feed_io import load_feed_payload, save_feed_payload

# Sentiment and NER are optional — they require heavy ML deps (torch, spaCy)
# that are not installed in lightweight environments (e.g. GitHub Actions).
# When unavailable, stub classes keep the rest of the code path intact while
# the --sentiment / --ner flags simply have no effect.
try:
    from sentiment_finbert import SentimentConfig, FinBERTScorer, log_sentiment_rollup
    HAS_SENTIMENT = True
except ImportError:
    HAS_SENTIMENT = False
    from dataclasses import dataclass as _dc

    @_dc
    class SentimentConfig:  # type: ignore[no-redef]
        enabled: bool = False
        model_name: str = "ProsusAI/finbert"
        model_backend: str = "finbert"
        batch_size: int = 32
        max_length: int = 128
        use_description: bool = False
        force_rescore: bool = False
        pipeline_mode: str = "commodity_v1"
        context_mode: str = "auto"

    class FinBERTScorer:  # type: ignore[no-redef]
        def __init__(self, config): pass

    def log_sentiment_rollup(articles): pass  # type: ignore[misc]

try:
    from ner_spacy import NERConfig, SpacyNERExtractor, log_ner_rollup
    HAS_NER = True
except ImportError:
    HAS_NER = False
    from dataclasses import dataclass as _dc2

    @_dc2
    class NERConfig:  # type: ignore[no-redef]
        enabled: bool = False
        model_name: str = "en_core_web_lg"
        batch_size: int = 64
        use_description: bool = False
        force_rescore: bool = False
        max_entities: int = 18

    class SpacyNERExtractor:  # type: ignore[no-redef]
        def __init__(self, config): pass

    def log_ner_rollup(articles): pass  # type: ignore[misc]

# Try to import curl_cffi (best TLS fingerprint impersonation — beats Akamai)
try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# Try to import cloudscraper (handles Cloudflare JS challenges, sometimes Akamai)
try:
    import cloudscraper
    _scraper_session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    _scraper_session = None

# Try to import Argus scraper (BeautifulSoup dependency may be optional in some environments)
try:
    import argus_scraper
    HAS_ARGUS = True
except ImportError:
    argus_scraper = None  # type: ignore[assignment]
    HAS_ARGUS = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FEEDS = {
    # ICIS
    "ICIS": {
        "url": "https://www.icis.com/rss/publicrss/",
        "source": "ICIS",
        "category": "General",
        "timezone_hint": "UTC",
    },
    # Fastmarkets
    "Fastmarkets": {
        "url": "https://www.fastmarkets.com/insights/category/commodity/feed",
        "source": "Fastmarkets",
        "category": "General",
    },
    # S&P Global Energy
    # Note: the top-level /rss/oil.xml feed was abandoned by S&P and has data
    # from 2023. Use the more specific crude and refined feeds instead.
    "S&P Oil - Crude": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/oil-crude.xml",
        "source": "S&P Global",
        "category": "Oil - Crude",
    },
    "S&P Oil - Refined Products": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/oil-refined-products.xml",
        "source": "S&P Global",
        "category": "Oil - Refined Products",
    },
    "S&P Fertilizers": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/fertilizers.xml",
        "source": "S&P Global",
        "category": "Fertilizers",
    },
    "S&P Electric Power": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/electric-power.xml",
        "source": "S&P Global",
        "category": "Electric Power",
    },
    "S&P Natural Gas": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/natural-gas.xml",
        "source": "S&P Global",
        "category": "Natural Gas",
    },
    "S&P Coal": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/coal.xml",
        "source": "S&P Global",
        "category": "Coal",
    },
    "S&P Chemicals": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/chemicals.xml",
        "source": "S&P Global",
        "category": "Chemicals",
    },
    "S&P Metals": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/metals.xml",
        "source": "S&P Global",
        "category": "Metals",
    },
    "S&P Shipping": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/shipping.xml",
        "source": "S&P Global",
        "category": "Shipping",
    },
    "S&P Agriculture": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/agriculture.xml",
        "source": "S&P Global",
        "category": "Agriculture",
    },
    "S&P LNG": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/lng.xml",
        "source": "S&P Global",
        "category": "LNG",
    },
    "S&P Energy Transition": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/energy-transition.xml",
        "source": "S&P Global",
        "category": "Energy Transition",
    },
}

# Browser-like headers to avoid bot blocking (Akamai CDN on S&P Global)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

REQUEST_TIMEOUT = 15  # seconds

DEFAULT_ARGUS_MAX_PAGES = (
    int(getattr(argus_scraper, "DEFAULT_MAX_PAGES", 1)) if HAS_ARGUS else 1
)
DEFAULT_ARGUS_TIMEOUT = (
    int(getattr(argus_scraper, "REQUEST_TIMEOUT", 20)) if HAS_ARGUS else 20
)
DEFAULT_ARGUS_PAUSE = 0.2
DEFAULT_ARGUS_INCLUDE_LEAD = False
ARGUS_FEED_NAME = str(getattr(argus_scraper, "FEED_NAME", "Argus NewsAll"))

# Output
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "feed.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rss_scraper")

# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _normalize_named_utc_suffix(raw: str) -> str:
    if raw.endswith(" GMT") or raw.endswith(" UTC"):
        return f"{raw.rsplit(' ', 1)[0]} +0000"
    return raw


def _normalize_iso_utc_suffix(raw: str) -> str:
    if raw.endswith("Z"):
        return f"{raw[:-1]}+00:00"
    return raw


def _raw_has_explicit_timezone(raw: str) -> bool:
    token = (raw or "").strip().upper()
    if not token:
        return False
    if token.endswith("Z") or token.endswith(" UTC") or token.endswith(" GMT"):
        return True
    return re.search(r"[+-]\d{2}:?\d{2}\b", token) is not None


def parse_pub_date(
    raw: str,
    default_tz: Optional[timezone] = None,
) -> Optional[datetime]:
    """
    Parse timestamp text into a timezone-aware UTC datetime.
    Returns None if no explicit timezone/offset is available.
    """
    if not raw:
        return None

    raw = raw.strip()
    if not raw:
        return None

    raw = _normalize_named_utc_suffix(raw)

    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None and default_tz is not None:
            dt = dt.replace(tzinfo=default_tz)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    iso_candidate = _normalize_iso_utc_suffix(raw)
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None and default_tz is not None:
            dt = dt.replace(tzinfo=default_tz)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M %z",
        "%d %b %Y %H:%M %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S %z",
    ]:
        try:
            dt = datetime.strptime(iso_candidate, fmt)
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    if default_tz is not None:
        for fmt in [
            "%a, %d %b %Y %H:%M:%S",
            "%a, %d %b %Y %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]:
            try:
                dt = datetime.strptime(iso_candidate, fmt).replace(tzinfo=default_tz)
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue

    return None


def parse_pub_date_from_struct(struct_time) -> Optional[datetime]:
    """Convert feedparser's struct_time to a timezone-aware UTC datetime."""
    if struct_time is None:
        return None
    try:
        return datetime(*struct_time[:6], tzinfo=timezone.utc)
    except Exception:
        return None


def normalize_article_published(article: dict[str, Any]) -> None:
    shared_normalize_article_published(article, parse_pub_date=parse_pub_date)


def feed_default_timezone(feed_config: dict[str, Any]) -> Optional[timezone]:
    hint = str(feed_config.get("timezone_hint", "") or "").strip().upper()
    if hint == "UTC":
        return timezone.utc
    return None

# ---------------------------------------------------------------------------
# Article ID generation
# ---------------------------------------------------------------------------

def generate_article_id(link: str, title: str) -> str:
    """
    Generate a stable ID from the canonical dedupe key.
    Primary key is normalized URL; title is fallback when link is absent.
    """
    return canonical_article_id(link, title)

# ---------------------------------------------------------------------------
# HTTP fetching — multi-tier strategy for Akamai-protected sites
# ---------------------------------------------------------------------------

def _fetch_with_requests(url: str) -> Optional[str]:
    """Tier 1: plain requests — works for ICIS and non-protected feeds."""
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.debug(f"  requests → HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"  requests → {e}")
    return None


def _fetch_with_curl_cffi(url: str) -> Optional[str]:
    """Tier 2: curl_cffi — impersonates Chrome's TLS fingerprint. Beats Akamai."""
    if not HAS_CURL_CFFI:
        return None
    try:
        resp = cffi_requests.get(url, impersonate="chrome", timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.debug(f"  curl_cffi → HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"  curl_cffi → {e}")
    return None


def _fetch_with_cloudscraper(url: str) -> Optional[str]:
    """Tier 3: cloudscraper — handles Cloudflare JS challenges, sometimes Akamai."""
    if not HAS_CLOUDSCRAPER or _scraper_session is None:
        return None
    try:
        resp = _scraper_session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.debug(f"  cloudscraper → HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"  cloudscraper → {e}")
    return None


def _fetch_with_curl(url: str) -> Optional[str]:
    """
    Tier 3: macOS/system curl — its TLS stack (SecureTransport / BoringSSL)
    has a different fingerprint than Python's, which often passes Akamai.
    """
    curl_path = shutil.which("curl")
    if not curl_path:
        return None
    try:
        result = subprocess.run(
            [
                curl_path,
                "-sS",                        # silent but show errors
                "--compressed",                # accept gzip/br
                "-L",                          # follow redirects
                "--max-time", str(REQUEST_TIMEOUT),
                "-H", f"User-Agent: {REQUEST_HEADERS['User-Agent']}",
                "-H", f"Accept: {REQUEST_HEADERS['Accept']}",
                "-H", f"Accept-Language: {REQUEST_HEADERS['Accept-Language']}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=REQUEST_TIMEOUT + 5,
        )
        if result.returncode == 0 and result.stdout and "<rss" in result.stdout[:500].lower():
            return result.stdout
        logger.debug(f"  curl → exit {result.returncode}, len={len(result.stdout or '')}")
    except Exception as e:
        logger.debug(f"  curl → {e}")
    return None


def fetch_url(url: str, feed_name: str) -> Optional[str]:
    """
    Try multiple HTTP strategies in order of preference.
    Returns the raw response body (XML text) or None.
    """
    # Tier 1: plain requests (fast, works for ICIS and non-protected feeds)
    body = _fetch_with_requests(url)
    if body:
        return body

    # Tier 2: curl_cffi (Chrome TLS impersonation — beats Akamai)
    body = _fetch_with_curl_cffi(url)
    if body:
        logger.debug(f"[{feed_name}] succeeded via curl_cffi")
        return body

    # Tier 3: cloudscraper (handles Cloudflare, sometimes Akamai)
    body = _fetch_with_cloudscraper(url)
    if body:
        logger.debug(f"[{feed_name}] succeeded via cloudscraper")
        return body

    # Tier 4: system curl (macOS curl has different TLS fingerprint)
    body = _fetch_with_curl(url)
    if body:
        logger.debug(f"[{feed_name}] succeeded via curl")
        return body

    return None


# ---------------------------------------------------------------------------
# Feed fetching & parsing
# ---------------------------------------------------------------------------

def parse_feed_entries(
    xml_text: str,
    feed_name: str,
    feed_config: dict,
) -> tuple[list[dict], int]:
    """Parse RSS XML into normalized article dicts and timestamp parse errors."""
    source = feed_config["source"]
    category = feed_config["category"]
    default_tz = feed_default_timezone(feed_config)

    feed = feedparser.parse(xml_text)

    if feed.bozo and not feed.entries:
        logger.warning(f"[{feed_name}] Feed parse error: {feed.bozo_exception}")
        return [], 0

    articles = []
    parse_errors = 0
    for entry in feed.entries:
        title = " ".join(entry.get("title", "").split())
        link = entry.get("link", "").strip()
        description = " ".join(
            entry.get("summary", entry.get("description", "")).split()
        )

        raw_date = str(entry.get("published", entry.get("updated", "")) or "").strip()
        published_dt = parse_pub_date(raw_date, default_tz=default_tz)
        if published_dt is None:
            struct_time = entry.get("published_parsed", entry.get("updated_parsed"))
            if default_tz is not None or not raw_date or _raw_has_explicit_timezone(raw_date):
                published_dt = parse_pub_date_from_struct(struct_time)
        if published_dt is None and raw_date:
            parse_errors += 1
            logger.warning("[%s] Unparseable publish timestamp: %r", feed_name, raw_date)

        article = {
            "id": generate_article_id(link, title),
            "title": title,
            "description": description,
            "link": link,
            "published": to_utc_iso(published_dt),
            "source": source,
            "feed": feed_name,
            "category": category,
            "categories": [category],
        }
        normalize_article_categories(
            article,
            classify_fallback=True,
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        articles.append(article)

    return articles, parse_errors


def fetch_feed(feed_name: str, feed_config: dict) -> tuple[list[dict], int]:
    """
    Fetch and parse a single RSS feed.
    Uses multi-tier HTTP strategy (requests → cloudscraper → curl).
    Returns a list of normalized article dicts.
    """
    url = feed_config["url"]

    try:
        xml_text = fetch_url(url, feed_name)

        if xml_text is None:
            method_hint = ""
            if not HAS_CURL_CFFI:
                method_hint = " (try: pip install curl_cffi)"
            logger.warning(f"[{feed_name}] All fetch methods failed — skipping.{method_hint}")
            return [], 0

        articles, parse_errors = parse_feed_entries(xml_text, feed_name, feed_config)
        logger.info(
            "[%s] Fetched %s articles (timestamp parse errors=%s)",
            feed_name,
            len(articles),
            parse_errors,
        )
        return articles, parse_errors

    except Exception as e:
        logger.error(f"[{feed_name}] Unexpected error: {e}")
        return [], 0


def classify_articles_in_place(articles: list[dict]) -> None:
    """
    Enforce canonical category contract for each article.
    """
    for article in articles:
        normalize_article_categories(
            article,
            classify_fallback=True,
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )


def enforce_category_contract(
    articles: list[dict],
    *,
    classify_fallback: bool = True,
) -> dict[str, int]:
    """Normalize category fields and report normalization stats."""
    unknown_tokens = 0
    classifier_fallback_used = 0

    for article in articles:
        result = normalize_article_categories(
            article,
            classify_fallback=classify_fallback,
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        unknown_tokens += len(result.get("unknown_tokens", []))
        classifier_fallback_used += 1 if result.get("used_classifier") else 0

    return {
        "unknown_tokens_rewritten": unknown_tokens,
        "classifier_fallback_used": classifier_fallback_used,
    }


def scrape_argus(
    max_pages: int = DEFAULT_ARGUS_MAX_PAGES,
    timeout: int = DEFAULT_ARGUS_TIMEOUT,
    include_lead: bool = DEFAULT_ARGUS_INCLUDE_LEAD,
    pause: float = DEFAULT_ARGUS_PAUSE,
) -> tuple[list[dict], dict[str, Any]]:
    """Fetch Argus NewsAll HTML feed and return normalized articles + per-feed stats."""
    if not HAS_ARGUS or argus_scraper is None:
        return [], {
            "status": "failed",
            "count": 0,
            "timestamp_parse_errors": 0,
            "error": "Argus scraper unavailable (install beautifulsoup4).",
        }

    try:
        scraper = argus_scraper.ArgusNewsAllScraper(timeout=timeout)
        articles, pages_scraped, total_pages_hint = scraper.scrape(
            max_pages=max_pages,
            include_lead=include_lead,
            pause=pause,
        )
        classify_articles_in_place(articles)
        articles = sort_by_date(deduplicate(articles))

        detail: dict[str, Any] = {
            "status": "ok" if pages_scraped > 0 else "failed",
            "count": len(articles),
            "pages_scraped": pages_scraped,
            "max_pages": max_pages,
            "timestamp_parse_errors": int(
                getattr(scraper, "metrics", {}).get("timestamp_parse_errors", 0)
            ),
        }
        if total_pages_hint is not None:
            detail["total_pages_hint"] = total_pages_hint

        if pages_scraped <= 0:
            detail.setdefault("error", "No pages scraped from Argus")

        if pages_scraped > 0:
            logger.info("[%s] Fetched %s articles across %s page(s)", ARGUS_FEED_NAME, len(articles), pages_scraped)

        return articles, detail
    except Exception as exc:
        logger.error("[%s] Unexpected error: %s", ARGUS_FEED_NAME, exc)
        return [], {
            "status": "failed",
            "count": 0,
            "timestamp_parse_errors": 0,
            "error": str(exc),
        }

# ---------------------------------------------------------------------------
# Deduplication & sorting
# ---------------------------------------------------------------------------

def deduplicate(articles: list[dict]) -> list[dict]:
    deduped, _ = deduplicate_with_diagnostics(articles)
    return deduped


def _merge_duplicate_articles(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    return shared_merge_duplicate_articles(
        existing,
        incoming,
        generate_article_id=generate_article_id,
        max_categories=MAX_CATEGORIES_PER_ARTICLE,
    )


def deduplicate_with_diagnostics(articles: list[dict]) -> tuple[list[dict], dict[str, int]]:
    """Collapse duplicates by canonical dedupe key and merge categories deterministically."""
    return shared_deduplicate_articles(
        articles,
        parse_pub_date=parse_pub_date,
        generate_article_id=generate_article_id,
        max_categories=MAX_CATEGORIES_PER_ARTICLE,
    )


def sort_by_date(articles: list[dict], descending: bool = True) -> list[dict]:
    """
    Sort articles by parsed UTC timestamp and deterministic tie-breakers.
    Invalid/missing timestamps are ordered after valid timestamps.
    """
    return shared_sort_articles_by_date(
        articles,
        parse_pub_date=parse_pub_date,
        descending=descending,
    )

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_existing_feed() -> dict:
    """Load the existing feed.json if it exists."""
    return load_feed_payload(OUTPUT_FILE)


def save_feed(
    articles: list[dict],
    fetch_stats: dict,
    sentiment_stats: Optional[dict[str, Any]] = None,
    ner_stats: Optional[dict[str, Any]] = None,
    category_stats: Optional[dict[str, int]] = None,
    dedupe_stats: Optional[dict[str, Any]] = None,
):
    """Save the unified feed to JSON."""
    metadata = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_articles": len(articles),
        "feeds_fetched": fetch_stats.get("success", 0),
        "feeds_failed": fetch_stats.get("failed", 0),
        "feed_details": fetch_stats.get("details", {}),
        "timestamp_parse_errors": int(fetch_stats.get("timestamp_parse_errors", 0)),
        "canonical_categories": list(CANONICAL_CATEGORIES),
    }
    if sentiment_stats:
        metadata["sentiment"] = sentiment_stats
    if ner_stats:
        metadata["ner"] = ner_stats
    if category_stats:
        metadata["category_contract"] = category_stats
    if dedupe_stats:
        metadata["dedupe"] = dedupe_stats

    save_feed_payload(
        OUTPUT_FILE,
        articles=articles,
        metadata=metadata,
    )

    logger.info(
        f"Saved {len(articles)} articles to {OUTPUT_FILE} "
        f"({fetch_stats.get('success', 0)} feeds OK, "
        f"{fetch_stats.get('failed', 0)} failed)"
    )

# ---------------------------------------------------------------------------
# Incremental merge
# ---------------------------------------------------------------------------

def merge_with_existing(
    new_articles: list[dict],
    max_age_days: int = 7,
) -> tuple[list[dict], dict[str, int]]:
    """
    Merge new articles with the existing feed.json.
    - Adds new articles that aren't already present
    - Drops articles older than max_age_days
    - Keeps the feed size manageable
    """
    existing = load_existing_feed()
    existing_articles = existing.get("articles", [])
    for article in existing_articles:
        normalize_article_published(article)
        article["id"] = generate_article_id(article.get("link", ""), article.get("title", ""))
    enforce_category_contract(existing_articles, classify_fallback=True)
    existing_articles, existing_dedupe = deduplicate_with_diagnostics(existing_articles)

    # Build lookup of canonical dedupe keys (not legacy IDs).
    existing_by_key = {
        canonical_article_dedupe_key(article): article
        for article in existing_articles
        if canonical_article_dedupe_key(article)
    }

    # Add genuinely new articles
    added = 0
    updated = 0
    for article in new_articles:
        normalize_article_published(article)
        normalize_article_categories(
            article,
            classify_fallback=True,
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        article["id"] = generate_article_id(article.get("link", ""), article.get("title", ""))

        dedupe_key = canonical_article_dedupe_key(article)
        if not dedupe_key:
            continue
        existing_article = existing_by_key.get(dedupe_key)
        if existing_article is None:
            existing_articles.append(article)
            existing_by_key[dedupe_key] = article
            added += 1
            continue

        # Refresh existing article fields from the latest scrape but keep any
        # existing enrichments until incremental scoring decides if a rescore is needed.
        prior_sentiment = existing_article.get("sentiment")
        prior_ner = existing_article.get("ner")
        merged_article = _merge_duplicate_articles(existing_article, article)
        existing_article.clear()
        existing_article.update(merged_article)
        if prior_sentiment is not None:
            existing_article["sentiment"] = prior_sentiment
        if prior_ner is not None:
            existing_article["ner"] = prior_ner
        normalize_article_categories(
            existing_article,
            classify_fallback=True,
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        updated += 1

    # Filter out articles older than max_age_days
    # (For simplicity, we keep all articles — the frontend can filter by date.)
    # If you want age-based pruning, uncomment and adjust:
    #
    # from datetime import timedelta
    # cutoff_dt = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    # cutoff_iso = cutoff_dt.isoformat()
    # existing_articles = [
    #     a for a in existing_articles
    #     if (a.get("published") or "") >= cutoff_iso
    # ]

    logger.info(
        f"Merged: {added} new articles added, {updated} updated, "
        f"{len(existing_articles)} total"
    )

    enforce_category_contract(existing_articles, classify_fallback=True)
    merged_articles, post_merge_dedupe = deduplicate_with_diagnostics(existing_articles)
    merged_count = (
        int(existing_dedupe.get("merged", 0))
        + int(post_merge_dedupe.get("merged", 0))
    )
    merge_stats = {"new": added, "updated": updated, "merged": merged_count}
    return sort_by_date(merged_articles), merge_stats

# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_all(
    include_rss: bool = True,
    include_argus: bool = True,
    argus_max_pages: int = DEFAULT_ARGUS_MAX_PAGES,
    argus_timeout: int = DEFAULT_ARGUS_TIMEOUT,
    argus_include_lead: bool = DEFAULT_ARGUS_INCLUDE_LEAD,
    argus_pause: float = DEFAULT_ARGUS_PAUSE,
) -> tuple[list[dict], dict, dict[str, int]]:
    """Fetch all enabled sources, normalize, deduplicate, and return articles + stats."""
    all_articles = []
    stats = {"success": 0, "failed": 0, "details": {}, "timestamp_parse_errors": 0}
    configured_feeds = 0

    if include_rss:
        for feed_name, feed_config in FEEDS.items():
            configured_feeds += 1
            articles, parse_errors = fetch_feed(feed_name, feed_config)
            stats["timestamp_parse_errors"] += parse_errors
            if articles:
                all_articles.extend(articles)
                stats["success"] += 1
                stats["details"][feed_name] = {
                    "status": "ok",
                    "count": len(articles),
                    "timestamp_parse_errors": parse_errors,
                }
            else:
                stats["failed"] += 1
                stats["details"][feed_name] = {
                    "status": "failed",
                    "count": 0,
                    "timestamp_parse_errors": parse_errors,
                }

    if include_argus:
        configured_feeds += 1
        argus_articles, argus_detail = scrape_argus(
            max_pages=argus_max_pages,
            timeout=argus_timeout,
            include_lead=argus_include_lead,
            pause=argus_pause,
        )
        if argus_detail.get("status") == "ok":
            all_articles.extend(argus_articles)
            stats["success"] += 1
        else:
            stats["failed"] += 1
        stats["timestamp_parse_errors"] += int(
            argus_detail.get("timestamp_parse_errors", 0)
        )
        stats["details"][ARGUS_FEED_NAME] = argus_detail

    # Deduplicate (articles may appear in multiple S&P category feeds)
    all_articles, dedupe_stats = deduplicate_with_diagnostics(all_articles)
    enforce_category_contract(all_articles, classify_fallback=True)

    # Sort newest first
    all_articles = sort_by_date(all_articles)

    logger.info(
        f"Scrape complete: {len(all_articles)} unique articles "
        f"from {stats['success']}/{configured_feeds} feeds "
        f"(merged duplicates={dedupe_stats.get('merged', 0)})"
    )

    return all_articles, stats, dedupe_stats


def run_once(
    sentiment_config: Optional[SentimentConfig] = None,
    scorer: Optional[FinBERTScorer] = None,
    ner_config: Optional[NERConfig] = None,
    ner_extractor: Optional[SpacyNERExtractor] = None,
    include_rss: bool = True,
    include_argus: bool = True,
    argus_max_pages: int = DEFAULT_ARGUS_MAX_PAGES,
    argus_timeout: int = DEFAULT_ARGUS_TIMEOUT,
    argus_include_lead: bool = DEFAULT_ARGUS_INCLUDE_LEAD,
    argus_pause: float = DEFAULT_ARGUS_PAUSE,
):
    """Single scrape run: fetch, merge with existing, (optional) enrichments, save."""
    new_articles, stats, scrape_dedupe_stats = scrape_all(
        include_rss=include_rss,
        include_argus=include_argus,
        argus_max_pages=argus_max_pages,
        argus_timeout=argus_timeout,
        argus_include_lead=argus_include_lead,
        argus_pause=argus_pause,
    )
    merged, merge_stats = merge_with_existing(new_articles)
    sentiment_stats = None
    ner_stats = None
    category_stats = enforce_category_contract(merged, classify_fallback=True)
    run_dedupe_stats = {
        "new": int(merge_stats.get("new", 0)),
        "updated": int(merge_stats.get("updated", 0)),
        "merged": int(scrape_dedupe_stats.get("merged", 0))
        + int(merge_stats.get("merged", 0)),
        "incoming_merged": int(scrape_dedupe_stats.get("merged", 0)),
        "existing_merged": int(merge_stats.get("merged", 0)),
    }
    logger.info(
        "Dedupe diagnostics: new=%s updated=%s merged=%s",
        run_dedupe_stats["new"],
        run_dedupe_stats["updated"],
        run_dedupe_stats["merged"],
    )

    if sentiment_config and sentiment_config.enabled:
        try:
            scorer = scorer or FinBERTScorer(sentiment_config)
            sentiment_stats = scorer.score_incremental(merged)
            logger.info(
                "Sentiment: scored %s, reused %s (candidates=%s, %sms)",
                sentiment_stats.get("scored", 0),
                sentiment_stats.get("reused", 0),
                sentiment_stats.get("candidate_articles", 0),
                sentiment_stats.get("duration_ms", 0),
            )
            if sentiment_stats.get("scored", 0) > 0:
                log_sentiment_rollup(merged)
        except Exception as e:
            logger.error(f"Sentiment scoring failed: {e}")
            sentiment_stats = {
                "enabled": True,
                "model": sentiment_config.model_name,
                "input_mode": (
                    sentiment_config.requested_context_mode()
                    if hasattr(sentiment_config, "requested_context_mode")
                    else ("title+description" if sentiment_config.use_description else "title")
                ),
                "error": str(e),
            }

    if ner_config and ner_config.enabled:
        try:
            ner_extractor = ner_extractor or SpacyNERExtractor(ner_config)
            ner_stats = ner_extractor.extract_incremental(merged)
            logger.info(
                "NER: extracted %s, reused %s (candidates=%s, %sms)",
                ner_stats.get("extracted", 0),
                ner_stats.get("reused", 0),
                ner_stats.get("candidate_articles", 0),
                ner_stats.get("duration_ms", 0),
            )
            if ner_stats.get("extracted", 0) > 0:
                log_ner_rollup(merged)
        except Exception as e:
            logger.error(f"NER extraction failed: {e}")
            ner_stats = {
                "enabled": True,
                "model": ner_config.model_name,
                "input_mode": (
                    "title+description" if ner_config.use_description else "title"
                ),
                "error": str(e),
            }

    save_feed(
        merged,
        stats,
        sentiment_stats=sentiment_stats,
        ner_stats=ner_stats,
        category_stats=category_stats,
        dedupe_stats=run_dedupe_stats,
    )
    return merged, stats, sentiment_stats, ner_stats


def run_daemon(
    interval: int = 600,
    sentiment_config: Optional[SentimentConfig] = None,
    ner_config: Optional[NERConfig] = None,
    include_rss: bool = True,
    include_argus: bool = True,
    argus_max_pages: int = DEFAULT_ARGUS_MAX_PAGES,
    argus_timeout: int = DEFAULT_ARGUS_TIMEOUT,
    argus_include_lead: bool = DEFAULT_ARGUS_INCLUDE_LEAD,
    argus_pause: float = DEFAULT_ARGUS_PAUSE,
):
    """Continuous scrape loop."""
    logger.info(f"Starting daemon mode — polling every {interval}s")
    scorer = None
    if sentiment_config and sentiment_config.enabled:
        scorer = FinBERTScorer(sentiment_config)
    ner_extractor = None
    if ner_config and ner_config.enabled:
        ner_extractor = SpacyNERExtractor(ner_config)

    while True:
        try:
            run_once(
                sentiment_config=sentiment_config,
                scorer=scorer,
                ner_config=ner_config,
                ner_extractor=ner_extractor,
                include_rss=include_rss,
                include_argus=include_argus,
                argus_max_pages=argus_max_pages,
                argus_timeout=argus_timeout,
                argus_include_lead=argus_include_lead,
                argus_pause=argus_pause,
            )
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
            break
        except Exception as e:
            logger.error(f"Scrape cycle failed: {e}")

        logger.info(f"Next poll in {interval}s...")
        time.sleep(interval)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# HTTP server (for --serve mode)
# ---------------------------------------------------------------------------

class SilentHTTPHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler with suppressed access logs."""

    def log_message(self, format, *args):
        pass  # suppress per-request stdout noise


def start_http_server(port: int, directory: str):
    """Serve the project directory over HTTP. Runs in a background thread."""
    os.chdir(directory)
    handler = SilentHTTPHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        httpd.allow_reuse_address = True
        logger.info(f"HTTP server running → http://localhost:{port}/")
        httpd.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Commodity News Feed RSS Scraper")
    parser.add_argument(
        "--daemon", action="store_true", help="Run continuously, polling on an interval"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=600,
        help="Poll interval in seconds (default: 600 = 10 min)",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Also start an HTTP server so you can open index.html in a browser",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="HTTP server port (default: 8000)",
    )
    parser.add_argument(
        "--no-rss",
        action="store_true",
        help="Disable RSS sources (ICIS/S&P/Fastmarkets) for this run",
    )
    parser.add_argument(
        "--no-argus",
        action="store_true",
        help="Disable Argus NewsAll HTML source for this run",
    )
    parser.add_argument(
        "--argus-max-pages",
        type=int,
        default=DEFAULT_ARGUS_MAX_PAGES,
        help=f"Maximum Argus pages to scrape per run (default: {DEFAULT_ARGUS_MAX_PAGES})",
    )
    parser.add_argument(
        "--argus-timeout",
        type=int,
        default=DEFAULT_ARGUS_TIMEOUT,
        help=f"Argus request timeout in seconds (default: {DEFAULT_ARGUS_TIMEOUT})",
    )
    parser.add_argument(
        "--argus-pause",
        type=float,
        default=DEFAULT_ARGUS_PAUSE,
        help=f"Pause between Argus requests in seconds (default: {DEFAULT_ARGUS_PAUSE})",
    )
    parser.add_argument(
        "--argus-include-lead",
        action="store_true",
        help="Fetch Argus article pages and store lead paragraph as description",
    )
    parser.add_argument(
        "--sentiment",
        action="store_true",
        help="Run FinBERT sentiment analysis and store scores in feed.json",
    )
    parser.add_argument(
        "--sentiment-model",
        default="ProsusAI/finbert",
        help="HuggingFace model to use for sentiment (default: ProsusAI/finbert)",
    )
    parser.add_argument(
        "--sentiment-backend",
        default="finbert",
        help="Sentiment backend to use (default: finbert)",
    )
    parser.add_argument(
        "--sentiment-batch-size",
        type=int,
        default=32,
        help="Sentiment inference batch size (default: 32)",
    )
    parser.add_argument(
        "--sentiment-max-length",
        type=int,
        default=128,
        help="Sentiment max token length (default: 128)",
    )
    parser.add_argument(
        "--sentiment-use-description",
        action="store_true",
        help="Score title + description instead of title only",
    )
    parser.add_argument(
        "--sentiment-pipeline-mode",
        choices=["baseline", "commodity_v1"],
        default="commodity_v1",
        help="Sentiment pipeline mode (default: commodity_v1)",
    )
    parser.add_argument(
        "--sentiment-context-mode",
        choices=["auto", "title", "title+description"],
        default="auto",
        help="Sentiment context mode (default: auto)",
    )
    parser.add_argument(
        "--sentiment-force-rescore",
        action="store_true",
        help="Rescore even if sentiment already exists for an article",
    )
    parser.add_argument(
        "--ner",
        action="store_true",
        help="Run spaCy entity + country extraction and store it in feed.json",
    )
    parser.add_argument(
        "--ner-model",
        default="en_core_web_lg",
        help="spaCy model to use for NER (default: en_core_web_lg)",
    )
    parser.add_argument(
        "--ner-batch-size",
        type=int,
        default=64,
        help="NER inference batch size (default: 64)",
    )
    parser.add_argument(
        "--ner-use-description",
        action="store_true",
        help="Run NER on title + description instead of title only",
    )
    parser.add_argument(
        "--ner-force-rescore",
        action="store_true",
        help="Re-extract NER even if cached results already exist",
    )
    parser.add_argument(
        "--ner-max-entities",
        type=int,
        default=18,
        help="Maximum stored entities per article for NER (default: 18)",
    )
    args = parser.parse_args()

    include_rss = not args.no_rss
    include_argus = not args.no_argus
    if not include_rss and not include_argus:
        parser.error("At least one source must be enabled (rss/argus).")

    sentiment_config = SentimentConfig(
        enabled=args.sentiment,
        model_name=args.sentiment_model,
        model_backend=args.sentiment_backend,
        batch_size=args.sentiment_batch_size,
        max_length=args.sentiment_max_length,
        use_description=args.sentiment_use_description,
        force_rescore=args.sentiment_force_rescore,
        pipeline_mode=args.sentiment_pipeline_mode,
        context_mode=args.sentiment_context_mode,
    )
    ner_config = NERConfig(
        enabled=args.ner,
        model_name=args.ner_model,
        batch_size=args.ner_batch_size,
        use_description=args.ner_use_description,
        force_rescore=args.ner_force_rescore,
        max_entities=args.ner_max_entities,
    )

    project_dir = str(Path(__file__).parent.resolve())

    if args.serve:
        # Launch HTTP server in a daemon thread so it dies when the main process exits
        server_thread = threading.Thread(
            target=start_http_server,
            args=(args.port, project_dir),
            daemon=True,
        )
        server_thread.start()
        logger.info(f"Open in browser: http://localhost:{args.port}/")

    if args.daemon or args.serve:
        # --serve alone implies daemon mode (keep refreshing feed.json)
        run_daemon(
            args.interval,
            sentiment_config=sentiment_config,
            ner_config=ner_config,
            include_rss=include_rss,
            include_argus=include_argus,
            argus_max_pages=args.argus_max_pages,
            argus_timeout=args.argus_timeout,
            argus_include_lead=args.argus_include_lead,
            argus_pause=args.argus_pause,
        )
    else:
        articles, stats, sentiment_stats, ner_stats = run_once(
            sentiment_config=sentiment_config,
            ner_config=ner_config,
            include_rss=include_rss,
            include_argus=include_argus,
            argus_max_pages=args.argus_max_pages,
            argus_timeout=args.argus_timeout,
            argus_include_lead=args.argus_include_lead,
            argus_pause=args.argus_pause,
        )
        print(f"\nDone! {len(articles)} articles saved to {OUTPUT_FILE}")
        print(f"Feeds OK: {stats['success']} | Failed: {stats['failed']}")
        if sentiment_stats and sentiment_stats.get("enabled"):
            if sentiment_stats.get("error"):
                print(f"Sentiment: failed ({sentiment_stats['error']})")
            else:
                print(
                    "Sentiment: "
                    f"scored={sentiment_stats.get('scored', 0)} | "
                    f"reused={sentiment_stats.get('reused', 0)} | "
                    f"candidates={sentiment_stats.get('candidate_articles', 0)}"
                )
        if ner_stats and ner_stats.get("enabled"):
            if ner_stats.get("error"):
                print(f"NER: failed ({ner_stats['error']})")
            else:
                print(
                    "NER: "
                    f"extracted={ner_stats.get('extracted', 0)} | "
                    f"reused={ner_stats.get('reused', 0)} | "
                    f"candidates={ner_stats.get('candidate_articles', 0)}"
                )

        # Print summary of what we got
        print("\nFeed breakdown:")
        for name, detail in stats["details"].items():
            status = "✓" if detail["status"] == "ok" else "✗"
            print(f"  {status} {name}: {detail['count']} articles")

        if args.serve:
            logger.info("Server running. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Stopped.")


if __name__ == "__main__":
    main()
