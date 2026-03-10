#!/usr/bin/env python3
"""
Argus NewsAll HTML Scraper
==========================
Scrapes Argus Media latest news from:
    https://www.argusmedia.com/pages/NewsAll.aspx

Normalizes output into the same JSON article schema used by RSS scrapers:
    {
      "metadata": {...},
      "articles": [
        {
          "id": "...",
          "title": "...",
          "description": "...",
          "link": "...",
          "published": "ISO-8601",
          "source": "Argus Media",
          "feed": "Argus NewsAll",
          "category": "General"
        }
      ]
    }

Usage:
    python argus_scraper.py
    python argus_scraper.py --max-pages 8
    python argus_scraper.py --include-lead --max-pages 2
    python argus_scraper.py --daemon --interval 600
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
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


BASE_URL = "https://www.argusmedia.com"
NEWS_ALL_URL = f"{BASE_URL}/pages/NewsAll.aspx"
SOURCE_NAME = "Argus Media"
FEED_NAME = "Argus NewsAll"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

REQUEST_TIMEOUT = 20
DEFAULT_INTERVAL = 600
DEFAULT_MAX_PAGES = 1
DEFAULT_OUTPUT = Path(__file__).parent / "data" / "argus_feed.json"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("argus_scraper")


def normalize_whitespace(text: str) -> str:
    return " ".join(unescape(text or "").replace("\xa0", " ").split())


def _normalize_named_utc_suffix(raw: str) -> str:
    if raw.endswith(" GMT") or raw.endswith(" UTC"):
        return f"{raw.rsplit(' ', 1)[0]} +0000"
    return raw


def _normalize_iso_utc_suffix(raw: str) -> str:
    if raw.endswith("Z"):
        return f"{raw[:-1]}+00:00"
    return raw


def parse_pub_date(
    raw: str,
    metrics: Optional[dict[str, int]] = None,
) -> Optional[datetime]:
    if not raw:
        return None

    raw = _normalize_named_utc_suffix(normalize_whitespace(raw))
    if not raw:
        return None

    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    iso_candidate = _normalize_iso_utc_suffix(raw)
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in (
        "%d %b %Y %H:%M %z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S %z",
    ):
        try:
            dt = datetime.strptime(iso_candidate, fmt)
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    if metrics is not None and raw:
        metrics["timestamp_parse_errors"] = metrics.get("timestamp_parse_errors", 0) + 1
        logger.warning("Could not parse date: '%s'", raw)
    return None


def normalize_article_published(article: dict[str, Any]) -> None:
    shared_normalize_article_published(article, parse_pub_date=parse_pub_date)


def generate_article_id(link: str, title: str) -> str:
    return canonical_article_id(link, title)


def extract_postback_target(href: str) -> Optional[str]:
    if not href:
        return None
    href = unescape(href)
    match = re.search(r"__doPostBack\('([^']+)'", href)
    if match:
        return match.group(1)
    return None


def canonicalize_article_link(raw_href: str, numeric_id: Optional[str]) -> str:
    if numeric_id:
        return f"{BASE_URL}/pages/NewsBody.aspx?id={numeric_id}&menu=yes"
    if raw_href:
        return urljoin(NEWS_ALL_URL, raw_href)
    return ""


class ArgusNewsAllScraper:
    def __init__(self, timeout: int = REQUEST_TIMEOUT):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self.metrics: dict[str, int] = {"timestamp_parse_errors": 0}

    def _get(self, url: str) -> str:
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def _post(self, url: str, data: dict[str, str]) -> str:
        resp = self.session.post(url, data=data, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _extract_form_fields(soup: BeautifulSoup) -> dict[str, str]:
        form = soup.select_one("form#aspnetForm")
        if not form:
            return {}

        fields: dict[str, str] = {}
        for inp in form.select("input[name]"):
            name = inp.get("name")
            if not name:
                continue
            fields[name] = inp.get("value", "")
        return fields

    @staticmethod
    def _current_page(soup: BeautifulSoup) -> Optional[int]:
        marker = soup.select_one("table[id$='pager_tblMain'] span b")
        if not marker:
            return None
        text = normalize_whitespace(marker.get_text(" ", strip=True))
        if text.isdigit():
            return int(text)
        return None

    @staticmethod
    def _total_pages_hint(soup: BeautifulSoup) -> Optional[int]:
        fields = ArgusNewsAllScraper._extract_form_fields(soup)
        total_raw = fields.get("ctl00$P$ucNewsList$pager$ctl07", "").strip()
        size_raw = fields.get("ctl00$P$ucNewsList$pager$ctl08", "").strip()
        if not (total_raw.isdigit() and size_raw.isdigit()):
            return None
        total = int(total_raw)
        page_size = int(size_raw)
        if total <= 0 or page_size <= 0:
            return None
        return math.ceil(total / page_size)

    @staticmethod
    def _next_target(soup: BeautifulSoup, current_page: int) -> Optional[str]:
        wanted = current_page + 1
        for link in soup.select("a[id*='pager_lstPages_'][id$='_lnkPage']"):
            text = normalize_whitespace(link.get_text(" ", strip=True))
            if text.isdigit() and int(text) == wanted:
                return extract_postback_target(link.get("href", ""))
        return None

    @staticmethod
    def _extract_numeric_id(row: BeautifulSoup) -> Optional[str]:
        title_link = row.select_one("a[id^='lnkNewsHeader']")
        if title_link:
            match = re.search(r"(\d+)$", title_link.get("id", ""))
            if match:
                return match.group(1)

        row_id = row.get("id", "")
        match = re.search(r"tr_(\d+)__", row_id)
        if match:
            return match.group(1)
        return None

    def _parse_page_articles(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        articles: list[dict[str, Any]] = []
        for row in soup.select("tr[name='tdDateData']"):
            date_link = row.select_one("a[id^='lnkNewsDate']")
            title_link = row.select_one("a[id^='lnkNewsHeader']")
            if not date_link or not title_link:
                continue

            title = normalize_whitespace(title_link.get_text(" ", strip=True))
            raw_date = normalize_whitespace(date_link.get_text(" ", strip=True))
            numeric_id = self._extract_numeric_id(row)
            link = canonicalize_article_link(title_link.get("href", ""), numeric_id)
            published_dt = parse_pub_date(raw_date, self.metrics)

            articles.append(
                {
                    "id": generate_article_id(link, title),
                    "title": title,
                    "description": "",
                    "link": link,
                    "published": to_utc_iso(published_dt),
                    "source": SOURCE_NAME,
                    "feed": FEED_NAME,
                    "category": "General",
                    "categories": ["General"],
                }
            )

        return articles

    def _extract_lead(self, article_url: str) -> str:
        try:
            html = self._get(article_url)
        except requests.RequestException as exc:
            logger.debug("Lead fetch failed (%s): %s", article_url, exc)
            return ""

        soup = BeautifulSoup(html, "html.parser")
        lead = soup.select_one("div.newsstory p.lead")
        if lead:
            return normalize_whitespace(lead.get_text(" ", strip=True))

        for p in soup.select("div.newsstory p"):
            text = normalize_whitespace(p.get_text(" ", strip=True))
            if text and not text.lower().startswith("by "):
                return text
        return ""

    def scrape(
        self,
        max_pages: int = DEFAULT_MAX_PAGES,
        include_lead: bool = False,
        pause: float = 0.2,
    ) -> tuple[list[dict[str, Any]], int, Optional[int]]:
        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        html = self._get(NEWS_ALL_URL)
        all_articles: list[dict[str, Any]] = []
        pages_scraped = 0
        total_pages_hint: Optional[int] = None

        while pages_scraped < max_pages:
            soup = BeautifulSoup(html, "html.parser")
            if total_pages_hint is None:
                total_pages_hint = self._total_pages_hint(soup)

            page_articles = self._parse_page_articles(soup)
            if not page_articles:
                logger.warning("No article rows found on page %s", pages_scraped + 1)
                break

            if include_lead:
                for article in page_articles:
                    article["description"] = self._extract_lead(article["link"])
                    if pause > 0:
                        time.sleep(pause)

            for article in page_articles:
                normalize_article_categories(
                    article,
                    classify_fallback=True,
                    max_categories=MAX_CATEGORIES_PER_ARTICLE,
                )

            all_articles.extend(page_articles)
            pages_scraped += 1

            current_page = self._current_page(soup) or pages_scraped
            next_target = self._next_target(soup, current_page)
            if not next_target:
                break
            if pages_scraped >= max_pages:
                break

            post_fields = self._extract_form_fields(soup)
            post_fields["__EVENTTARGET"] = next_target
            post_fields["__EVENTARGUMENT"] = ""
            if pause > 0:
                time.sleep(pause)
            html = self._post(NEWS_ALL_URL, post_fields)

        return all_articles, pages_scraped, total_pages_hint


def deduplicate(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def deduplicate_with_diagnostics(
    articles: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    return shared_deduplicate_articles(
        articles,
        parse_pub_date=parse_pub_date,
        generate_article_id=generate_article_id,
        max_categories=MAX_CATEGORIES_PER_ARTICLE,
    )


def enforce_category_contract(
    articles: list[dict[str, Any]],
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


def sort_by_date(articles: list[dict[str, Any]], descending: bool = True) -> list[dict[str, Any]]:
    return shared_sort_articles_by_date(
        articles,
        parse_pub_date=parse_pub_date,
        descending=descending,
    )


def load_existing_feed(output_file: Path) -> dict[str, Any]:
    return load_feed_payload(output_file)


def merge_with_existing(
    new_articles: list[dict[str, Any]],
    output_file: Path,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    existing = load_existing_feed(output_file)
    existing_articles = existing.get("articles", [])
    for article in existing_articles:
        normalize_article_published(article)
        article["id"] = generate_article_id(article.get("link", ""), article.get("title", ""))
    enforce_category_contract(existing_articles, classify_fallback=True)
    existing_articles, existing_dedupe = deduplicate_with_diagnostics(existing_articles)

    existing_by_key = {
        canonical_article_dedupe_key(article): article
        for article in existing_articles
        if canonical_article_dedupe_key(article)
    }

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

        merged_article = _merge_duplicate_articles(existing_article, article)
        existing_article.clear()
        existing_article.update(merged_article)
        normalize_article_categories(
            existing_article,
            classify_fallback=True,
            max_categories=MAX_CATEGORIES_PER_ARTICLE,
        )
        updated += 1

    enforce_category_contract(existing_articles, classify_fallback=True)
    merged_articles, post_merge_dedupe = deduplicate_with_diagnostics(existing_articles)
    merged = sort_by_date(merged_articles)
    merged_count = (
        int(existing_dedupe.get("merged", 0))
        + int(post_merge_dedupe.get("merged", 0))
    )
    return merged, {"new": added, "updated": updated, "merged": merged_count}


def save_feed(
    output_file: Path,
    articles: list[dict[str, Any]],
    fetch_stats: dict[str, Any],
    pages_scraped: int,
    total_pages_hint: Optional[int],
    merge_stats: Optional[dict[str, int]] = None,
    category_stats: Optional[dict[str, int]] = None,
    dedupe_stats: Optional[dict[str, int]] = None,
) -> None:
    metadata: dict[str, Any] = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_articles": len(articles),
        "feeds_fetched": fetch_stats.get("success", 0),
        "feeds_failed": fetch_stats.get("failed", 0),
        "feed_details": fetch_stats.get("details", {}),
        "timestamp_parse_errors": int(fetch_stats.get("timestamp_parse_errors", 0)),
        "pages_scraped": pages_scraped,
        "scraper": "argus-newsall-html",
        "canonical_categories": list(CANONICAL_CATEGORIES),
    }
    if total_pages_hint is not None:
        metadata["total_pages_hint"] = total_pages_hint
    if merge_stats:
        metadata["merge"] = merge_stats
    if category_stats:
        metadata["category_contract"] = category_stats
    if dedupe_stats:
        metadata["dedupe"] = dedupe_stats

    save_feed_payload(
        output_file,
        articles=articles,
        metadata=metadata,
    )

    logger.info(
        "Saved %s articles to %s (pages_scraped=%s)",
        len(articles),
        output_file,
        pages_scraped,
    )


def run_once(
    output_file: Path,
    max_pages: int,
    timeout: int,
    include_lead: bool,
    pause: float,
    merge_existing: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any], int, Optional[int]]:
    scraper = ArgusNewsAllScraper(timeout=timeout)
    try:
        articles, pages_scraped, total_pages_hint = scraper.scrape(
            max_pages=max_pages,
            include_lead=include_lead,
            pause=pause,
        )
        articles, incoming_dedupe = deduplicate_with_diagnostics(articles)
        articles = sort_by_date(articles)
        timestamp_parse_errors = int(
            scraper.metrics.get("timestamp_parse_errors", 0)
        )
        fetch_stats = {
            "success": 1 if pages_scraped > 0 else 0,
            "failed": 0 if pages_scraped > 0 else 1,
            "timestamp_parse_errors": timestamp_parse_errors,
            "details": {
                FEED_NAME: {
                    "status": "ok" if pages_scraped > 0 else "failed",
                    "count": len(articles),
                    "pages_scraped": pages_scraped,
                    "total_pages_hint": total_pages_hint,
                    "timestamp_parse_errors": timestamp_parse_errors,
                }
            },
        }
    except Exception as exc:
        logger.error("Scrape failed: %s", exc)
        return [], {
            "success": 0,
            "failed": 1,
            "timestamp_parse_errors": 0,
            "details": {
                FEED_NAME: {
                    "status": "failed",
                    "count": 0,
                    "error": str(exc),
                    "timestamp_parse_errors": 0,
                }
            },
        }, 0, None

    merge_stats: Optional[dict[str, int]] = None
    if merge_existing:
        articles, merge_stats = merge_with_existing(articles, output_file)
    else:
        merge_stats = {"new": len(articles), "updated": 0, "merged": 0}
    category_stats = enforce_category_contract(articles, classify_fallback=True)
    run_dedupe_stats = {
        "new": int((merge_stats or {}).get("new", len(articles))),
        "updated": int((merge_stats or {}).get("updated", 0)),
        "merged": int(incoming_dedupe.get("merged", 0))
        + int((merge_stats or {}).get("merged", 0)),
        "incoming_merged": int(incoming_dedupe.get("merged", 0)),
        "existing_merged": int((merge_stats or {}).get("merged", 0)),
    }
    logger.info(
        "Dedupe diagnostics: new=%s updated=%s merged=%s",
        run_dedupe_stats["new"],
        run_dedupe_stats["updated"],
        run_dedupe_stats["merged"],
    )

    save_feed(
        output_file=output_file,
        articles=articles,
        fetch_stats=fetch_stats,
        pages_scraped=pages_scraped,
        total_pages_hint=total_pages_hint,
        merge_stats=merge_stats,
        category_stats=category_stats,
        dedupe_stats=run_dedupe_stats,
    )
    return articles, fetch_stats, pages_scraped, total_pages_hint


def run_daemon(
    output_file: Path,
    interval: int,
    max_pages: int,
    timeout: int,
    include_lead: bool,
    pause: float,
    merge_existing: bool,
) -> None:
    logger.info("Starting daemon mode; polling every %ss", interval)
    while True:
        try:
            run_once(
                output_file=output_file,
                max_pages=max_pages,
                timeout=timeout,
                include_lead=include_lead,
                pause=pause,
                merge_existing=merge_existing,
            )
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
            break
        except Exception as exc:
            logger.error("Daemon cycle failed: %s", exc)

        logger.info("Next poll in %ss...", interval)
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Argus NewsAll HTML into JSON")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output JSON file (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Maximum pages to scrape (default: {DEFAULT_MAX_PAGES})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=REQUEST_TIMEOUT,
        help=f"HTTP request timeout in seconds (default: {REQUEST_TIMEOUT})",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=0.2,
        help="Sleep between page/article requests in seconds (default: 0.2)",
    )
    parser.add_argument(
        "--include-lead",
        action="store_true",
        help="Fetch each article page and store the lead paragraph as description",
    )
    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="Do not merge with existing output file; overwrite with fresh scrape",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously and poll at an interval",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Polling interval in seconds for daemon mode (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    output_file = Path(args.output).expanduser().resolve()
    merge_existing = not args.no_merge

    if args.daemon:
        run_daemon(
            output_file=output_file,
            interval=args.interval,
            max_pages=args.max_pages,
            timeout=args.timeout,
            include_lead=args.include_lead,
            pause=args.pause,
            merge_existing=merge_existing,
        )
        return

    articles, stats, pages_scraped, total_pages_hint = run_once(
        output_file=output_file,
        max_pages=args.max_pages,
        timeout=args.timeout,
        include_lead=args.include_lead,
        pause=args.pause,
        merge_existing=merge_existing,
    )

    print(f"\nDone! {len(articles)} articles saved to {output_file}")
    print(f"Feeds OK: {stats['success']} | Failed: {stats['failed']}")
    print(f"Pages scraped: {pages_scraped}")
    if total_pages_hint is not None:
        print(f"Estimated total pages: {total_pages_hint}")
    detail = stats.get("details", {}).get(FEED_NAME, {})
    print(f"Feed: {FEED_NAME} ({detail.get('status', 'unknown')})")


if __name__ == "__main__":
    main()
