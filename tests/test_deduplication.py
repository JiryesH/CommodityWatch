import copy
import unittest

import argus_scraper
import rss_scraper


def _article(
    *,
    title: str,
    link: str,
    category: str,
    source: str,
    feed: str,
    description: str = "",
) -> dict:
    return {
        "id": "legacy-id",
        "title": title,
        "description": description,
        "link": link,
        "published": "2026-03-05T00:00:00+00:00",
        "source": source,
        "feed": feed,
        "category": category,
        "categories": [category],
    }


class DedupeStrategyTests(unittest.TestCase):
    def test_rss_deduplicate_merges_normalized_url_duplicates(self) -> None:
        articles = [
            _article(
                title="Duplicate headline",
                link="https://example.com/news/1/?utm_source=rss&a=1&b=2",
                category="Oil - Crude",
                source="ICIS",
                feed="Feed A",
                description="short",
            ),
            _article(
                title="Duplicate headline",
                link="https://EXAMPLE.com/news/1?b=2&a=1#frag",
                category="Shipping",
                source="S&P Global",
                feed="Feed B",
                description="a little longer",
            ),
            _article(
                title="Duplicate headline",
                link="https://example.com//news/1/?a=1&b=2",
                category="Metals",
                source="Fastmarkets",
                feed="Feed C",
            ),
        ]

        deduped, stats = rss_scraper.deduplicate_with_diagnostics(copy.deepcopy(articles))
        self.assertEqual(stats["input"], 3)
        self.assertEqual(stats["new"], 1)
        self.assertEqual(stats["merged"], 2)
        self.assertEqual(len(deduped), 1)

        merged = deduped[0]
        # Deterministic canonical ordering + hard cap at MAX_CATEGORIES_PER_ARTICLE (2).
        self.assertEqual(merged["categories"], ["Oil - Crude", "Metals"])
        self.assertEqual(merged["category"], "Oil - Crude")

    def test_rss_deduplicate_uses_title_fallback_when_link_missing(self) -> None:
        articles = [
            _article(
                title="OPEC raises output target",
                link="",
                category="General",
                source="ICIS",
                feed="Feed A",
            ),
            _article(
                title="  opec   raises   output   target ",
                link="   ",
                category="Shipping",
                source="S&P Global",
                feed="Feed B",
            ),
        ]

        deduped, stats = rss_scraper.deduplicate_with_diagnostics(copy.deepcopy(articles))
        self.assertEqual(stats["new"], 1)
        self.assertEqual(stats["merged"], 1)
        self.assertEqual(len(deduped), 1)

    def test_rss_deduplicate_keeps_same_title_with_distinct_links(self) -> None:
        articles = [
            _article(
                title="Shared title",
                link="https://source-a.example/news/42",
                category="General",
                source="Source A",
                feed="Feed A",
            ),
            _article(
                title="Shared title",
                link="https://source-b.example/news/42",
                category="General",
                source="Source B",
                feed="Feed B",
            ),
        ]

        deduped, stats = rss_scraper.deduplicate_with_diagnostics(copy.deepcopy(articles))
        self.assertEqual(stats["new"], 2)
        self.assertEqual(stats["merged"], 0)
        self.assertEqual(len(deduped), 2)

    def test_argus_deduplicate_reports_diagnostics(self) -> None:
        articles = [
            _article(
                title="Argus Duplicate",
                link="https://www.argusmedia.com/pages/NewsBody.aspx?id=123&menu=yes&utm_source=abc",
                category="General",
                source="Argus Media",
                feed="Argus NewsAll",
            ),
            _article(
                title="Argus Duplicate",
                link="https://www.argusmedia.com/pages/NewsBody.aspx?id=123&menu=yes",
                category="General",
                source="Argus Media",
                feed="Argus NewsAll",
            ),
        ]

        deduped, stats = argus_scraper.deduplicate_with_diagnostics(copy.deepcopy(articles))
        self.assertEqual(stats["new"], 1)
        self.assertEqual(stats["merged"], 1)
        self.assertEqual(len(deduped), 1)


if __name__ == "__main__":
    unittest.main()
