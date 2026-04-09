import copy
import json
import unittest
from datetime import timezone
from pathlib import Path
from unittest import mock

import argus_scraper
import rss_scraper


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "timestamp_cases.json"


class TimestampNormalizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_rss_parse_cases(self) -> None:
        for case in self.fixture["parse_cases"]:
            dt = rss_scraper.parse_pub_date(case["raw"])
            got = dt.isoformat() if dt else None
            self.assertEqual(got, case["expected_utc_iso"], msg=case["raw"])

    def test_argus_parse_cases(self) -> None:
        for case in self.fixture["parse_cases"]:
            dt = argus_scraper.parse_pub_date(case["raw"])
            got = dt.isoformat() if dt else None
            self.assertEqual(got, case["expected_utc_iso"], msg=case["raw"])

    def test_rss_parse_feed_entries_sets_null_and_counts_errors(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Test Feed</title>
            <item>
              <title>Valid timestamp</title>
              <link>https://example.com/1</link>
              <pubDate>Wed, 04 Mar 2026 09:37:00 +0200</pubDate>
            </item>
            <item>
              <title>Invalid timestamp</title>
              <link>https://example.com/2</link>
              <pubDate>BAD-DATE-TOKEN</pubDate>
            </item>
          </channel>
        </rss>
        """
        articles, parse_errors = rss_scraper.parse_feed_entries(
            xml,
            "Fixture Feed",
            {"source": "Fixture", "category": "General"},
        )

        self.assertEqual(len(articles), 2)
        self.assertEqual(parse_errors, 1)
        self.assertEqual(
            articles[0]["published"],
            "2026-03-04T07:37:00+00:00",
        )
        self.assertIsNone(articles[1]["published"])

    def test_rss_parse_pub_date_with_explicit_feed_timezone_hint(self) -> None:
        dt = rss_scraper.parse_pub_date(
            "Fri, 06 Mar 2026 07:36",
            default_tz=timezone.utc,
        )
        self.assertIsNotNone(dt)
        self.assertEqual(dt.isoformat(), "2026-03-06T07:36:00+00:00")

    def test_rss_feed_timezone_hint_parses_naive_ici_style_date(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Test Feed</title>
            <item>
              <title>Naive ICIS-style date</title>
              <link>https://example.com/icis-1</link>
              <pubDate>Fri, 06 Mar 2026 07:36</pubDate>
            </item>
          </channel>
        </rss>
        """
        articles, parse_errors = rss_scraper.parse_feed_entries(
            xml,
            "ICIS Fixture Feed",
            {"source": "ICIS", "category": "General", "timezone_hint": "UTC"},
        )

        self.assertEqual(parse_errors, 0)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["published"], "2026-03-06T07:36:00+00:00")

    def test_argus_parse_error_metric_counts_only_invalid(self) -> None:
        metrics = {"timestamp_parse_errors": 0}
        ok_dt = argus_scraper.parse_pub_date("06 Mar 2026 05:13 GMT", metrics)
        bad_dt = argus_scraper.parse_pub_date("06 Mar 2026 05:13", metrics)

        self.assertIsNotNone(ok_dt)
        self.assertIsNone(bad_dt)
        self.assertEqual(metrics["timestamp_parse_errors"], 1)

    def test_argus_parse_parenthesized_offset_format(self) -> None:
        metrics = {"timestamp_parse_errors": 0}
        dt = argus_scraper.parse_pub_date(
            "09 Apr 2026 12:26 (+01:00 GMT)",
            metrics,
        )

        self.assertIsNotNone(dt)
        self.assertEqual(dt.isoformat(), "2026-04-09T11:26:00+00:00")
        self.assertEqual(metrics["timestamp_parse_errors"], 0)

    def test_scrape_argus_marks_all_invalid_timestamps_as_failed(self) -> None:
        class FakeScraper:
            def __init__(self, timeout: int = 20) -> None:
                self.timeout = timeout
                self.metrics = {"timestamp_parse_errors": 2}

            def scrape(
                self,
                max_pages: int = 1,
                include_lead: bool = False,
                pause: float = 0.0,
            ):
                return (
                    [
                        {
                            "id": "1",
                            "title": "Broken 1",
                            "description": "",
                            "link": "https://example.com/1",
                            "published": None,
                            "source": "Argus Media",
                            "feed": "Argus NewsAll",
                            "category": "General",
                            "categories": ["General"],
                        },
                        {
                            "id": "2",
                            "title": "Broken 2",
                            "description": "",
                            "link": "https://example.com/2",
                            "published": None,
                            "source": "Argus Media",
                            "feed": "Argus NewsAll",
                            "category": "General",
                            "categories": ["General"],
                        },
                    ],
                    1,
                    30,
                )

        with mock.patch.object(rss_scraper.argus_scraper, "ArgusNewsAllScraper", FakeScraper):
            articles, detail = rss_scraper.scrape_argus(max_pages=1)

        self.assertEqual(len(articles), 2)
        self.assertEqual(detail["status"], "failed")
        self.assertEqual(
            detail["error"],
            "Argus returned article rows, but none had parseable timestamps.",
        )
        self.assertEqual(detail["valid_timestamp_count"], 0)
        self.assertEqual(detail["missing_timestamp_count"], 2)

    def test_rss_sort_cases(self) -> None:
        for case in self.fixture["sort_cases"]:
            articles = copy.deepcopy(case["articles"])
            sorted_articles = rss_scraper.sort_by_date(
                articles,
                descending=case["descending"],
            )
            self.assertEqual(
                [a.get("id") for a in sorted_articles],
                case["expected_ids"],
            )
            invalid = [a for a in sorted_articles if a.get("id") in ("a", "b")]
            self.assertTrue(all(a.get("published") is None for a in invalid))

    def test_argus_sort_cases(self) -> None:
        for case in self.fixture["sort_cases"]:
            articles = copy.deepcopy(case["articles"])
            sorted_articles = argus_scraper.sort_by_date(
                articles,
                descending=case["descending"],
            )
            self.assertEqual(
                [a.get("id") for a in sorted_articles],
                case["expected_ids"],
            )
            invalid = [a for a in sorted_articles if a.get("id") in ("a", "b")]
            self.assertTrue(all(a.get("published") is None for a in invalid))


if __name__ == "__main__":
    unittest.main()
