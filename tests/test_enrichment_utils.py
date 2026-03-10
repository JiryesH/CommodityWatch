"""Tests for shared enrichment utility functions."""

from enrichment_utils import build_enrichment_text, enrichment_text_hash


class TestBuildEnrichmentText:
    def test_title_only(self):
        article = {"title": "Oil prices rise sharply"}
        assert build_enrichment_text(article, use_description=False) == "Oil prices rise sharply"

    def test_title_with_description_enabled(self):
        article = {"title": "Oil rises", "description": "Brent crude up 3%."}
        assert build_enrichment_text(article, use_description=True) == "Oil rises. Brent crude up 3%."

    def test_description_flag_false_ignores_description(self):
        article = {"title": "Oil rises", "description": "Brent crude up 3%."}
        assert build_enrichment_text(article, use_description=False) == "Oil rises"

    def test_empty_description_falls_back_to_title(self):
        article = {"title": "Oil rises", "description": ""}
        assert build_enrichment_text(article, use_description=True) == "Oil rises"

    def test_none_description_falls_back_to_title(self):
        article = {"title": "Oil rises", "description": None}
        assert build_enrichment_text(article, use_description=True) == "Oil rises"

    def test_missing_title_returns_empty(self):
        article = {}
        assert build_enrichment_text(article, use_description=False) == ""

    def test_whitespace_normalization(self):
        article = {"title": "  Oil   prices   rise  "}
        assert build_enrichment_text(article, use_description=False) == "Oil prices rise"

    def test_none_title(self):
        article = {"title": None}
        assert build_enrichment_text(article, use_description=False) == ""


class TestEnrichmentTextHash:
    def test_deterministic(self):
        h1 = enrichment_text_hash("Oil prices rise")
        h2 = enrichment_text_hash("Oil prices rise")
        assert h1 == h2

    def test_different_text_different_hash(self):
        h1 = enrichment_text_hash("Oil prices rise")
        h2 = enrichment_text_hash("Gas prices fall")
        assert h1 != h2

    def test_empty_string(self):
        h = enrichment_text_hash("")
        assert isinstance(h, str) and len(h) == 64  # SHA-256 hex length


class TestBackwardCompatibility:
    """Verify that the wrappers in sentiment_finbert and ner_spacy
    produce identical results to the shared implementation."""

    def test_sentiment_wrapper_matches(self):
        from sentiment_finbert import build_sentiment_text, sentiment_text_hash

        article = {"title": "Gold surges", "description": "Spot gold up 2%."}
        assert build_sentiment_text(article, True) == build_enrichment_text(article, True)
        assert build_sentiment_text(article, False) == build_enrichment_text(article, False)

        text = "Gold surges"
        assert sentiment_text_hash(text) == enrichment_text_hash(text)

    def test_ner_wrapper_matches(self):
        from ner_spacy import build_ner_text, ner_text_hash

        article = {"title": "Copper falls", "description": "LME copper down 1%."}
        assert build_ner_text(article, True) == build_enrichment_text(article, True)
        assert build_ner_text(article, False) == build_enrichment_text(article, False)

        text = "Copper falls"
        assert ner_text_hash(text) == enrichment_text_hash(text)
