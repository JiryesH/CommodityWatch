from __future__ import annotations

from dataclasses import dataclass

import pytest

from ner_spacy import (
    NERConfig,
    SpacyNERExtractor,
    build_ner_text,
    normalize_country_reference,
    scan_country_hints,
    should_store_entity,
    strip_description_dateline,
)


@dataclass
class FakeEnt:
    text: str
    label_: str


@dataclass
class FakeDoc:
    text: str
    ents: list[FakeEnt]


class TestDatelineStripping:
    @pytest.mark.parametrize(
        ("description", "expected"),
        [
            (
                "LONDON (ICIS)--European naphtha prices are rising.",
                "European naphtha prices are rising.",
            ),
            (
                "LONDON(ICIS)---Poland's Orlen reports stronger demand.",
                "Poland's Orlen reports stronger demand.",
            ),
            (
                "by Ed Cox LONDON (ICIS)--Qatar has confirmed force majeure.",
                "Qatar has confirmed force majeure.",
            ),
            (
                "(Correction: Dates updated) HOUSTON (ICIS)--Dow has declared order control.",
                "Dow has declared order control.",
            ),
        ],
    )
    def test_strip_description_dateline(self, description: str, expected: str):
        assert strip_description_dateline(description) == expected

    def test_build_ner_text_strips_dateline_only_in_description_mode(self):
        article = {
            "title": "Dow tightens MMA supply",
            "description": "HOUSTON (ICIS)--Dow has declared order control.",
        }
        assert build_ner_text(article, True) == "Dow tightens MMA supply. Dow has declared order control."
        assert build_ner_text(article, False) == "Dow tightens MMA supply"


class TestCountryNormalization:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("Indian", "India"),
            ("Guyanese", "Guyana"),
            ("French", "France"),
            ("German", "Germany"),
            ("Venezuelan", "Venezuela"),
            ("Indonesian", "Indonesia"),
            ("S Korea", "South Korea"),
            ("US", "United States"),
            ("UK", "United Kingdom"),
            ("UAE", "United Arab Emirates"),
            ("Alaska", "United States"),
            ("Texas", "United States"),
            ("East China", "China"),
            ("Fujairah", "United Arab Emirates"),
            ("Ras Tanura", "Saudi Arabia"),
            ("Ruwais", "United Arab Emirates"),
            ("Cabinda", "Angola"),
            ("Houston", "United States"),
            ("Lake Charles", "United States"),
            ("New York", "United States"),
            ("Queensland", "Australia"),
            ("Onsan", "South Korea"),
            ("Zhejiang", "China"),
        ],
    )
    def test_normalize_country_reference_for_explicit_mappings(
        self, value: str, expected: str
    ):
        assert normalize_country_reference(value) == expected

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("US E15 group floats fewer biofuel mandate waivers", ["United States"]),
            ("Plant status: S Korea's S-Oil commences turnaround", ["South Korea"]),
            ("UAE's Fujairah bunker suppliers declare force majeure", ["United Arab Emirates"]),
            ("Cabinda refinery still in testing, no supply yet", ["Angola"]),
            ("Lake Charles LNG terminates some offtake contracts", ["United States"]),
            ("Queensland met coal royalties hit producers", ["Australia"]),
            ("Plant status: China's ZPC slightly cuts cracker operating rate in Zhejiang", ["China"]),
        ],
    )
    def test_scan_country_hints(self, text: str, expected: list[str]):
        assert scan_country_hints(text) == expected

    @pytest.mark.parametrize(
        ("text", "expected_country"),
        [
            ("Alaska North Slope crude hits record high: Correction", "United States"),
            ("Woodside achieves first ammonia output at Texas plant", "United States"),
            (
                "East China MEG inventories rise further due to limited recovery in offtakes",
                "China",
            ),
            (
                "Macroeconomic headwinds to be felt in 2024 China Sep oil demand hits all time high on petchem boom",
                "China",
            ),
        ],
    )
    def test_extract_from_doc_scans_country_text_when_spacy_misses(
        self,
        text: str,
        expected_country: str,
    ):
        extractor = SpacyNERExtractor(NERConfig())
        doc = FakeDoc(text=text, ents=[])

        _, countries = extractor._extract_from_doc(doc)

        assert expected_country in countries

    def test_false_positive_country_inputs_are_rejected(self):
        assert normalize_country_reference("Yara") is None
        assert normalize_country_reference("Europe") is None
        assert normalize_country_reference("Middle East") is None
        assert scan_country_hints("The conflict leaves us exposed.") == []
        assert scan_country_hints("Virgin Islands, U.S. customs zone") == []


class TestEntityFiltering:
    @pytest.mark.parametrize(
        "label",
        ["DATE", "TIME", "MONEY", "PERCENT", "QUANTITY", "ORDINAL", "CARDINAL"],
    )
    def test_low_value_labels_are_filtered(self, label: str):
        assert should_store_entity("6 March", label) is False

    @pytest.mark.parametrize(
        "text",
        [
            "ICIS",
            "ICIS)--Here",
            "LNG",
            "MEG",
            "PX",
            "MMA",
            "GMAA",
            "RFCC",
            "Capacity",
            "Location",
            "Q1",
            "No 2",
        ],
    )
    def test_noise_entities_are_filtered(self, text: str):
        assert should_store_entity(text, "ORG") is False

    def test_entity_normalization_deduplicates_case_insensitively(self):
        extractor = SpacyNERExtractor(NERConfig())
        doc = FakeDoc(
            text="the Middle East and Zimbabwe and zimbabwe remain in focus.",
            ents=[
                FakeEnt("the Middle East", "GPE"),
                FakeEnt("Zimbabwe", "GPE"),
                FakeEnt("zimbabwe", "GPE"),
            ],
        )

        entities, _ = extractor._extract_from_doc(doc)

        assert entities == [
            {"text": "Middle East", "label": "LOC"},
            {"text": "Zimbabwe", "label": "GPE"},
        ]

    def test_targeted_label_corrections_and_text_hints(self):
        extractor = SpacyNERExtractor(NERConfig())
        doc = FakeDoc(
            text="Sinopec Trump Maduro Methanex Hormuz Strait of Hormuz Ras Tanura Valero Yara CBAM Opec+",
            ents=[
                FakeEnt("Sinopec", "PERSON"),
                FakeEnt("Trump", "ORG"),
                FakeEnt("Maduro", "GPE"),
                FakeEnt("Methanex", "PERSON"),
                FakeEnt("Hormuz", "GPE"),
                FakeEnt("Strait of Hormuz", "GPE"),
                FakeEnt("Ras Tanura", "PERSON"),
                FakeEnt("Valero", "PRODUCT"),
                FakeEnt("Yara", "GPE"),
                FakeEnt("CBAM", "ORG"),
            ],
        )

        entities, _ = extractor._extract_from_doc(doc)

        assert entities == [
            {"text": "Sinopec", "label": "ORG"},
            {"text": "Trump", "label": "PERSON"},
            {"text": "Maduro", "label": "PERSON"},
            {"text": "Methanex", "label": "ORG"},
            {"text": "Hormuz", "label": "LOC"},
            {"text": "Strait of Hormuz", "label": "LOC"},
            {"text": "Ras Tanura", "label": "GPE"},
            {"text": "Valero", "label": "ORG"},
            {"text": "Yara", "label": "ORG"},
            {"text": "CBAM", "label": "LAW"},
            {"text": "OPEC+", "label": "ORG"},
        ]

    def test_span_canonicalization_and_split(self):
        extractor = SpacyNERExtractor(NERConfig())
        doc = FakeDoc(
            text="US E15 group floats fewer biofuel mandate waivers while UAE Fujairah bunker market braced for impact.",
            ents=[
                FakeEnt("US E15", "ORG"),
                FakeEnt("UAE Fujairah", "ORG"),
            ],
        )

        entities, countries = extractor._extract_from_doc(doc)

        assert entities == [
            {"text": "US E15 group", "label": "ORG"},
            {"text": "UAE", "label": "GPE"},
            {"text": "Fujairah", "label": "GPE"},
        ]
        assert countries == ["United States", "United Arab Emirates"]

    def test_plant_status_cleanup_repairs_field_label_fragments(self):
        extractor = SpacyNERExtractor(NERConfig())
        doc = FakeDoc(
            text="BASF Location: Ludwigshafen, Germany. Refining Location: Ruwais, United Arab Emirates. Capacity: 705,000.",
            ents=[
                FakeEnt("BASF Location:", "ORG"),
                FakeEnt("Refining Location: Ruwais", "ORG"),
                FakeEnt("Capacity", "ORG"),
            ],
        )

        entities, countries = extractor._extract_from_doc(doc)

        assert entities == [
            {"text": "BASF", "label": "ORG"},
            {"text": "Ruwais", "label": "GPE"},
        ]
        assert "United Arab Emirates" in countries

    def test_suffix_cleanup_repairs_bad_spans_without_dropping_facilities(self):
        extractor = SpacyNERExtractor(NERConfig())
        doc = FakeDoc(
            text="China Sep oil demand hits a high while East China MEG inventories rise. Lake Charles LNG terminates some offtake contracts.",
            ents=[
                FakeEnt("China Sep", "EVENT"),
                FakeEnt("East China MEG", "PERSON"),
                FakeEnt("Lake Charles LNG", "FAC"),
                FakeEnt("No 2", "WORK_OF_ART"),
            ],
        )

        entities, countries = extractor._extract_from_doc(doc)

        assert entities == [
            {"text": "China", "label": "GPE"},
            {"text": "East China", "label": "LOC"},
            {"text": "Lake Charles LNG", "label": "FAC"},
        ]
        assert countries == ["China", "United States"]

    def test_extract_from_doc_filters_noise_and_keeps_useful_entities(self):
        extractor = SpacyNERExtractor(NERConfig(max_entities=2))
        doc = FakeDoc(
            text="US E15 group floats fewer biofuel mandate waivers while BASF expands in Houston.",
            ents=[
                FakeEnt("March 6", "DATE"),
                FakeEnt("ICIS", "ORG"),
                FakeEnt("LNG", "ORG"),
                FakeEnt("BASF", "ORG"),
                FakeEnt("Houston", "GPE"),
            ],
        )

        entities, countries = extractor._extract_from_doc(doc)

        assert entities == [
            {"text": "BASF", "label": "ORG"},
            {"text": "Houston", "label": "GPE"},
        ]
        assert countries == ["United States"]
