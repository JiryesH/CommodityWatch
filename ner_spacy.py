#!/usr/bin/env python3
"""
Incremental spaCy NER extraction for commodity headlines.

This module can be:
1) Imported by rss_scraper.py to extract entities in-memory.
2) Run standalone to enrich a JSON feed file.
"""

from __future__ import annotations

import argparse
import html
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from enrichment_utils import build_enrichment_text, enrichment_text_hash
from feed_io import (
    ensure_feed_metadata,
    load_feed_json as load_feed,
    save_feed_json as save_feed,
)

try:
    import spacy

    HAS_NER_DEPS = True
except ImportError:
    HAS_NER_DEPS = False
    spacy = None


COUNTRY_ENTITY_LABELS = {"GPE", "LOC", "NORP"}
LOW_VALUE_ENTITY_LABELS = {
    "CARDINAL",
    "DATE",
    "MONEY",
    "ORDINAL",
    "PERCENT",
    "QUANTITY",
    "TIME",
}
GENERIC_COMMODITY_ENTITY_KEYS = {"lng", "meg", "px", "mma", "gmaa", "rfcc"}
NOISE_ENTITY_KEYS = {"icis"}
COUNTRY_DISPLAY_OVERRIDES = {
    "Iran, Islamic Republic of": "Iran",
    "Korea, Republic of": "South Korea",
    "Korea, Democratic People's Republic of": "North Korea",
    "Russian Federation": "Russia",
    "Syrian Arab Republic": "Syria",
    "Venezuela, Bolivarian Republic of": "Venezuela",
    "Bolivia, Plurinational State of": "Bolivia",
    "Moldova, Republic of": "Moldova",
    "Tanzania, United Republic of": "Tanzania",
    "Taiwan, Province of China": "Taiwan",
    "Viet Nam": "Vietnam",
    "Lao People's Democratic Republic": "Laos",
    "Brunei Darussalam": "Brunei",
}
COUNTRY_EXCLUDED_KEYS = {
    "asia",
    "europe",
    "hormuz",
    "mediterranean",
    "middle east",
    "mideast",
    "strait of hormuz",
}
COUNTRY_ALIAS_HINTS: tuple[tuple[str, str, bool, bool], ...] = (
    ("US", "United States", True, True),
    ("U.S.", "United States", True, True),
    ("USA", "United States", True, False),
    ("UK", "United Kingdom", True, True),
    ("U.K.", "United Kingdom", True, True),
    ("UAE", "United Arab Emirates", True, False),
    ("U.A.E.", "United Arab Emirates", True, False),
    ("Britain", "United Kingdom", False, False),
    ("S Korea", "South Korea", False, False),
    ("S. Korea", "South Korea", False, False),
)
COUNTRY_DEMONYM_HINT_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Angola", ("Angolan",)),
    ("Argentina", ("Argentine", "Argentinian")),
    ("Australia", ("Australian",)),
    ("Austria", ("Austrian",)),
    ("Bahrain", ("Bahraini",)),
    ("Belgium", ("Belgian",)),
    ("Brazil", ("Brazilian",)),
    ("Brunei", ("Bruneian",)),
    ("Canada", ("Canadian",)),
    ("Chile", ("Chilean",)),
    ("China", ("Chinese",)),
    ("Colombia", ("Colombian",)),
    ("Denmark", ("Danish",)),
    ("Egypt", ("Egyptian",)),
    ("France", ("French",)),
    ("Germany", ("German",)),
    ("Greece", ("Greek",)),
    ("Guyana", ("Guyanese",)),
    ("India", ("Indian",)),
    ("Indonesia", ("Indonesian",)),
    ("Iran", ("Iranian",)),
    ("Iraq", ("Iraqi",)),
    ("Ireland", ("Irish",)),
    ("Israel", ("Israeli",)),
    ("Italy", ("Italian",)),
    ("Japan", ("Japanese",)),
    ("Kazakhstan", ("Kazakh", "Kazakhstani")),
    ("Kuwait", ("Kuwaiti",)),
    ("Malaysia", ("Malaysian",)),
    ("Mexico", ("Mexican",)),
    ("Morocco", ("Moroccan",)),
    ("Netherlands", ("Dutch",)),
    ("Nigeria", ("Nigerian",)),
    ("Norway", ("Norwegian",)),
    ("Oman", ("Omani",)),
    ("Pakistan", ("Pakistani",)),
    ("Peru", ("Peruvian",)),
    ("Philippines", ("Filipino", "Philippine")),
    ("Poland", ("Polish",)),
    ("Portugal", ("Portuguese",)),
    ("Qatar", ("Qatari",)),
    ("Romania", ("Romanian",)),
    ("Russia", ("Russian",)),
    ("Saudi Arabia", ("Saudi Arabian",)),
    ("Singapore", ("Singaporean",)),
    ("South Africa", ("South African",)),
    ("South Korea", ("S Korean", "S. Korean", "South Korean")),
    ("Spain", ("Spanish",)),
    ("Sweden", ("Swedish",)),
    ("Switzerland", ("Swiss",)),
    ("Taiwan", ("Taiwanese",)),
    ("Thailand", ("Thai",)),
    ("Turkey", ("Turkish",)),
    ("Ukraine", ("Ukrainian",)),
    ("United Arab Emirates", ("Emirati",)),
    ("United Kingdom", ("British",)),
    ("Uruguay", ("Uruguayan",)),
    ("Venezuela", ("Venezuelan",)),
    ("Vietnam", ("Vietnamese",)),
    ("Zimbabwe", ("Zimbabwean",)),
)
COUNTRY_SUBNATIONAL_PLACE_HINTS = {
    "Abu Dhabi": "United Arab Emirates",
    "Alaska": "United States",
    "Bilbao": "Spain",
    "Cabinda": "Angola",
    "East China": "China",
    "Fujairah": "United Arab Emirates",
    "Houston": "United States",
    "Lake Charles": "United States",
    "New York": "United States",
    "Onsan": "South Korea",
    "Queensland": "Australia",
    "Ras Tanura": "Saudi Arabia",
    "Ruwais": "United Arab Emirates",
    "Sabine Pass": "United States",
    "Texas": "United States",
    "Zhejiang": "China",
}
COUNTRY_CONTEXT_BLOCKED_FOLLOWERS = ("time",)
HTML_TAG_RE = re.compile(r"<[^>]+>")
STRUCTURED_FIELD_LABELS = (
    "Refining Location",
    "Location",
    "Name",
    "Products",
    "Product",
    "Refining capacity",
    "Capacity (tonnes/year)",
    "Capacity",
    "Event start",
    "Event finish",
)
STRUCTURED_FIELD_PATTERN = "|".join(
    sorted((re.escape(label) for label in STRUCTURED_FIELD_LABELS), key=len, reverse=True)
)
STRUCTURED_FIELD_BREAK_RE = re.compile(
    rf"\s+(?=(?:{STRUCTURED_FIELD_PATTERN}):)",
    re.IGNORECASE,
)
STRUCTURED_FIELD_PREFIX_RE = re.compile(
    rf"^(?P<label>{STRUCTURED_FIELD_PATTERN})\s*:\s*(?P<value>.+)$",
    re.IGNORECASE,
)
STRUCTURED_FIELD_SPLIT_RE = re.compile(
    rf"^(?P<left>.+?)\s+(?P<label>{STRUCTURED_FIELD_PATTERN})\s*:\s*(?P<right>.+)$",
    re.IGNORECASE,
)
STRUCTURED_FIELD_SUFFIX_RE = re.compile(
    rf"\s+(?:{STRUCTURED_FIELD_PATTERN})\s*:?\s*$",
    re.IGNORECASE,
)
STRUCTURED_FIELD_ENTITY_LABELS = {
    "location": "GPE",
    "refining location": "GPE",
}
STRUCTURED_FIELD_BLOCK_KEYS = {
    re.sub(r"[^\w]+", "", label).lower() for label in STRUCTURED_FIELD_LABELS
}
ENTITY_EXACT_CORRECTIONS = {
    "alaska": ("Alaska", "GPE"),
    "asia": ("Asia", "LOC"),
    "cbam": ("CBAM", "LAW"),
    "china": ("China", "GPE"),
    "east china": ("East China", "LOC"),
    "europe": ("Europe", "LOC"),
    "fujairah": ("Fujairah", "GPE"),
    "hormuz": ("Hormuz", "LOC"),
    "iea": ("IEA", "ORG"),
    "international energy agency": ("IEA", "ORG"),
    "maduro": ("Maduro", "PERSON"),
    "mediterranean": ("Mediterranean", "LOC"),
    "methanex": ("Methanex", "ORG"),
    "middle east": ("Middle East", "LOC"),
    "north slope": ("North Slope", "LOC"),
    "opec+": ("OPEC+", "ORG"),
    "onsan": ("Onsan", "GPE"),
    "ras tanura": ("Ras Tanura", "GPE"),
    "s korea": ("S Korea", "GPE"),
    "sinopec": ("Sinopec", "ORG"),
    "strait of hormuz": ("Strait of Hormuz", "LOC"),
    "texas": ("Texas", "GPE"),
    "trump": ("Trump", "PERSON"),
    "uae": ("UAE", "GPE"),
    "valero": ("Valero", "ORG"),
    "yara": ("Yara", "ORG"),
}
ENTITY_NOISE_PATTERNS = (
    re.compile(r"^No\.?\s+\d+$", re.IGNORECASE),
    re.compile(r"^Q[1-4](?:\s+\d{4})?$", re.IGNORECASE),
    re.compile(r"^H[12](?:\s+\d{4})?$", re.IGNORECASE),
)
ENTITY_MONTH_SUFFIX_RE = re.compile(
    r"^(?P<base>.+?)\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)$",
    re.IGNORECASE,
)
ENTITY_COMMODITY_SUFFIX_RE = re.compile(
    r"^(?P<base>.+?)\s+(?:LNG|MEG|PX|MMA|GMAA|RFCC)$",
    re.IGNORECASE,
)
TEXT_ENTITY_HINT_PATTERNS: tuple[tuple[re.Pattern[str], str, str], ...] = (
    (re.compile(r"(?<!\w)US E15 group(?!\w)"), "US E15 group", "ORG"),
    (re.compile(r"(?<!\w)Opec\+(?!\w)", re.IGNORECASE), "OPEC+", "ORG"),
    (re.compile(r"(?<!\w)Sinopec(?!\w)", re.IGNORECASE), "Sinopec", "ORG"),
)
WIRE_SOURCE_PATTERN = r"(?:ICIS|ARGUS|FASTMARKETS|S&P GLOBAL|S&P|PLATTS)"
WIRE_DATELINE_RE = re.compile(
    rf"""
    ^\s*
    (?:(?:\([^)]{{0,160}}\)|by\s+[A-Z][\w .'-]{{1,80}})\s+)*
    [A-Z][A-Z .,'/&-]{{1,80}}?
    \s*
    \(\s*{WIRE_SOURCE_PATTERN}\s*\)
    \s*---?\s*
    """,
    re.VERBOSE | re.IGNORECASE,
)


@dataclass
class NERConfig:
    enabled: bool = True
    model_name: str = "en_core_web_lg"
    batch_size: int = 64
    use_description: bool = False
    force_rescore: bool = False
    max_entities: int = 18


def _collapse_whitespace(value: Any) -> str:
    text = str(value or "").replace("\u200b", " ").replace("\ufeff", " ")
    return re.sub(r"\s+", " ", text).strip()


def strip_description_dateline(description: str) -> str:
    stripped = _collapse_whitespace(description)
    while stripped:
        updated = WIRE_DATELINE_RE.sub("", stripped, count=1).lstrip(" -:;")
        if updated == stripped:
            break
        stripped = updated
    return stripped


def preprocess_description_for_ner(description: str) -> str:
    text = html.unescape(str(description or ""))
    text = HTML_TAG_RE.sub(" ", text)
    text = strip_description_dateline(text)
    text = re.sub(r"^Here is a plant status report:\s*", "", text, flags=re.IGNORECASE)
    text = STRUCTURED_FIELD_BREAK_RE.sub(". ", text)
    return _collapse_whitespace(text)


def build_ner_text(article: dict[str, Any], use_description: bool) -> str:
    if not use_description:
        return build_enrichment_text(article, use_description)

    ner_article = dict(article)
    ner_article["description"] = preprocess_description_for_ner(str(article.get("description") or ""))
    return build_enrichment_text(ner_article, use_description)


def ner_text_hash(text: str) -> str:
    return enrichment_text_hash(text)


def unique_in_order(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen = set()
    out: list[tuple[str, str]] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_country_key(value: str) -> str:
    compact = re.sub(r"\s+", " ", (value or "").strip())
    compact = compact.replace(".", "")
    return compact.lower()


def _build_country_demonym_hints() -> dict[str, str]:
    hints: dict[str, str] = {}
    for country, demonyms in COUNTRY_DEMONYM_HINT_GROUPS:
        for demonym in demonyms:
            hints[demonym] = country
    return hints


COUNTRY_DEMONYM_HINTS = _build_country_demonym_hints()


def _iter_country_hint_entries() -> list[tuple[str, str, bool, bool]]:
    entries = list(COUNTRY_ALIAS_HINTS)
    entries.extend(
        (phrase, country, False, False)
        for phrase, country in COUNTRY_DEMONYM_HINTS.items()
    )
    entries.extend(
        (phrase, country, False, False)
        for phrase, country in COUNTRY_SUBNATIONAL_PLACE_HINTS.items()
    )
    return entries


COUNTRY_HINT_ENTRIES = tuple(_iter_country_hint_entries())
COUNTRY_CONTEXT_BLOCKED_FOLLOWER_RE = re.compile(
    rf"^\s+(?:{'|'.join(re.escape(value) for value in COUNTRY_CONTEXT_BLOCKED_FOLLOWERS)})\b",
    re.IGNORECASE,
)
COUNTRY_EXPLICIT_NORMALIZATIONS = {
    _normalize_country_key(alias): country for alias, country, _, _ in COUNTRY_HINT_ENTRIES
}
COUNTRY_CONTEXT_RULES = {
    _normalize_country_key(alias): (case_sensitive, block_comma_prefixed)
    for alias, _, case_sensitive, block_comma_prefixed in COUNTRY_HINT_ENTRIES
}
COUNTRY_EXPLICIT_NORMALIZATIONS.update(
    {
        "north korea": "North Korea",
        "south korea": "South Korea",
    }
)


def _normalize_country_candidate(value: str) -> str:
    candidate = _collapse_whitespace(value)
    candidate = candidate.replace("’", "'").replace("`", "'")
    candidate = re.sub(r"^(?:the)\s+", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"[’']s\b", "", candidate)
    return candidate.strip(" -,:;")


def _compile_country_hint_patterns() -> list[tuple[re.Pattern[str], str, bool]]:
    patterns: list[tuple[re.Pattern[str], str, bool]] = []
    for phrase, country, case_sensitive, block_comma_prefixed in COUNTRY_HINT_ENTRIES:
        escaped = re.escape(phrase)
        left_boundary = r"(?<!\w)"
        flags = 0 if case_sensitive else re.IGNORECASE
        patterns.append(
            (
                re.compile(
                    rf"{left_boundary}{escaped}(?:['’]s)?(?!\w)",
                    flags,
                ),
                country,
                block_comma_prefixed,
            )
        )
    return patterns


COUNTRY_HINT_PATTERNS = _compile_country_hint_patterns()


def _is_blocked_country_text_match(
    haystack: str,
    match_start: int,
    match_end: int,
    *,
    block_comma_prefixed: bool = False,
) -> bool:
    if block_comma_prefixed and haystack[:match_start].rstrip().endswith(","):
        prefix = haystack[:match_start].rstrip()[:-1].strip()
        tail = re.split(r"[.;:!?()\[\]]\s*", prefix)[-1].strip()
        tail_words = re.findall(r"[A-Za-z][A-Za-z'.-]*", tail)
        if 2 <= len(tail_words) <= 4 and all(word[:1].isupper() for word in tail_words):
            return True
    return bool(COUNTRY_CONTEXT_BLOCKED_FOLLOWER_RE.match(haystack[match_end:]))


def scan_country_hints(text: str) -> list[str]:
    matches: list[tuple[int, str]] = []
    haystack = _collapse_whitespace(text)
    if not haystack:
        return []

    for pattern, country, block_comma_prefixed in COUNTRY_HINT_PATTERNS:
        for match in pattern.finditer(haystack):
            if _is_blocked_country_text_match(
                haystack,
                match.start(),
                match.end(),
                block_comma_prefixed=block_comma_prefixed,
            ):
                continue
            matches.append((match.start(), country))

    matches.sort(key=lambda item: item[0])

    countries: list[str] = []
    for _, country in matches:
        if country not in countries:
            countries.append(country)
    return countries


def try_country_name_patterns() -> list[tuple[re.Pattern[str], str]]:
    try:
        import pycountry  # type: ignore
    except Exception:
        return []

    candidates: list[tuple[str, str]] = []
    seen_keys: set[str] = set()
    for country in list(pycountry.countries):
        canonical = getattr(country, "name", "")
        if not canonical:
            continue
        display_name = COUNTRY_DISPLAY_OVERRIDES.get(canonical, canonical)
        for candidate in [
            canonical,
            getattr(country, "official_name", None),
            getattr(country, "common_name", None),
        ]:
            if not candidate:
                continue
            phrase = _collapse_whitespace(str(candidate))
            key = _normalize_country_key(phrase)
            if (
                not phrase
                or "," in phrase
                or key in COUNTRY_EXCLUDED_KEYS
                or key in COUNTRY_EXPLICIT_NORMALIZATIONS
                or key in seen_keys
            ):
                continue
            if len(re.sub(r"[^A-Za-z]", "", phrase)) <= 3:
                continue
            seen_keys.add(key)
            candidates.append((phrase, display_name))

    candidates.sort(key=lambda item: len(item[0]), reverse=True)
    return [
        (
            re.compile(
                rf"(?<!\w){re.escape(phrase)}(?:['’]s)?(?!\w)",
                re.IGNORECASE,
            ),
            display_name,
        )
        for phrase, display_name in candidates
    ]


def scan_country_names(
    text: str,
    patterns: list[tuple[re.Pattern[str], str]],
) -> list[str]:
    matches: list[tuple[int, str]] = []
    haystack = _collapse_whitespace(text)
    if not haystack:
        return []

    for pattern, country in patterns:
        for match in pattern.finditer(haystack):
            if _is_blocked_country_text_match(haystack, match.start(), match.end()):
                continue
            matches.append((match.start(), country))

    matches.sort(key=lambda item: item[0])

    countries: list[str] = []
    for _, country in matches:
        if country not in countries:
            countries.append(country)
    return countries


def text_contains_country_reference(text: str, value: str) -> bool:
    haystack = _collapse_whitespace(text)
    candidate = _normalize_country_candidate(value)
    if not haystack or not candidate:
        return False

    key = _normalize_country_key(candidate)
    if key in COUNTRY_EXCLUDED_KEYS:
        return False

    case_sensitive, block_comma_prefixed = COUNTRY_CONTEXT_RULES.get(key, (False, False))
    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = re.compile(
        rf"(?<!\w){re.escape(candidate)}(?:['’]s)?(?!\w)",
        flags,
    )
    for match in pattern.finditer(haystack):
        if _is_blocked_country_text_match(
            haystack,
            match.start(),
            match.end(),
            block_comma_prefixed=block_comma_prefixed,
        ):
            continue
        return True
    return False


def normalize_country_reference(
    value: str,
    direct_lookup: Optional[Callable[[str], Optional[str]]] = None,
) -> Optional[str]:
    candidate = _normalize_country_candidate(value)
    if not candidate:
        return None

    key = _normalize_country_key(candidate)
    if key in COUNTRY_EXCLUDED_KEYS:
        return None

    explicit = COUNTRY_EXPLICIT_NORMALIZATIONS.get(key)
    if explicit:
        return explicit

    if direct_lookup is not None:
        return direct_lookup(candidate)

    return None


def try_country_matcher() -> Optional[Callable[[str], Optional[str]]]:
    try:
        import pycountry  # type: ignore
    except Exception:
        return None

    by_name: dict[str, str] = {}
    by_code: dict[str, str] = {}
    for country in list(pycountry.countries):
        canonical = getattr(country, "name", "")
        if not canonical:
            continue
        display_name = COUNTRY_DISPLAY_OVERRIDES.get(canonical, canonical)
        for candidate in [
            canonical,
            getattr(country, "official_name", None),
            getattr(country, "common_name", None),
        ]:
            if not candidate:
                continue
            by_name[_normalize_country_key(str(candidate))] = display_name
        alpha_2 = getattr(country, "alpha_2", None)
        alpha_3 = getattr(country, "alpha_3", None)
        if alpha_2:
            by_code[str(alpha_2).upper()] = display_name
        if alpha_3:
            by_code[str(alpha_3).upper()] = display_name

    def to_country(ent_text: str) -> Optional[str]:
        candidate = _normalize_country_candidate(ent_text)
        if not candidate:
            return None

        direct = by_name.get(_normalize_country_key(candidate))
        if direct:
            return direct

        letters = re.sub(r"[^A-Za-z]", "", candidate)
        if len(letters) in (2, 3) and letters.upper() == letters:
            return by_code.get(letters.upper())

        return None

    return to_country


def _normalize_entity_key(value: str) -> str:
    compact = _collapse_whitespace(value)
    compact = compact.replace("’", "'").replace("`", "'")
    return re.sub(r"[^\w]+", "", compact).lower()


def normalize_entity_text(value: str) -> str:
    clean = _collapse_whitespace(html.unescape(value))
    clean = clean.replace("’", "'").replace("`", "'")
    clean = re.sub(r"^(?:the|a|an)\s+", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"[’']s\b$", "", clean)
    return clean.strip(" \t\r\n,;:")


def _entity_lookup_key(value: str) -> str:
    return normalize_entity_text(value).casefold()


def _titlecase_location_text(value: str) -> str:
    return " ".join(part.capitalize() for part in value.split())


def _normalize_structured_location_value(value: str) -> str:
    first_segment = re.split(r"\s*[,;]\s*", value, maxsplit=1)[0]
    return normalize_entity_text(first_segment)


def extract_text_hint_entities(text: str) -> list[tuple[str, str]]:
    haystack = _collapse_whitespace(text)
    if not haystack:
        return []

    matches: list[tuple[int, int, str, str]] = []
    for pattern, entity_text, label in TEXT_ENTITY_HINT_PATTERNS:
        for match in pattern.finditer(haystack):
            matches.append((match.start(), match.end(), entity_text, label))

    matches.sort(key=lambda item: (item[0], -(item[1] - item[0])))

    entities: list[tuple[str, str]] = []
    accepted_spans: list[tuple[int, int]] = []
    for start, end, entity_text, label in matches:
        if any(start < other_end and end > other_start for other_start, other_end in accepted_spans):
            continue
        accepted_spans.append((start, end))
        entities.append((entity_text, label))
    return entities


def should_store_entity(text: str, label: str) -> bool:
    clean_text = normalize_entity_text(text)
    clean_label = str(label or "").strip()
    if not clean_text or not clean_label:
        return False
    if clean_label in LOW_VALUE_ENTITY_LABELS:
        return False

    normalized_key = _normalize_entity_key(clean_text)
    if normalized_key in NOISE_ENTITY_KEYS:
        return False
    if normalized_key in GENERIC_COMMODITY_ENTITY_KEYS:
        return False
    if normalized_key in STRUCTURED_FIELD_BLOCK_KEYS:
        return False
    if re.search(r"\)\s*--", clean_text):
        return False
    if any(pattern.match(clean_text) for pattern in ENTITY_NOISE_PATTERNS):
        return False
    return True


class SpacyNERExtractor:
    """Incremental spaCy NER extractor that skips unchanged articles."""

    def __init__(self, config: NERConfig):
        self.config = config
        self._nlp = None
        self._country_matcher = try_country_matcher()
        self._country_name_patterns = try_country_name_patterns()
        self._input_mode = "title+description" if config.use_description else "title"
        self.logger = logging.getLogger("ner_spacy")

    def _ensure_nlp(self):
        if self._nlp is not None:
            return

        if not HAS_NER_DEPS:
            raise RuntimeError(
                "spaCy NER requires spacy and a model package. "
                "Install with: pip install -U spacy && python -m spacy download en_core_web_lg"
            )

        self.logger.info("Loading NER model '%s'...", self.config.model_name)
        self._nlp = spacy.load(self.config.model_name)

    def _needs_refresh(self, article: dict[str, Any], text_hash: str) -> bool:
        if self.config.force_rescore:
            return True

        ner = article.get("ner")
        if not isinstance(ner, dict):
            return True
        if ner.get("model") != self.config.model_name:
            return True
        if ner.get("input_mode") != self._input_mode:
            return True
        if ner.get("text_hash") != text_hash:
            return True
        if not isinstance(ner.get("countries"), list):
            return True
        if not isinstance(ner.get("entities"), list):
            return True
        return False

    def _extract_countries(self, raw_entities: list[tuple[str, str]], text: str) -> list[str]:
        countries = scan_country_hints(text)
        for country in scan_country_names(text, self._country_name_patterns):
            if country not in countries:
                countries.append(country)
        for ent_text, label in raw_entities:
            if label not in COUNTRY_ENTITY_LABELS:
                continue
            normalized = normalize_country_reference(ent_text, self._country_matcher)
            if normalized and normalized not in countries:
                if not text_contains_country_reference(text, ent_text):
                    continue
                countries.append(normalized)
        return countries

    def _rewrite_entity(
        self,
        text: str,
        label: str,
        full_text: str,
    ) -> list[tuple[str, str]]:
        clean_text = normalize_entity_text(text)
        clean_label = str(label or "").strip()
        if not clean_text or not clean_label:
            return []

        exact_key = _entity_lookup_key(clean_text)
        if exact_key == "uae fujairah":
            return [("UAE", "GPE"), ("Fujairah", "GPE")]
        if exact_key == "us e15" and re.search(r"(?<!\w)US E15 group(?!\w)", full_text):
            return [("US E15 group", "ORG")]

        prefix_match = STRUCTURED_FIELD_PREFIX_RE.match(clean_text)
        if prefix_match:
            prefix_label = prefix_match.group("label").casefold()
            value = prefix_match.group("value")
            if prefix_label in STRUCTURED_FIELD_ENTITY_LABELS:
                value = _normalize_structured_location_value(value)
                clean_label = STRUCTURED_FIELD_ENTITY_LABELS[prefix_label]
            else:
                value = normalize_entity_text(value)
            return self._rewrite_entity(value, clean_label, full_text)

        split_match = STRUCTURED_FIELD_SPLIT_RE.match(clean_text)
        if split_match:
            split_label = split_match.group("label").casefold()
            left = normalize_entity_text(split_match.group("left"))
            right = split_match.group("right")
            if split_label in STRUCTURED_FIELD_ENTITY_LABELS:
                right = _normalize_structured_location_value(right)
                right_label = STRUCTURED_FIELD_ENTITY_LABELS[split_label]
            else:
                right = normalize_entity_text(right)
                right_label = clean_label
            rewritten: list[tuple[str, str]] = []
            if left:
                rewritten.extend(self._rewrite_entity(left, clean_label, full_text))
            if right:
                rewritten.extend(self._rewrite_entity(right, right_label, full_text))
            return rewritten

        clean_text = STRUCTURED_FIELD_SUFFIX_RE.sub("", clean_text).strip()
        if not clean_text:
            return []

        month_match = ENTITY_MONTH_SUFFIX_RE.match(clean_text)
        if month_match:
            base = normalize_entity_text(month_match.group("base"))
            base_key = _entity_lookup_key(base)
            if base_key in ENTITY_EXACT_CORRECTIONS:
                clean_text, clean_label = ENTITY_EXACT_CORRECTIONS[base_key]
            elif normalize_country_reference(base, self._country_matcher):
                clean_text = base
                clean_label = "GPE"

        commodity_match = ENTITY_COMMODITY_SUFFIX_RE.match(clean_text)
        if commodity_match and clean_label not in {"FAC", "ORG"}:
            clean_text = normalize_entity_text(commodity_match.group("base"))

        correction = ENTITY_EXACT_CORRECTIONS.get(_entity_lookup_key(clean_text))
        if correction is not None:
            clean_text, clean_label = correction

        if clean_label in COUNTRY_ENTITY_LABELS and clean_text.islower():
            clean_text = _titlecase_location_text(clean_text)

        if not should_store_entity(clean_text, clean_label):
            return []
        return [(clean_text, clean_label)]

    def _finalize_entities(
        self,
        raw_entities: list[tuple[str, str]],
        text: str,
    ) -> list[tuple[str, str]]:
        candidates = list(raw_entities)
        candidates.extend(extract_text_hint_entities(text))

        finalized: list[tuple[str, str]] = []
        seen_texts: set[str] = set()
        for raw_text, raw_label in candidates:
            for clean_text, clean_label in self._rewrite_entity(raw_text, raw_label, text):
                dedupe_key = _entity_lookup_key(clean_text)
                if dedupe_key in seen_texts:
                    continue
                seen_texts.add(dedupe_key)
                finalized.append((clean_text, clean_label))
        return finalized

    def _extract_from_doc(self, doc) -> tuple[list[dict[str, str]], list[str]]:
        full_text = _collapse_whitespace(getattr(doc, "text", ""))
        raw_entities: list[tuple[str, str]] = []
        for ent in doc.ents:
            text = _collapse_whitespace(ent.text)
            label = str(ent.label_ or "").strip()
            if not text or not label:
                continue
            raw_entities.append((text, label))

        raw_entities = unique_in_order(raw_entities)

        countries = self._extract_countries(raw_entities, full_text)

        filtered_entities = self._finalize_entities(raw_entities, full_text)
        if self.config.max_entities > 0:
            filtered_entities = filtered_entities[: self.config.max_entities]

        entities = [{"text": text, "label": label} for text, label in filtered_entities]

        return entities, countries

    def extract_incremental(self, articles: list[dict]) -> dict[str, Any]:
        started = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        stats: dict[str, Any] = {
            "enabled": True,
            "model": self.config.model_name,
            "input_mode": self._input_mode,
            "candidate_articles": 0,
            "extracted": 0,
            "reused": 0,
            "blank_text": 0,
            "errors": 0,
            "extracted_at": now_iso,
        }

        to_extract_idx: list[int] = []
        to_extract_text: list[str] = []
        to_extract_hash: list[str] = []

        for idx, article in enumerate(articles):
            text = build_ner_text(article, self.config.use_description)
            if not text:
                stats["blank_text"] += 1
                continue

            text_hash = ner_text_hash(text)
            if self._needs_refresh(article, text_hash):
                to_extract_idx.append(idx)
                to_extract_text.append(text)
                to_extract_hash.append(text_hash)
            else:
                stats["reused"] += 1

        stats["candidate_articles"] = len(to_extract_idx)
        if not to_extract_idx:
            stats["duration_ms"] = int((time.time() - started) * 1000)
            return stats

        self._ensure_nlp()
        docs = self._nlp.pipe(to_extract_text, batch_size=self.config.batch_size)

        for idx, text_hash, doc in zip(to_extract_idx, to_extract_hash, docs):
            try:
                entities, countries = self._extract_from_doc(doc)
                articles[idx]["ner"] = {
                    "entities": entities,
                    "countries": countries,
                    "model": self.config.model_name,
                    "input_mode": self._input_mode,
                    "text_hash": text_hash,
                    "extracted_at": now_iso,
                }
                stats["extracted"] += 1
            except Exception:
                stats["errors"] += 1

        stats["duration_ms"] = int((time.time() - started) * 1000)
        return stats


def log_ner_rollup(
    articles: list[dict],
    logger: Optional[logging.Logger] = None,
) -> None:
    """Log a compact rollup of the most-mentioned countries."""
    log = logger or logging.getLogger("ner_spacy")

    counts: dict[str, int] = {}
    for article in articles:
        ner = article.get("ner") or {}
        countries = ner.get("countries")
        if not isinstance(countries, list):
            continue
        for country in countries:
            value = str(country or "").strip()
            if not value:
                continue
            counts[value] = counts.get(value, 0) + 1

    if not counts:
        log.info("NER rollup: no country mentions found.")
        return

    log.info("NER rollup (top country mentions):")
    top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]
    for country, count in top:
        log.info("  %s: %s", country, count)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run incremental spaCy NER extraction on feed JSON."
    )
    parser.add_argument("--input", required=True, help="Path to input feed JSON.")
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSON (can be same as --input).",
    )
    parser.add_argument("--model", default="en_core_web_lg")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--use-description",
        action="store_true",
        help="Extract from title + description instead of title only.",
    )
    parser.add_argument(
        "--force-rescore",
        action="store_true",
        help="Re-extract all non-empty items even if cached NER exists.",
    )
    parser.add_argument(
        "--max-entities",
        type=int,
        default=18,
        help="Maximum stored entities per article (default: 18).",
    )
    args = parser.parse_args()

    data = load_feed(Path(args.input))
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = NERConfig(
        enabled=True,
        model_name=args.model,
        batch_size=args.batch_size,
        use_description=args.use_description,
        force_rescore=args.force_rescore,
        max_entities=args.max_entities,
    )

    extractor = SpacyNERExtractor(config)
    stats = extractor.extract_incremental(articles)

    metadata = ensure_feed_metadata(data)
    metadata["ner"] = stats

    save_feed(Path(args.output), data)
    print(
        f"Wrote: {args.output} "
        f"(extracted={stats.get('extracted', 0)}, reused={stats.get('reused', 0)})"
    )
    log_ner_rollup(articles)


if __name__ == "__main__":
    main()
