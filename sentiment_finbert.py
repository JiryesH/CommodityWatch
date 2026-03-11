#!/usr/bin/env python3
"""
Measured sentiment scoring for commodity headlines.

This module can be:
1) Imported by rss_scraper.py to score articles in-memory.
2) Run standalone to score a JSON feed file.
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from enrichment_utils import build_enrichment_text, enrichment_text_hash
from feed_io import (
    ensure_feed_metadata,
    load_feed_json as load_feed,
    save_feed_json as save_feed,
)

try:
    import torch
    from transformers import pipeline as hf_pipeline

    HAS_SENTIMENT_DEPS = True
except ImportError:
    HAS_SENTIMENT_DEPS = False
    torch = None
    hf_pipeline = None


SENTIMENT_LABEL_KEYS = ("positive", "negative", "neutral")
PIPELINE_MODE_BASELINE = "baseline"
PIPELINE_MODE_COMMODITY = "commodity_v1"
CONTEXT_MODE_TITLE = "title"
CONTEXT_MODE_TITLE_DESCRIPTION = "title+description"
CONTEXT_MODE_AUTO = "auto"
PIPELINE_VERSION_BY_MODE = {
    PIPELINE_MODE_BASELINE: "baseline_v1",
    PIPELINE_MODE_COMMODITY: "commodity_v1",
}

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_TITLE_SPLIT_RE = re.compile(r"\s*(?::|[-\u2013])\s*", re.IGNORECASE)
_WRAPPER_HEAD_RE = re.compile(
    r"^\s*(?:NOON|EVENING|MORNING|MIDDAY|DAILY|WEEKLY|MONTHLY|MARKET|OUTLOOK)?"
    r"(?:\s+ASIA|\s+EUROPE|\s+AMERICAS|\s+GLOBAL)?\s*"
    r"(?:SNAPSHOT|SUMMARY|WRAP|REPORT|PODCAST|Q&A)\b",
    re.IGNORECASE,
)
_TOPIC_UPDATE_RE = re.compile(r"^\s*update to .* topics\b", re.IGNORECASE)


def _compile_patterns(patterns: dict[str, str]) -> dict[str, re.Pattern[str]]:
    return {name: re.compile(pattern, re.IGNORECASE) for name, pattern in patterns.items()}


POSITIVE_OPERATION_PATTERNS = _compile_patterns(
    {
        "restart": r"\brestart(?:s|ed|ing)?\b",
        "commission": (
            r"\bto\s+commission\b|"
            r"\bcommission(?:s|ed|ing)?\b(?:\s+\w+){0,4}\s+"
            r"\b(?:plant|unit|facility|terminal|shredder|capacity|reactor|project|line|train|site)\b"
        ),
        "resume": r"\bresum(?:e|es|ed|ing)\b",
        "debottleneck": r"\bdebottleneck(?:s|ed|ing)?\b",
        "enhancement": (
            r"\benhancement\b(?:\s+\w+){0,4}\s+\b(?:at|of|for)\b"
            r"(?:\s+\w+){0,4}\s+\b(?:facility|plant|unit|capacity|operations?|line|project|site)\b|"
            r"\benhancement\b(?:\s+\w+){0,3}\s+\bcapacity\b"
        ),
        "starts_production_or_test": (
            r"\bstarts?\b(?:\s+\w+){0,5}\s+\b(?:production|output|operations?|test|tests|co-firing)\b"
        ),
        "boosts_output": r"\bboosts?\b(?:\s+\w+){0,4}\s+\boutput\b",
        "restart_output": r"\bto restart\b(?:\s+\w+){0,4}\s+\b(?:output|production|operations?)\b",
    }
)
NEGATIVE_OPERATION_PATTERNS = _compile_patterns(
    {
        "force_majeure": r"\bforce majeure\b|\bfm issued\b",
        "halts": r"\bhalts?\b",
        "shuts": r"\bshuts?\b",
        "cut_runs": r"\bcut(?:s|ting)?\b(?:\s+\w+){0,5}\s+\bruns?\b",
        "stops_production": r"\bstops?\b(?:\s+\w+){0,4}\s+\bproduction\b",
        "offline": r"\boffline\b",
        "outage": r"\boutage\b|\bshutdown\b",
        "stranded": r"\bstranded\b",
        "technical_issues": r"\btechnical issues\b",
        "production_halt": r"\bproduction halt\b",
        "cuts_output": r"\bcuts?\b(?:\s+\w+){0,3}\s+\b(?:crude\s+)?output\b",
    }
)
POSITIVE_MARKET_PATTERNS = _compile_patterns(
    {
        "prices_rise": r"\bprices?\b\s+(?:surge|surges|soar|soars|rise|rises|higher|firm|firms|gain|gains)\b",
        "rally_higher": r"\brall(?:y|ies)\s+higher\b",
        "upward_pressure": r"\bupward pressure\b",
        "price_hikes": r"\bprice hikes?\b",
        "prices_increase": r"\bprices?\b(?:\s+\w+){0,2}\s+(?:increase|increases|increased)\b",
        "boosts_prices": r"\bboosts?\b(?:\s+\w+){0,4}\s+\bprices?\b",
        "derivatives_surge": r"\bderivatives?\b(?:\s+\w+){0,2}\s+\bsurge(?:s|d)?\b",
        "premiums_rise": r"\bpremiums?\b(?:\s+\w+){0,2}\s+(?:surge|surges|soar|soars|rise|rises|higher)\b",
        "shortage_deepens": r"\bshortages?\b(?:\s+\w+){0,2}\s+deepens?\b",
        "rebound": r"\brebound(?:s|ed|ing)?\b",
        "quick_boost": r"\b(?:quick|immediate|near-term)\s+boost\b",
        "lower_utility_tariff_rate": (
            r"\blower(?:s|ed|ing)?\b(?:\s+\d{4})?(?:\s+\w+){0,4}\s+"
            r"\b(?:utility|power|electricity|energy)\b(?:\s+\w+){0,2}\s+\b(?:tariff|rate)s?\b"
        ),
        "investment_surge": r"\bsurge\b(?:\s+\w+){0,3}\s+\binvestments?\b|\bnew investments?\b",
        "record_exports": r"\brecord\b(?:\s+\w+){0,2}\s+\bexports?\b|\bexports?\b(?:\s+\w+){0,2}\s+\brecord\b",
        "no_shortage_despite": r"\bno\b(?:\s+\w+){0,4}\s+\bshortages?\b.*\bdespite\b",
    }
)
NEGATIVE_MARKET_PATTERNS = _compile_patterns(
    {
        "under_pressure": r"\bunder pressure\b",
        "slump": r"\bslump(?:s|ed|ing)?\b",
        "tumble": r"\btumble(?:s|d|ing)?\b",
        "falls": r"\bfalls?\b|\bdrops?\b|\bdeclines?\b",
        "weakens": r"\bweak(?:ens?|er)\b",
        "stalls": r"\bstalls?\b",
        "vulnerability": r"\bvulnerability\b|\bproves vulnerability\b",
        "costlier": r"\bcostlier\b|\bmore complex\b",
        "security_risk": r"\bsecurity risks?\b|\bwar-risk\b",
        "at_risk": r"\bat risk\b",
        "cannot_pay": r"\b(?:can\s*not|cannot|unable\s+to)\s+pay\b",
        "buckling_under": r"\bbuckl(?:e|es|ed|ing)\b(?:\s+\w+){0,3}\s+\bunder\b",
        "end_exports": r"\bend\b(?:\s+\w+){0,4}\s+\bexports?\b",
        "export_ban": r"\bban on exports?\b|\bimposes immediate ban\b",
        "crew_casualties": r"\bcrew casualties\b",
    }
)
BULLISH_TIGHTNESS_PATTERNS = _compile_patterns(
    {
        "tighten": r"\btight(?:en|ens|ening|er|ness)\b",
        "shortage": r"\bshortages?\b|\bscarcity\b",
        "import_shortages": r"\bimport shortages?\b",
        "disrupted_exports": r"\bdisrupted exports?\b|\bexport disruptions?\b",
        "below_demand": r"\b(?:below|less than)\s+demand\b",
        "output_may_tighten": r"\boutput may tighten\b",
    }
)
BEARISH_SUPPLY_PATTERNS = _compile_patterns(
    {
        "opec_production_up": r"\bopec\+?\b.*\b(boosts?|raises?|increases?)\b.*\bproduction\b",
        "supply_growth": r"\bsupply growth estimates?\b|\bsupply growth\b",
        "oversupply": r"\boversupply\b|\bsurplus\b|\bglut\b",
    }
)
MIXED_PATTERNS = _compile_patterns(
    {
        "mixed": r"\bmixed\b",
        "split": r"\bsplit(?:s|ting)?\b",
        "hinge_on": r"\bhinge on\b|\bdepends? on\b",
        "offsetting": r"\boffset(?:ting)?\b",
        "demand_and_supply": r"\bdemand and supply\b",
        "skirt": r"\bskirt(?:s|ing)?\b",
    }
)
FACTUAL_PATTERNS = _compile_patterns(
    {
        "inflation": r"\binflation\b",
        "gdp": r"\bgdp\b",
        "industrial_output": r"\bindustrial output\b",
        "margin_report": r"\bmodel petchem margin\b|\bmargin at\b",
        "says_no_plans": r"\bsays no plans?\b|\bno plans to release\b",
        "expects_policy": r"\bpolicy clarity\b",
        "cbam_benchmarks": r"\bcbam\b.*\bbenchmarks?\b|\bbenchmarks?\b.*\bcbam\b",
        "retail_fuel_prices": r"\bretail\b.*\b(?:gasoline|diesel|fuel)\b.*\bprices?\b",
        "agency_update": r"\b(?:iea|eia)\b",
    }
)
PROCEDURAL_PATTERNS = _compile_patterns(
    {
        "expects": r"\bexpects?\b",
        "sees": r"\bsees?\b",
        "says": r"\bsays?\b",
        "plans": r"\bplans?\b",
        "seeks": r"\bseeks?\b",
        "could": r"\bcould\b",
        "may": r"\bmay\b",
        "might": r"\bmight\b",
        "how": r"^\s*insight:\s*how\b|^\s*how\b",
        "topic_update": r"^\s*update to .* topics\b",
        "podcast": r"^\s*podcast\b",
        "q_and_a": r"^\s*q&a\b",
        "viewpoint": r"^\s*viewpoint\b",
        "outlook": r"\boutlook\b",
    }
)
SEVERITY_PATTERNS = _compile_patterns(
    {
        "war": r"\bwar\b|\bconflict\b|\bcrisis\b",
        "disruption": r"\bdisruption(?:s)?\b",
        "threat": r"\bthreats?\b",
        "tensions": r"\btensions?\b",
        "cuts": r"\bcuts?\b",
    }
)


@dataclass
class SentimentConfig:
    enabled: bool = True
    model_name: str = "ProsusAI/finbert"
    model_backend: str = "finbert"
    batch_size: int = 32
    max_length: int = 128
    use_description: bool = False
    force_rescore: bool = False
    pipeline_mode: str = PIPELINE_MODE_COMMODITY
    context_mode: str = CONTEXT_MODE_AUTO
    confidence_power: float = 0.85

    def resolved_pipeline_mode(self) -> str:
        if self.pipeline_mode in PIPELINE_VERSION_BY_MODE:
            return self.pipeline_mode
        return PIPELINE_MODE_COMMODITY

    def pipeline_version(self) -> str:
        return PIPELINE_VERSION_BY_MODE[self.resolved_pipeline_mode()]

    def requested_context_mode(self) -> str:
        requested = self.context_mode
        if requested not in {
            CONTEXT_MODE_TITLE,
            CONTEXT_MODE_TITLE_DESCRIPTION,
            CONTEXT_MODE_AUTO,
        }:
            requested = CONTEXT_MODE_AUTO
        if self.resolved_pipeline_mode() == PIPELINE_MODE_BASELINE:
            return (
                CONTEXT_MODE_TITLE_DESCRIPTION
                if self.use_description or requested == CONTEXT_MODE_TITLE_DESCRIPTION
                else CONTEXT_MODE_TITLE
            )
        if requested != CONTEXT_MODE_AUTO:
            return requested
        if self.use_description:
            return CONTEXT_MODE_TITLE_DESCRIPTION
        return CONTEXT_MODE_AUTO


@dataclass(frozen=True)
class TextSignals:
    positive_ops: tuple[str, ...] = ()
    negative_ops: tuple[str, ...] = ()
    positive_market: tuple[str, ...] = ()
    negative_market: tuple[str, ...] = ()
    bullish_tightness: tuple[str, ...] = ()
    bearish_supply: tuple[str, ...] = ()
    mixed: tuple[str, ...] = ()
    factual: tuple[str, ...] = ()
    procedural: tuple[str, ...] = ()
    severity: tuple[str, ...] = ()

    @property
    def positive_strength(self) -> int:
        return (3 * len(self.positive_ops)) + (2 * len(self.bullish_tightness)) + (2 * len(self.positive_market))

    @property
    def negative_strength(self) -> int:
        return (3 * len(self.negative_ops)) + (2 * len(self.bearish_supply)) + (2 * len(self.negative_market))

    @property
    def directional_strength(self) -> int:
        return self.positive_strength + self.negative_strength

    @property
    def has_strong_direction(self) -> bool:
        return self.directional_strength >= 2

    @property
    def has_opposing_direction(self) -> bool:
        return self.positive_strength > 0 and self.negative_strength > 0

    @property
    def explicit_mixed(self) -> bool:
        return bool(self.mixed)

    @property
    def severity_only(self) -> bool:
        return bool(self.severity) and self.directional_strength == 0

    def to_trace(self) -> dict[str, list[str]]:
        return {
            "positive_ops": list(self.positive_ops),
            "negative_ops": list(self.negative_ops),
            "positive_market": list(self.positive_market),
            "negative_market": list(self.negative_market),
            "bullish_tightness": list(self.bullish_tightness),
            "bearish_supply": list(self.bearish_supply),
            "mixed": list(self.mixed),
            "factual": list(self.factual),
            "procedural": list(self.procedural),
            "severity": list(self.severity),
        }


@dataclass(frozen=True)
class ArticleSignals:
    title: TextSignals
    description: TextSignals
    title_tail: str
    title_tail_signals: TextSignals
    has_wrapper_head: bool
    generic_wrapper: bool
    topic_update: bool


@dataclass(frozen=True)
class ContextSelection:
    text: str
    input_mode: str
    reason: str


@dataclass(frozen=True)
class GateDecision:
    directional: bool
    reason: str
    prior_probabilities: dict[str, float]


@dataclass(frozen=True)
class PreparedArticle:
    index: int
    text_hash: str
    context: ContextSelection
    article_signals: ArticleSignals
    selected_signals: TextSignals
    gate: GateDecision


@dataclass(frozen=True)
class PipelineResult:
    label: str
    probabilities: dict[str, float]
    rules_applied: tuple[str, ...]
    raw_probabilities: Optional[dict[str, float]]


def normalize_sentiment_scores(raw_scores: Any) -> dict[str, float]:
    """
    Normalize model output to always include all labels.
    Supports:
      - top_k=None output: [{"label": "...", "score": ...}, ...]
      - single top output: {"label": "...", "score": ...}
    """
    out = {k: 0.0 for k in SENTIMENT_LABEL_KEYS}

    if isinstance(raw_scores, dict):
        label = str(raw_scores.get("label", "")).lower()
        score = float(raw_scores.get("score", 0.0))
        if label in out:
            out[label] = score
        return out

    if isinstance(raw_scores, list):
        for item in raw_scores:
            label = str(item.get("label", "")).lower()
            score = float(item.get("score", 0.0))
            if label in out:
                out[label] = score
        return out

    return out


def normalize_probability_dict(scores: dict[str, float]) -> dict[str, float]:
    out = {label: max(float(scores.get(label, 0.0)), 0.0) for label in SENTIMENT_LABEL_KEYS}
    total = sum(out.values())
    if total <= 0:
        share = 1.0 / len(SENTIMENT_LABEL_KEYS)
        return {label: share for label in SENTIMENT_LABEL_KEYS}
    return {label: out[label] / total for label in SENTIMENT_LABEL_KEYS}


def pick_sentiment_label(scores: dict[str, float]) -> tuple[str, float]:
    label = max(scores, key=lambda key: scores[key])
    return label, float(scores[label])


def build_sentiment_text(article: dict[str, Any], use_description: bool) -> str:
    return build_enrichment_text(article, use_description)


def sentiment_text_hash(text: str) -> str:
    return enrichment_text_hash(text)


def normalize_rule_text(value: str) -> str:
    without_html = _HTML_TAG_RE.sub(" ", value or "")
    return " ".join(without_html.split())


def extract_title_tail(title: str) -> tuple[str, str]:
    parts = _TITLE_SPLIT_RE.split(title, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return title, ""


def match_pattern_names(patterns: dict[str, re.Pattern[str]], text: str) -> tuple[str, ...]:
    if not text:
        return ()
    return tuple(name for name, pattern in patterns.items() if pattern.search(text))


def analyze_text_signals(text: str) -> TextSignals:
    return TextSignals(
        positive_ops=match_pattern_names(POSITIVE_OPERATION_PATTERNS, text),
        negative_ops=match_pattern_names(NEGATIVE_OPERATION_PATTERNS, text),
        positive_market=match_pattern_names(POSITIVE_MARKET_PATTERNS, text),
        negative_market=match_pattern_names(NEGATIVE_MARKET_PATTERNS, text),
        bullish_tightness=match_pattern_names(BULLISH_TIGHTNESS_PATTERNS, text),
        bearish_supply=match_pattern_names(BEARISH_SUPPLY_PATTERNS, text),
        mixed=match_pattern_names(MIXED_PATTERNS, text),
        factual=match_pattern_names(FACTUAL_PATTERNS, text),
        procedural=match_pattern_names(PROCEDURAL_PATTERNS, text),
        severity=match_pattern_names(SEVERITY_PATTERNS, text),
    )


def merge_text_signals(*signals: TextSignals) -> TextSignals:
    def merged(attr: str) -> tuple[str, ...]:
        values: set[str] = set()
        for item in signals:
            values.update(getattr(item, attr))
        return tuple(sorted(values))

    return TextSignals(
        positive_ops=merged("positive_ops"),
        negative_ops=merged("negative_ops"),
        positive_market=merged("positive_market"),
        negative_market=merged("negative_market"),
        bullish_tightness=merged("bullish_tightness"),
        bearish_supply=merged("bearish_supply"),
        mixed=merged("mixed"),
        factual=merged("factual"),
        procedural=merged("procedural"),
        severity=merged("severity"),
    )


def analyze_article(article: dict[str, Any]) -> ArticleSignals:
    title = normalize_rule_text(str(article.get("title") or ""))
    description = normalize_rule_text(str(article.get("description") or ""))
    title_signals = analyze_text_signals(title)
    description_signals = analyze_text_signals(description)
    title_head, title_tail = extract_title_tail(title)
    title_tail_signals = analyze_text_signals(title_tail)
    has_wrapper_head = bool(
        _WRAPPER_HEAD_RE.match(title_head)
        or _TOPIC_UPDATE_RE.match(title)
        or "market report" in title.lower()
    )
    topic_update = bool(_TOPIC_UPDATE_RE.match(title))
    generic_wrapper = has_wrapper_head and not (
        title_tail_signals.positive_ops
        or title_tail_signals.negative_ops
        or title_tail_signals.bullish_tightness
        or title_tail_signals.bearish_supply
        or title_tail_signals.negative_market
        or len(title_tail_signals.positive_market) > 1
    )
    return ArticleSignals(
        title=title_signals,
        description=description_signals,
        title_tail=title_tail,
        title_tail_signals=title_tail_signals,
        has_wrapper_head=has_wrapper_head,
        generic_wrapper=generic_wrapper,
        topic_update=topic_update,
    )


def calibrated_probabilities(scores: dict[str, float], power: float) -> dict[str, float]:
    if power <= 0:
        return normalize_probability_dict(scores)
    adjusted = {
        label: max(float(scores.get(label, 0.0)), 1e-9) ** power
        for label in SENTIMENT_LABEL_KEYS
    }
    return normalize_probability_dict(adjusted)


def build_label_prior(label: str, confidence: float) -> dict[str, float]:
    confidence = min(max(confidence, 0.34), 0.95)
    remainder = max(0.0, 1.0 - confidence)
    share = remainder / 2.0
    prior = {key: share for key in SENTIMENT_LABEL_KEYS}
    prior[label] = confidence
    return normalize_probability_dict(prior)


def blend_probabilities(
    base: Optional[dict[str, float]],
    prior: dict[str, float],
    prior_weight: float,
) -> dict[str, float]:
    prior_weight = min(max(prior_weight, 0.0), 1.0)
    prior = normalize_probability_dict(prior)
    if base is None:
        return prior
    base = normalize_probability_dict(base)
    blended = {
        label: ((1.0 - prior_weight) * base[label]) + (prior_weight * prior[label])
        for label in SENTIMENT_LABEL_KEYS
    }
    return normalize_probability_dict(blended)


def ensure_label_argmax(scores: dict[str, float], label: str) -> dict[str, float]:
    probs = normalize_probability_dict(scores)
    if label not in probs:
        return probs
    other_top = max(probs[key] for key in SENTIMENT_LABEL_KEYS if key != label)
    if probs[label] > other_top:
        return probs
    target_confidence = min(max(other_top + 0.05, 0.56), 0.9)
    prior = build_label_prior(label, target_confidence)
    enforced = blend_probabilities(probs, prior, prior_weight=0.65)
    if enforced[label] <= max(enforced[key] for key in SENTIMENT_LABEL_KEYS if key != label):
        return prior
    return enforced


class SentimentBackend(ABC):
    def __init__(self, config: SentimentConfig):
        self.config = config
        self.logger = logging.getLogger("sentiment_finbert")

    @property
    @abstractmethod
    def backend_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def score_texts(self, texts: list[str]) -> list[dict[str, float]]:
        raise NotImplementedError


class FinBERTBackend(SentimentBackend):
    def __init__(self, config: SentimentConfig):
        super().__init__(config)
        self._classifier = None

    @property
    def backend_name(self) -> str:
        return "finbert"

    def _ensure_classifier(self) -> None:
        if self._classifier is not None:
            return
        if not HAS_SENTIMENT_DEPS:
            raise RuntimeError(
                "FinBERT scoring requires transformers and torch. "
                "Install with: pip install -U transformers torch"
            )

        device = 0 if (torch is not None and torch.cuda.is_available()) else -1
        device_name = "cuda" if device == 0 else "cpu"
        self.logger.info(
            "Loading sentiment model '%s' with backend '%s' on %s...",
            self.config.model_name,
            self.backend_name,
            device_name,
        )
        self._classifier = hf_pipeline(
            task="text-classification",
            model=self.config.model_name,
            tokenizer=self.config.model_name,
            top_k=None,
            device=device,
        )

    def score_texts(self, texts: list[str]) -> list[dict[str, float]]:
        self._ensure_classifier()
        raw_results = self._classifier(
            texts,
            batch_size=self.config.batch_size,
            truncation=True,
            max_length=self.config.max_length,
        )
        if isinstance(raw_results, dict):
            raw_results = [raw_results]
        return [normalize_sentiment_scores(item) for item in raw_results]


BACKEND_REGISTRY: dict[str, type[SentimentBackend]] = {
    "finbert": FinBERTBackend,
}


def build_backend(config: SentimentConfig) -> SentimentBackend:
    backend_cls = BACKEND_REGISTRY.get(config.model_backend)
    if backend_cls is None:
        supported = ", ".join(sorted(BACKEND_REGISTRY))
        raise ValueError(
            f"Unsupported sentiment backend {config.model_backend!r}. Supported backends: {supported}"
        )
    return backend_cls(config)


class SentimentPipeline(ABC):
    def __init__(self, config: SentimentConfig):
        self.config = config

    @property
    def mode(self) -> str:
        return self.config.resolved_pipeline_mode()

    @property
    def version(self) -> str:
        return self.config.pipeline_version()

    @abstractmethod
    def select_context(self, article: dict[str, Any], article_signals: ArticleSignals) -> ContextSelection:
        raise NotImplementedError

    @abstractmethod
    def gate(self, article_signals: ArticleSignals, selected_signals: TextSignals) -> GateDecision:
        raise NotImplementedError

    @abstractmethod
    def resolve_label(
        self,
        prepared: PreparedArticle,
        raw_probabilities: Optional[dict[str, float]],
    ) -> PipelineResult:
        raise NotImplementedError


class BaselineSentimentPipeline(SentimentPipeline):
    def select_context(self, article: dict[str, Any], article_signals: ArticleSignals) -> ContextSelection:
        use_description = self.config.requested_context_mode() == CONTEXT_MODE_TITLE_DESCRIPTION
        input_mode = (
            CONTEXT_MODE_TITLE_DESCRIPTION if use_description else CONTEXT_MODE_TITLE
        )
        reason = "legacy_use_description" if use_description else "legacy_title_only"
        return ContextSelection(
            text=build_sentiment_text(article, use_description=use_description),
            input_mode=input_mode,
            reason=reason,
        )

    def gate(self, article_signals: ArticleSignals, selected_signals: TextSignals) -> GateDecision:
        return GateDecision(
            directional=True,
            reason="baseline_no_gate",
            prior_probabilities=build_label_prior("neutral", 0.34),
        )

    def resolve_label(
        self,
        prepared: PreparedArticle,
        raw_probabilities: Optional[dict[str, float]],
    ) -> PipelineResult:
        probs = normalize_probability_dict(raw_probabilities or {})
        label, _ = pick_sentiment_label(probs)
        return PipelineResult(
            label=label,
            probabilities=probs,
            rules_applied=(),
            raw_probabilities=probs,
        )


class CommoditySentimentPipeline(SentimentPipeline):
    def select_context(self, article: dict[str, Any], article_signals: ArticleSignals) -> ContextSelection:
        requested = self.config.requested_context_mode()
        if requested == CONTEXT_MODE_TITLE:
            return ContextSelection(
                text=build_sentiment_text(article, use_description=False),
                input_mode=CONTEXT_MODE_TITLE,
                reason="explicit_title_only",
            )
        if requested == CONTEXT_MODE_TITLE_DESCRIPTION:
            return ContextSelection(
                text=build_sentiment_text(article, use_description=True),
                input_mode=CONTEXT_MODE_TITLE_DESCRIPTION,
                reason="explicit_title_description",
            )

        title_text = build_sentiment_text(article, use_description=False)
        description_text = normalize_rule_text(str(article.get("description") or ""))
        if not description_text:
            return ContextSelection(
                text=title_text,
                input_mode=CONTEXT_MODE_TITLE,
                reason="auto_no_description",
            )
        if article_signals.topic_update or article_signals.generic_wrapper:
            return ContextSelection(
                text=title_text,
                input_mode=CONTEXT_MODE_TITLE,
                reason="auto_wrapper_prefers_title",
            )
        if article_signals.title.has_strong_direction:
            return ContextSelection(
                text=title_text,
                input_mode=CONTEXT_MODE_TITLE,
                reason="auto_strong_title_signal",
            )
        if article_signals.title.explicit_mixed:
            return ContextSelection(
                text=title_text,
                input_mode=CONTEXT_MODE_TITLE,
                reason="auto_mixed_title_prefers_title",
            )

        title_needs_help = (
            article_signals.title.directional_strength == 0
            or article_signals.title.severity_only
            or (
                bool(article_signals.title.procedural)
                and article_signals.title.directional_strength <= 2
            )
            or (
                bool(article_signals.title.factual)
                and article_signals.title.directional_strength <= 2
            )
        )
        description_directional = (
            article_signals.description.directional_strength >= 2
            and not article_signals.description.explicit_mixed
        )
        if title_needs_help and description_directional:
            return ContextSelection(
                text=build_sentiment_text(article, use_description=True),
                input_mode=CONTEXT_MODE_TITLE_DESCRIPTION,
                reason="auto_description_disambiguates",
            )

        return ContextSelection(
            text=title_text,
            input_mode=CONTEXT_MODE_TITLE,
            reason="auto_default_title",
        )

    def gate(self, article_signals: ArticleSignals, selected_signals: TextSignals) -> GateDecision:
        if article_signals.topic_update:
            return GateDecision(
                directional=False,
                reason="topic_update_wrapper",
                prior_probabilities=build_label_prior("neutral", 0.82),
            )
        if article_signals.generic_wrapper and not selected_signals.has_strong_direction:
            return GateDecision(
                directional=False,
                reason="generic_summary_wrapper",
                prior_probabilities=build_label_prior("neutral", 0.8),
            )
        if selected_signals.explicit_mixed:
            return GateDecision(
                directional=False,
                reason="mixed_headline",
                prior_probabilities=build_label_prior("neutral", 0.78),
            )
        if selected_signals.has_opposing_direction:
            return GateDecision(
                directional=False,
                reason="offsetting_directional_cues",
                prior_probabilities=build_label_prior("neutral", 0.76),
            )
        if selected_signals.severity_only:
            return GateDecision(
                directional=False,
                reason="severity_without_market_direction",
                prior_probabilities=build_label_prior("neutral", 0.72),
            )
        if (
            article_signals.title.factual
            and selected_signals.directional_strength == 0
        ):
            return GateDecision(
                directional=False,
                reason="factual_macro_headline",
                prior_probabilities=build_label_prior("neutral", 0.74),
            )
        if (
            article_signals.title.procedural
            and selected_signals.directional_strength == 0
        ):
            return GateDecision(
                directional=False,
                reason="procedural_headline",
                prior_probabilities=build_label_prior("neutral", 0.72),
            )
        return GateDecision(
            directional=True,
            reason="directional_gate_pass",
            prior_probabilities=build_label_prior("neutral", 0.4),
        )

    def resolve_label(
        self,
        prepared: PreparedArticle,
        raw_probabilities: Optional[dict[str, float]],
    ) -> PipelineResult:
        if raw_probabilities is None:
            return PipelineResult(
                label="neutral",
                probabilities=prepared.gate.prior_probabilities,
                rules_applied=("gate_to_neutral",),
                raw_probabilities=None,
            )

        selected = prepared.selected_signals
        title_signals = prepared.article_signals.title
        raw = normalize_probability_dict(raw_probabilities or {})
        calibrated = calibrated_probabilities(raw, self.config.confidence_power)
        rules_applied: list[str] = []

        if selected.explicit_mixed or selected.has_opposing_direction:
            rules_applied.append("mixed_or_offsetting_to_neutral")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("neutral", 0.72),
                prior_weight=0.45,
            )
            return PipelineResult(
                label="neutral",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if selected.negative_ops and not selected.positive_ops:
            rules_applied.append("operational_negative_override")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("negative", 0.78),
                prior_weight=0.45,
            )
            return PipelineResult(
                label="negative",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if selected.positive_ops and not selected.negative_ops:
            rules_applied.append("operational_positive_override")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("positive", 0.76),
                prior_weight=0.42,
            )
            return PipelineResult(
                label="positive",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if selected.bullish_tightness and not selected.negative_ops and not selected.negative_market:
            rules_applied.append("tightness_is_bullish")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("positive", 0.74),
                prior_weight=0.35,
            )
            return PipelineResult(
                label="positive",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if selected.bearish_supply and not selected.positive_ops:
            rules_applied.append("supply_growth_is_bearish")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("negative", 0.74),
                prior_weight=0.35,
            )
            return PipelineResult(
                label="negative",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if (
            selected.positive_market
            and not selected.negative_market
            and not selected.negative_ops
            and not selected.explicit_mixed
            and not selected.factual
        ):
            rules_applied.append("positive_market_cue_override")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("positive", 0.7),
                prior_weight=0.28,
            )
            return PipelineResult(
                label="positive",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if (
            selected.negative_market
            and not selected.positive_market
            and not selected.positive_ops
            and not selected.explicit_mixed
            and not selected.factual
        ):
            rules_applied.append("negative_market_cue_override")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("negative", 0.7),
                prior_weight=0.28,
            )
            return PipelineResult(
                label="negative",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        if calibrated["neutral"] >= max(calibrated["positive"], calibrated["negative"]) + 0.12:
            rules_applied.append("model_prefers_neutral")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("neutral", 0.68),
                prior_weight=0.3,
            )
            return PipelineResult(
                label="neutral",
                probabilities=final_probs,
                rules_applied=tuple(rules_applied),
                raw_probabilities=raw,
            )

        directional_label = (
            "positive"
            if calibrated["positive"] >= calibrated["negative"]
            else "negative"
        )
        if title_signals.severity_only and calibrated["negative"] > calibrated["positive"]:
            rules_applied.append("severity_words_not_enough_for_negative")
            directional_label = "neutral"

        if directional_label == "neutral":
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior("neutral", 0.68),
                prior_weight=0.35,
            )
        else:
            rules_applied.append("directional_resolution")
            final_probs = blend_probabilities(
                calibrated,
                build_label_prior(directional_label, 0.62),
                prior_weight=0.18,
            )

        return PipelineResult(
            label=directional_label,
            probabilities=final_probs,
            rules_applied=tuple(rules_applied),
            raw_probabilities=raw,
        )


def build_pipeline(config: SentimentConfig) -> SentimentPipeline:
    if config.resolved_pipeline_mode() == PIPELINE_MODE_BASELINE:
        return BaselineSentimentPipeline(config)
    return CommoditySentimentPipeline(config)


class FinBERTScorer:
    """Incremental sentiment scorer with a benchmarkable pipeline abstraction."""

    def __init__(
        self,
        config: SentimentConfig,
        backend: Optional[SentimentBackend] = None,
    ):
        self.config = config
        self.pipeline = build_pipeline(config)
        self.backend = backend or build_backend(config)
        self.logger = logging.getLogger("sentiment_finbert")

    def _needs_rescore(
        self,
        article: dict[str, Any],
        text_hash: str,
        input_mode: str,
    ) -> bool:
        if self.config.force_rescore:
            return True

        sentiment = article.get("sentiment")
        if not isinstance(sentiment, dict):
            return True
        if sentiment.get("model") != self.config.model_name:
            return True
        if sentiment.get("input_mode") != input_mode:
            return True
        if sentiment.get("text_hash") != text_hash:
            return True
        if sentiment.get("pipeline_mode") != self.pipeline.mode:
            return True
        if sentiment.get("pipeline_version") != self.pipeline.version:
            return True
        if sentiment.get("backend") != self.backend.backend_name:
            return True
        probabilities = sentiment.get("probabilities")
        if not isinstance(probabilities, dict):
            return True
        return False

    def _selected_signals(
        self,
        article_signals: ArticleSignals,
        input_mode: str,
    ) -> TextSignals:
        if input_mode == CONTEXT_MODE_TITLE_DESCRIPTION:
            return merge_text_signals(article_signals.title, article_signals.description)
        return article_signals.title

    def _build_payload(
        self,
        prepared: PreparedArticle,
        result: PipelineResult,
        now_iso: str,
    ) -> dict[str, Any]:
        probs = ensure_label_argmax(result.probabilities, result.label)
        confidence = float(probs.get(result.label, 0.0))
        compound = float(probs["positive"] - probs["negative"])
        payload = {
            "label": result.label,
            "confidence": confidence,
            "probabilities": probs,
            "compound": compound,
            "model": self.config.model_name,
            "input_mode": prepared.context.input_mode,
            "text_hash": prepared.text_hash,
            "scored_at": now_iso,
            "backend": self.backend.backend_name,
            "pipeline_mode": self.pipeline.mode,
            "pipeline_version": self.pipeline.version,
            "pipeline": {
                "mode": self.pipeline.mode,
                "version": self.pipeline.version,
                "context_mode": self.config.requested_context_mode(),
                "context_reason": prepared.context.reason,
                "gate_reason": prepared.gate.reason,
                "rules_applied": list(result.rules_applied),
                "signals": prepared.selected_signals.to_trace(),
            },
        }
        if result.raw_probabilities is not None:
            payload["pipeline"]["raw_probabilities"] = result.raw_probabilities
        return payload

    def score_incremental(self, articles: list[dict]) -> dict[str, Any]:
        started = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()
        stats = {
            "enabled": True,
            "model": self.config.model_name,
            "backend": self.backend.backend_name,
            "pipeline_mode": self.pipeline.mode,
            "pipeline_version": self.pipeline.version,
            "context_mode": self.config.requested_context_mode(),
            "candidate_articles": 0,
            "scored": 0,
            "model_scored": 0,
            "gated_neutral": 0,
            "reused": 0,
            "blank_text": 0,
            "errors": 0,
            "scored_at": now_iso,
        }

        prepared_items: list[PreparedArticle] = []
        to_model: list[PreparedArticle] = []
        staged_results: dict[int, dict[str, Any]] = {}

        for idx, article in enumerate(articles):
            article_signals = analyze_article(article)
            context = self.pipeline.select_context(article, article_signals)
            if not context.text:
                stats["blank_text"] += 1
                continue

            text_hash = sentiment_text_hash(context.text)
            if not self._needs_rescore(article, text_hash, context.input_mode):
                stats["reused"] += 1
                continue

            selected_signals = self._selected_signals(article_signals, context.input_mode)
            gate = self.pipeline.gate(article_signals, selected_signals)
            prepared = PreparedArticle(
                index=idx,
                text_hash=text_hash,
                context=context,
                article_signals=article_signals,
                selected_signals=selected_signals,
                gate=gate,
            )
            prepared_items.append(prepared)
            if gate.directional:
                to_model.append(prepared)
                continue

            neutral_result = self.pipeline.resolve_label(prepared, raw_probabilities=None)
            staged_results[idx] = self._build_payload(prepared, neutral_result, now_iso)
            stats["gated_neutral"] += 1

        stats["candidate_articles"] = len(prepared_items)
        if to_model:
            raw_outputs = self.backend.score_texts([item.context.text for item in to_model])
            for prepared, raw_probabilities in zip(to_model, raw_outputs):
                result = self.pipeline.resolve_label(prepared, raw_probabilities=raw_probabilities)
                staged_results[prepared.index] = self._build_payload(prepared, result, now_iso)
                stats["model_scored"] += 1

        for idx, payload in staged_results.items():
            articles[idx]["sentiment"] = payload

        stats["scored"] = len(staged_results)
        stats["duration_ms"] = int((time.time() - started) * 1000)
        return stats


def log_sentiment_rollup(
    articles: list[dict],
    logger: Optional[logging.Logger] = None,
) -> None:
    """Log a compact rollup of average compound score by feed + category."""
    log = logger or logging.getLogger("sentiment_finbert")

    buckets: dict[tuple[str, str], list[float]] = {}
    for article in articles:
        sentiment = article.get("sentiment") or {}
        compound = sentiment.get("compound")
        if compound is None:
            continue
        key = (
            str(article.get("feed") or "Unknown"),
            str(article.get("category") or "Unknown"),
        )
        buckets.setdefault(key, []).append(float(compound))

    if not buckets:
        log.info("No sentiment scores to summarize.")
        return

    log.info("Sentiment rollup (avg compound by feed/category):")
    rows = sorted(
        ((key[0], key[1], sum(values) / len(values), len(values)) for key, values in buckets.items()),
        key=lambda row: (row[0], row[1]),
    )
    for feed, category, avg_compound, count in rows:
        log.info(
            "  %s | %-25s | avg=%+.3f | n=%s",
            feed,
            category,
            avg_compound,
            count,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run measured sentiment scoring on feed JSON."
    )
    parser.add_argument("--input", required=True, help="Path to input feed JSON.")
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSON (can be same as --input).",
    )
    parser.add_argument("--model", default="ProsusAI/finbert")
    parser.add_argument(
        "--backend",
        default="finbert",
        help="Sentiment backend to use (default: finbert).",
    )
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-length", type=int, default=128)
    parser.add_argument(
        "--pipeline-mode",
        choices=[PIPELINE_MODE_BASELINE, PIPELINE_MODE_COMMODITY],
        default=PIPELINE_MODE_COMMODITY,
        help="Scoring pipeline to use: baseline or commodity_v1.",
    )
    parser.add_argument(
        "--context-mode",
        choices=[
            CONTEXT_MODE_AUTO,
            CONTEXT_MODE_TITLE,
            CONTEXT_MODE_TITLE_DESCRIPTION,
        ],
        default=CONTEXT_MODE_AUTO,
        help="Input context policy: title, title+description, or auto.",
    )
    parser.add_argument(
        "--use-description",
        action="store_true",
        help="Legacy shortcut for title+description input.",
    )
    parser.add_argument(
        "--force-rescore",
        action="store_true",
        help="Rescore all non-empty items even if cached sentiment exists.",
    )
    args = parser.parse_args()

    data = load_feed(Path(args.input))
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = SentimentConfig(
        enabled=True,
        model_name=args.model,
        model_backend=args.backend,
        batch_size=args.batch_size,
        max_length=args.max_length,
        use_description=args.use_description,
        force_rescore=args.force_rescore,
        pipeline_mode=args.pipeline_mode,
        context_mode=args.context_mode,
    )

    scorer = FinBERTScorer(config)
    stats = scorer.score_incremental(articles)

    metadata = ensure_feed_metadata(data)
    metadata["sentiment"] = stats

    save_feed(Path(args.output), data)
    print(
        f"Wrote: {args.output} "
        f"(scored={stats.get('scored', 0)}, reused={stats.get('reused', 0)}, "
        f"gated_neutral={stats.get('gated_neutral', 0)}, model_scored={stats.get('model_scored', 0)})"
    )
    log_sentiment_rollup(articles)


if __name__ == "__main__":
    main()
