from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import sentiment_finbert
from sentiment_finbert import FinBERTScorer, SentimentBackend, SentimentConfig


class MockBackend(SentimentBackend):
    def __init__(self, config: SentimentConfig, outputs: list[dict[str, float]]):
        super().__init__(config)
        self.outputs = list(outputs)
        self.calls: list[list[str]] = []

    @property
    def backend_name(self) -> str:
        return "mock"

    def score_texts(self, texts: list[str]) -> list[dict[str, float]]:
        self.calls.append(list(texts))
        if len(texts) != len(self.outputs):
            raise AssertionError(
                f"Expected {len(self.outputs)} texts but received {len(texts)}"
            )
        return list(self.outputs)


def build_scorer(
    outputs: list[dict[str, float]],
    **config_overrides: object,
) -> tuple[FinBERTScorer, MockBackend]:
    config = SentimentConfig(
        enabled=True,
        model_name="mock-model",
        pipeline_mode="commodity_v1",
        context_mode="auto",
        **config_overrides,
    )
    backend = MockBackend(config, outputs)
    return FinBERTScorer(config, backend=backend), backend


def test_summary_wrapper_neutralizes_without_model_call() -> None:
    scorer, backend = build_scorer([])
    articles = [
        {
            "id": "a1",
            "title": "NOON SNAPSHOT - Americas Markets Summary",
            "description": "CRUDE: Up, but extremely volatile. Prices surge while downstream markets split.",
        }
    ]

    stats = scorer.score_incremental(articles)

    assert stats["gated_neutral"] == 1
    assert backend.calls == []
    assert articles[0]["sentiment"]["label"] == "neutral"
    assert articles[0]["sentiment"]["input_mode"] == "title"
    assert articles[0]["sentiment"]["pipeline"]["gate_reason"] == "generic_summary_wrapper"


def test_operational_positive_rule_overrides_model_call() -> None:
    scorer, backend = build_scorer(
        [{"positive": 0.08, "negative": 0.18, "neutral": 0.74}]
    )
    articles = [{"id": "a2", "title": "OCP to restart tMAP output at end of Jan"}]

    scorer.score_incremental(articles)

    assert len(backend.calls) == 1
    assert articles[0]["sentiment"]["label"] == "positive"
    assert "operational_positive_override" in articles[0]["sentiment"]["pipeline"]["rules_applied"]


def test_operational_negative_rule_overrides_model_call() -> None:
    scorer, backend = build_scorer(
        [{"positive": 0.62, "negative": 0.09, "neutral": 0.29}]
    )
    articles = [
        {
            "id": "a3",
            "title": "INEOS Inovyn declares force majeure on PVC on Middle East crisis, applies energy surcharge",
        }
    ]

    scorer.score_incremental(articles)

    assert len(backend.calls) == 1
    assert articles[0]["sentiment"]["label"] == "negative"
    assert "operational_negative_override" in articles[0]["sentiment"]["pipeline"]["rules_applied"]


def test_commission_asset_signal_stays_positive() -> None:
    scorer, backend = build_scorer(
        [{"positive": 0.09, "negative": 0.74, "neutral": 0.17}]
    )
    articles = [{"id": "a3b", "title": "Dimeca to commission central Mexico shredder"}]

    scorer.score_incremental(articles)

    assert len(backend.calls) == 1
    assert articles[0]["sentiment"]["label"] == "positive"
    assert "operational_positive_override" in articles[0]["sentiment"]["pipeline"]["rules_applied"]


def test_selective_description_uses_description_when_title_needs_help() -> None:
    scorer, _backend = build_scorer(
        [{"positive": 0.06, "negative": 0.82, "neutral": 0.12}]
    )
    articles = [
        {
            "id": "a4",
            "title": "Marine insurers seek solutions as Gulf shipping threats evolve",
            "description": (
                "Marine insurers are exploring new ways to revive seaborne trades. "
                "More than 10 ships have been targeted, leading to crew casualties."
            ),
        }
    ]

    scorer.score_incremental(articles)

    sentiment = articles[0]["sentiment"]
    assert sentiment["input_mode"] == "title+description"
    assert sentiment["pipeline"]["context_reason"] == "auto_description_disambiguates"
    assert sentiment["label"] == "negative"


def test_two_step_pipeline_only_scores_directional_items() -> None:
    scorer, backend = build_scorer(
        [{"positive": 0.05, "negative": 0.14, "neutral": 0.81}]
    )
    articles = [
        {
            "id": "a5",
            "title": "NOON SNAPSHOT - Asia Markets Summary",
            "description": "Mixed moves across multiple markets.",
        },
        {
            "id": "a6",
            "title": "South Korea sees no LNG shortages despite Middle East supply disruptions",
            "description": "The country has sufficient LNG reserves for a considerable period.",
        },
    ]

    stats = scorer.score_incremental(articles)

    assert stats["gated_neutral"] == 1
    assert stats["model_scored"] == 1
    assert len(backend.calls) == 1
    assert len(backend.calls[0]) == 1
    assert articles[0]["sentiment"]["label"] == "neutral"
    assert articles[1]["sentiment"]["label"] == "positive"


def test_expansionary_cues_override_procedural_titles() -> None:
    scorer, backend = build_scorer(
        [
            {"positive": 0.07, "negative": 0.79, "neutral": 0.14},
            {"positive": 0.09, "negative": 0.76, "neutral": 0.15},
        ]
    )
    articles = [
        {
            "id": "a6b",
            "title": "TPC Group plans debottlenecking, enhancement at US BD facility",
        },
        {
            "id": "a6c",
            "title": "US refiners could see quick boost from Venezuela oil",
        },
    ]

    scorer.score_incremental(articles)

    assert len(backend.calls) == 1
    assert articles[0]["sentiment"]["label"] == "positive"
    assert articles[1]["sentiment"]["label"] == "positive"
    assert "operational_positive_override" in articles[0]["sentiment"]["pipeline"]["rules_applied"]
    assert "positive_market_cue_override" in articles[1]["sentiment"]["pipeline"]["rules_applied"]


def test_lower_utility_tariff_cost_relief_is_positive() -> None:
    scorer, backend = build_scorer(
        [{"positive": 0.05, "negative": 0.83, "neutral": 0.12}]
    )
    articles = [
        {"id": "a6d", "title": "Thailand floats lower 2026 green utility tariff rate"}
    ]

    scorer.score_incremental(articles)

    assert len(backend.calls) == 1
    assert articles[0]["sentiment"]["label"] == "positive"
    assert "positive_market_cue_override" in articles[0]["sentiment"]["pipeline"]["rules_applied"]


def test_blockage_and_strain_cues_override_neutral_bias() -> None:
    scorer, backend = build_scorer(
        [
            {"positive": 0.12, "negative": 0.18, "neutral": 0.7},
            {"positive": 0.16, "negative": 0.09, "neutral": 0.75},
            {"positive": 0.61, "negative": 0.11, "neutral": 0.28},
        ]
    )
    articles = [
        {"id": "a6e", "title": "US says it can not pay tariff refunds quickly"},
        {
            "id": "a6f",
            "title": "Putin muses on early end to Russian gas exports to Europe, will instruct government to work on idea",
        },
        {
            "id": "a6g",
            "title": "INFOGRAPHIC: EU's flagship carbon market buckling under political strain",
            "description": (
                "The European Commission is set to review key rules under the bloc's "
                "EU Emissions Trading System in the third quarter of 2026."
            ),
        },
    ]

    scorer.score_incremental(articles)

    assert len(backend.calls) == 1
    for article in articles:
        assert article["sentiment"]["label"] == "negative"
        assert "negative_market_cue_override" in article["sentiment"]["pipeline"]["rules_applied"]
    assert articles[2]["sentiment"]["pipeline"]["signals"]["positive_ops"] == []


def test_viewpoint_and_admin_updates_gate_neutral_without_model_call() -> None:
    scorer, backend = build_scorer([])
    articles = [
        {"id": "a6h", "title": "Viewpoint: Pemex fuel imports likely lower in 2026"},
        {"id": "a6i", "title": "EU lowers CBAM benchmarks for aluminium"},
        {"id": "a6j", "title": "Pakistan hikes retail gasoline, diesel prices"},
    ]

    stats = scorer.score_incremental(articles)

    assert stats["gated_neutral"] == 3
    assert backend.calls == []
    assert articles[0]["sentiment"]["pipeline"]["gate_reason"] == "procedural_headline"
    assert articles[1]["sentiment"]["pipeline"]["gate_reason"] == "factual_macro_headline"
    assert articles[2]["sentiment"]["pipeline"]["gate_reason"] == "factual_macro_headline"
    for article in articles:
        assert article["sentiment"]["label"] == "neutral"


def test_output_schema_remains_backward_compatible() -> None:
    scorer, _backend = build_scorer(
        [{"positive": 0.7, "negative": 0.1, "neutral": 0.2}]
    )
    articles = [{"id": "a7", "title": "Urea derivatives surge on Middle East conflict"}]

    scorer.score_incremental(articles)

    sentiment = articles[0]["sentiment"]
    for key in (
        "label",
        "confidence",
        "probabilities",
        "compound",
        "model",
        "input_mode",
        "text_hash",
        "scored_at",
    ):
        assert key in sentiment
    assert set(sentiment["probabilities"]) == {"positive", "negative", "neutral"}


def test_baseline_pipeline_mode_preserves_legacy_single_pass_behavior() -> None:
    config = SentimentConfig(
        enabled=True,
        model_name="mock-model",
        pipeline_mode="baseline",
        use_description=True,
        context_mode="auto",
    )
    backend = MockBackend(config, [{"positive": 0.12, "negative": 0.77, "neutral": 0.11}])
    scorer = FinBERTScorer(config, backend=backend)
    articles = [
        {
            "id": "a8",
            "title": "Generic title",
            "description": "Additional description",
        }
    ]

    scorer.score_incremental(articles)

    assert articles[0]["sentiment"]["label"] == "negative"
    assert articles[0]["sentiment"]["input_mode"] == "title+description"
    assert articles[0]["sentiment"]["pipeline"]["gate_reason"] == "baseline_no_gate"


def test_cli_main_writes_output_with_pipeline_metadata(tmp_path, monkeypatch) -> None:
    input_path = tmp_path / "feed.json"
    output_path = tmp_path / "feed.out.json"
    input_path.write_text(
        json.dumps({"articles": [{"id": "a9", "title": "OCP to restart tMAP output at end of Jan"}]}),
        encoding="utf-8",
    )

    created_backend: dict[str, MockBackend] = {}

    def fake_build_backend(config: SentimentConfig) -> MockBackend:
        backend = MockBackend(config, [{"positive": 0.1, "negative": 0.2, "neutral": 0.7}])
        created_backend["backend"] = backend
        return backend

    monkeypatch.setattr(sentiment_finbert, "build_backend", fake_build_backend)
    monkeypatch.setattr(sentiment_finbert, "log_sentiment_rollup", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sentiment_finbert.py",
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--pipeline-mode",
            "commodity_v1",
            "--context-mode",
            "auto",
        ],
    )

    sentiment_finbert.main()

    data = json.loads(output_path.read_text())
    assert data["metadata"]["sentiment"]["pipeline_mode"] == "commodity_v1"
    assert data["articles"][0]["sentiment"]["label"] == "positive"
    assert created_backend["backend"].calls
