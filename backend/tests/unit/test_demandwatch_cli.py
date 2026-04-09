from __future__ import annotations

from app.modules.demandwatch.cli import build_parser


def test_refresh_parser_defaults_to_manual_mode_and_accepts_multiple_sources() -> None:
    args = build_parser().parse_args(
        [
            "refresh",
            "--source",
            "demand_fred_g17",
            "--source",
            "demand_eia_wpsr",
        ]
    )

    assert args.command == "refresh"
    assert args.run_mode == "manual"
    assert args.sources == ["demand_fred_g17", "demand_eia_wpsr"]


def test_audit_parser_defaults_to_standard_artifact_paths() -> None:
    args = build_parser().parse_args(["audit"])

    assert args.command == "audit"
    assert args.fail_on == "failing"
    assert args.json_output.name == "audit.json"
    assert args.markdown_output.name == "audit.md"
