from __future__ import annotations

import ast
from pathlib import Path


def test_demandwatch_ingest_jobs_are_registered() -> None:
    registry_path = Path(__file__).resolve().parents[2] / "app" / "ingest" / "registry.py"
    tree = ast.parse(registry_path.read_text(encoding="utf-8"), filename=str(registry_path))
    registry_node = next(
        node for node in tree.body if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "JOB_REGISTRY" for target in node.targets
        )
    )
    assert isinstance(registry_node.value, ast.Dict)
    actual_jobs = {
        key.value
        for key in registry_node.value.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }
    expected_jobs = {
        "demand_eia_wpsr",
        "demand_eia_grid_monitor",
        "demand_fred_g17",
        "demand_fred_new_residential_construction",
        "demand_fred_motor_vehicle_sales",
        "demand_fred_traffic_volume_trends",
        "demand_usda_wasde",
        "demand_usda_export_sales",
        "demand_ember_monthly_electricity",
    }

    assert expected_jobs.issubset(actual_jobs)


def test_demandwatch_usda_psd_job_records_runs_under_release_slug() -> None:
    job_path = Path(__file__).resolve().parents[2] / "app" / "ingest" / "sources" / "usda_psd" / "jobs.py"
    tree = ast.parse(job_path.read_text(encoding="utf-8"), filename=str(job_path))

    create_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "create_ingest_run"
        and node.args
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
    ]

    assert any(call.args[1].value == "demand_usda_wasde" for call in create_calls)
