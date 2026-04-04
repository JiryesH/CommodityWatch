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
        "demand_usda_wasde",
        "demand_usda_export_sales",
        "demand_ember_monthly_electricity",
    }

    assert expected_jobs.issubset(actual_jobs)
