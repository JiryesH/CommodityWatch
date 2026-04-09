from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.api.deps import SessionDep
from app.modules.demandwatch.presentation import public_vertical_id, resolve_vertical_code
from app.processing.demandwatch import DemandWatchSetupError, get_demandwatch_snapshot_payload
from app.schemas.demandwatch import (
    DemandCoverageNotesResponse,
    DemandIndicatorTableResponse,
    DemandMacroStripResponse,
    DemandMoversResponse,
    DemandNextReleasesResponse,
    DemandScorecardResponse,
    DemandVerticalDetailResponse,
)


router = APIRouter(prefix="/api/demandwatch", tags=["demandwatch"])


async def _load_snapshot(session: SessionDep) -> dict:
    try:
        return await get_demandwatch_snapshot_payload(session)
    except DemandWatchSetupError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _require_vertical_code(raw_vertical: str, valid_codes: set[str]) -> str:
    vertical_code = resolve_vertical_code(raw_vertical)
    if vertical_code is None or vertical_code not in valid_codes:
        raise HTTPException(status_code=404, detail="DemandWatch vertical not found.")
    return vertical_code


def _valid_snapshot_vertical_codes(snapshot_payload: dict) -> set[str]:
    verticals = snapshot_payload.get("coverage_notes", {}).get("verticals", [])
    return {
        str(vertical.get("code"))
        for vertical in verticals
        if isinstance(vertical, dict) and vertical.get("code") is not None
    }


def _snapshot_vertical_detail(snapshot_payload: dict, vertical_code: str) -> dict:
    target_public_id = public_vertical_id(vertical_code)
    for detail in snapshot_payload.get("vertical_details", []):
        if not isinstance(detail, dict):
            continue
        if detail.get("code") == vertical_code or detail.get("id") == target_public_id:
            return detail

    for item in snapshot_payload.get("vertical_errors", []):
        if isinstance(item, dict) and item.get("vertical_id") == target_public_id:
            raise HTTPException(status_code=503, detail=str(item.get("message") or "DemandWatch vertical detail is unavailable."))

    raise HTTPException(status_code=404, detail="DemandWatch vertical not found.")


@router.get("/macro-strip", response_model=DemandMacroStripResponse)
async def demandwatch_macro_strip(session: SessionDep) -> DemandMacroStripResponse:
    snapshot_payload = await _load_snapshot(session)
    return DemandMacroStripResponse.model_validate(snapshot_payload["macro_strip"])


@router.get("/scorecard", response_model=DemandScorecardResponse)
async def demandwatch_scorecard(session: SessionDep) -> DemandScorecardResponse:
    snapshot_payload = await _load_snapshot(session)
    return DemandScorecardResponse.model_validate(snapshot_payload["scorecard"])


@router.get("/movers", response_model=DemandMoversResponse)
async def demandwatch_movers(
    session: SessionDep,
    limit: int = Query(default=10, ge=1, le=50),
) -> DemandMoversResponse:
    snapshot_payload = await _load_snapshot(session)
    movers_payload = snapshot_payload["movers"]
    return DemandMoversResponse.model_validate(
        {
            "generated_at": movers_payload["generated_at"],
            "items": movers_payload["items"][:limit],
        }
    )


@router.get("/verticals/{vertical_id}", response_model=DemandVerticalDetailResponse)
async def demandwatch_vertical_detail(vertical_id: str, session: SessionDep) -> DemandVerticalDetailResponse:
    snapshot_payload = await _load_snapshot(session)
    vertical_code = _require_vertical_code(vertical_id, _valid_snapshot_vertical_codes(snapshot_payload))
    return DemandVerticalDetailResponse.model_validate(_snapshot_vertical_detail(snapshot_payload, vertical_code))


@router.get("/verticals/{vertical_id}/indicator-table", response_model=DemandIndicatorTableResponse)
async def demandwatch_indicator_table(vertical_id: str, session: SessionDep) -> DemandIndicatorTableResponse:
    snapshot_payload = await _load_snapshot(session)
    vertical_code = _require_vertical_code(vertical_id, _valid_snapshot_vertical_codes(snapshot_payload))
    detail_payload = _snapshot_vertical_detail(snapshot_payload, vertical_code)
    return DemandIndicatorTableResponse.model_validate(
        {
            "generated_at": detail_payload["generated_at"],
            "vertical_id": detail_payload["id"],
            "vertical_code": detail_payload["code"],
            "sections": [
                {
                    "id": section["id"],
                    "title": section["title"],
                    "rows": section["table_rows"],
                }
                for section in detail_payload["sections"]
            ],
        }
    )


@router.get("/coverage-notes", response_model=DemandCoverageNotesResponse)
async def demandwatch_coverage_notes(session: SessionDep) -> DemandCoverageNotesResponse:
    snapshot_payload = await _load_snapshot(session)
    return DemandCoverageNotesResponse.model_validate(snapshot_payload["coverage_notes"])


@router.get("/next-release-dates", response_model=DemandNextReleasesResponse)
async def demandwatch_next_release_dates(
    session: SessionDep,
    vertical: str | None = Query(default=None),
) -> DemandNextReleasesResponse:
    snapshot_payload = await _load_snapshot(session)
    releases_payload = snapshot_payload["next_release_dates"]
    if vertical is None:
        return DemandNextReleasesResponse.model_validate(releases_payload)

    vertical_code = _require_vertical_code(vertical, _valid_snapshot_vertical_codes(snapshot_payload))
    return DemandNextReleasesResponse.model_validate(
        {
            "generated_at": releases_payload["generated_at"],
            "items": [
                item
                for item in releases_payload["items"]
                if vertical_code in item["vertical_codes"]
            ],
        }
    )
