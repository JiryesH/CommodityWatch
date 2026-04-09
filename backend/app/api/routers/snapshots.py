from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.api.deps import SessionDep
from app.processing.demandwatch import DemandWatchSetupError, get_demandwatch_snapshot_payload
from app.processing.snapshots import get_snapshot_payload
from app.schemas.demandwatch import DemandBootstrapResponse
from app.schemas.indicators import SnapshotResponse


router = APIRouter(prefix="/api/snapshot", tags=["snapshots"])


@router.get("/inventorywatch", response_model=SnapshotResponse)
async def inventorywatch_snapshot(
    session: SessionDep,
    commodity: str | None = None,
    geography: str | None = None,
    limit: int = Query(default=20, le=100),
    include_sparklines: bool = True,
) -> SnapshotResponse:
    payload = await get_snapshot_payload(session)
    if payload.get("module") != "inventorywatch":
        raise HTTPException(status_code=500, detail="Invalid snapshot payload.")

    cards = payload["cards"]
    if commodity:
        cards = [card for card in cards if card["commodity_code"] == commodity]
    if geography:
        cards = [card for card in cards if card["geography_code"] == geography]
    cards = cards[:limit]
    if not include_sparklines:
        cards = [{**card, "sparkline": []} for card in cards]
    return SnapshotResponse(
        module=payload["module"],
        generated_at=payload["generated_at"],
        expires_at=payload["expires_at"],
        cards=cards,
    )


@router.get("/demandwatch", response_model=DemandBootstrapResponse)
async def demandwatch_snapshot(session: SessionDep) -> DemandBootstrapResponse:
    try:
        payload = await get_demandwatch_snapshot_payload(session)
    except DemandWatchSetupError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if payload.get("module") != "demandwatch":
        raise HTTPException(status_code=500, detail="Invalid DemandWatch snapshot payload.")

    return DemandBootstrapResponse.model_validate(payload)
