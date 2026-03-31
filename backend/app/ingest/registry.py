from __future__ import annotations

from app.ingest.sources.agsi.jobs import fetch_agsi_daily
from app.ingest.sources.eia.jobs import fetch_eia_wngs, fetch_eia_wpsr


JOB_REGISTRY = {
    "eia_wpsr": fetch_eia_wpsr,
    "eia_wngs": fetch_eia_wngs,
    "agsi_daily": fetch_agsi_daily,
}

