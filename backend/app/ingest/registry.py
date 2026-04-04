from __future__ import annotations

from app.ingest.sources.agsi.jobs import fetch_agsi_daily
from app.ingest.sources.comex_warehouse.jobs import fetch_comex_warehouse
from app.ingest.sources.eia.jobs import (
    fetch_demand_eia_grid_monitor,
    fetch_demand_eia_wpsr,
    fetch_eia_wngs,
    fetch_eia_wpsr,
)
from app.ingest.sources.ember.jobs import fetch_demand_ember_monthly_electricity
from app.ingest.sources.etf_holdings.jobs import fetch_etf_holdings
from app.ingest.sources.fred.jobs import fetch_demand_fred_g17, fetch_demand_fred_new_residential_construction
from app.ingest.sources.ice_certified.jobs import fetch_ice_certified
from app.ingest.sources.lme_warehouse.jobs import fetch_lme_warehouse
from app.ingest.sources.usda_export_sales.jobs import fetch_demand_usda_export_sales
from app.ingest.sources.usda_psd.jobs import fetch_demand_usda_psd
from app.ingest.sources.usda_wasde.jobs import fetch_usda_wasde


JOB_REGISTRY = {
    "eia_wpsr": fetch_eia_wpsr,
    "eia_wngs": fetch_eia_wngs,
    "demand_eia_wpsr": fetch_demand_eia_wpsr,
    "demand_eia_grid_monitor": fetch_demand_eia_grid_monitor,
    "demand_fred_g17": fetch_demand_fred_g17,
    "demand_fred_new_residential_construction": fetch_demand_fred_new_residential_construction,
    "demand_usda_wasde": fetch_demand_usda_psd,
    "demand_usda_export_sales": fetch_demand_usda_export_sales,
    "demand_ember_monthly_electricity": fetch_demand_ember_monthly_electricity,
    "agsi_daily": fetch_agsi_daily,
    "usda_wasde": fetch_usda_wasde,
    "lme_warehouse": fetch_lme_warehouse,
    "comex_warehouse": fetch_comex_warehouse,
    "etf_holdings": fetch_etf_holdings,
    "ice_certified": fetch_ice_certified,
}
