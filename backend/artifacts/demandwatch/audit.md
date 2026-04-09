# DemandWatch Operational Audit

Generated at: 2026-04-08T21:19:21.027670+00:00

## Run Manifest

- Operation: audit
- Signature: `b2b6d3e32c688a22f141b4f616f4d77ef6321ad492f1bed224a29b645ffa53d2`
- Sources: demand_eia_grid_monitor, demand_eia_wpsr, demand_ember_monthly_electricity, demand_fred_g17, demand_fred_motor_vehicle_sales, demand_fred_new_residential_construction, demand_fred_traffic_volume_trends, demand_usda_export_sales, demand_usda_wasde
- Run mode: manual

## Source Health

- Healthy: 6
- Degraded: 3
- Failing: 0
- Deferred: 0

| Feed | Status | Active Series | Stale | Last Run | Reasons |
| --- | --- | ---: | ---: | --- | --- |
| EIA WPSR | healthy | 6 | 0 | success | - |
| EIA Grid Monitor | degraded | 1 | 0 | success | 1 active series have partial coverage. |
| FRED G.17 | healthy | 3 | 0 | success | - |
| FRED Housing | healthy | 2 | 0 | success | Most recent failed run was classified as http_error. |
| FRED Vehicle Sales | healthy | 1 | 0 | success | - |
| FRED Traffic Volume Trends | healthy | 1 | 0 | success | - |
| USDA PSD/WASDE | degraded | 3 | 0 | success | 3 active series have partial coverage.; Most recent failed run was classified as parse_error. |
| USDA Export Sales | healthy | 3 | 0 | success | Most recent failed run was classified as parse_error. |
| Ember Monthly Electricity | degraded | 2 | 0 | partial | 2 active series have partial coverage.; Latest ingest run finished with partial status.; Most recent failed run was classified as parse_error. |

## Canonical Units

- OK: 27
- Violations: 0
- Missing policy: 0

# DemandWatch Coverage Audit

Generated at: 2026-04-08T21:19:21.027670+00:00

## Summary

- Verticals: 4
- Indicators: 27
- Live: 16
- Partial: 6
- Deferred: 3
- Blocked: 2

## By Vertical

| Vertical | Live | Partial | Deferred | Blocked |
| --- | ---: | ---: | ---: | ---: |
| Crude Oil + Refined Products | 6 | 0 | 2 | 1 |
| Electricity / Power | 0 | 3 | 0 | 0 |
| Grains & Oilseeds | 4 | 3 | 0 | 0 |
| Base Metals | 6 | 0 | 1 | 1 |

## Indicator Detail

| Vertical | Status | Code | Tier | Latest | Freshness | Reasons |
| --- | --- | --- | --- | --- | --- | --- |
| Crude Oil + Refined Products | live | EIA_US_TOTAL_PRODUCT_SUPPLIED | t1_direct | Week ending 2026-04-03 | fresh | - |
| Crude Oil + Refined Products | live | EIA_GASOLINE_US_PRODUCT_SUPPLIED | t1_direct | Week ending 2026-04-03 | fresh | - |
| Crude Oil + Refined Products | live | EIA_DISTILLATE_US_PRODUCT_SUPPLIED | t1_direct | Week ending 2026-04-03 | fresh | - |
| Crude Oil + Refined Products | live | EIA_CRUDE_US_REFINERY_INPUTS | t2_throughput | Week ending 2026-04-03 | fresh | - |
| Crude Oil + Refined Products | live | EIA_CRUDE_US_REFINERY_UTILISATION | t2_throughput | Week ending 2026-04-03 | fresh | - |
| Crude Oil + Refined Products | live | FRED_US_VEHICLE_MILES_TRAVELED | t4_end_use | Jan 2026 | fresh | - |
| Crude Oil + Refined Products | deferred | CHINA_CRUDE_IMPORTS_MONTHLY | t3_trade | - | unknown | Direct China customs republication terms are unresolved. Keep out of ingestion until legal review clears systematic use. |
| Crude Oil + Refined Products | deferred | CHINA_REFINERY_THROUGHPUT_MONTHLY | t2_throughput | - | unknown | NBS throughput publication terms remain unresolved for commercial republication. |
| Crude Oil + Refined Products | blocked | IEA_GLOBAL_OIL_DEMAND_TABLE | t6_macro | - | unknown | IEA data tables are not cleared for MVP republication. Use EIA or qualitative commentary instead. |
| Electricity / Power | partial | EIA_US_ELECTRICITY_GRID_LOAD | t1_direct | 2026-04-08 19:00 UTC | fresh | Backfill window is 14 days; DemandWatch MVP expects at least 1095 days. |
| Electricity / Power | partial | EMBER_GLOBAL_ELECTRICITY_DEMAND | t1_direct | - | unknown | No observations have been published.; Backfill window is 0 days; DemandWatch MVP expects at least 1095 days. |
| Electricity / Power | partial | EMBER_CHINA_ELECTRICITY_DEMAND | t1_direct | - | unknown | No observations have been published.; Backfill window is 0 days; DemandWatch MVP expects at least 1095 days. |
| Grains & Oilseeds | live | USDA_US_CORN_EXPORT_SALES | t3_trade | Week ending 2026-03-26 | fresh | - |
| Grains & Oilseeds | live | USDA_US_SOYBEAN_EXPORT_SALES | t3_trade | Week ending 2026-03-26 | fresh | - |
| Grains & Oilseeds | live | USDA_US_WHEAT_EXPORT_SALES | t3_trade | Week ending 2026-03-26 | fresh | - |
| Grains & Oilseeds | live | EIA_US_ETHANOL_PRODUCTION | t2_throughput | Week ending 2026-04-03 | fresh | - |
| Grains & Oilseeds | partial | USDA_US_CORN_TOTAL_USE_WASDE | t1_direct | Mar 2026 | fresh | Backfill window is 0 days; DemandWatch MVP expects at least 1095 days. |
| Grains & Oilseeds | partial | USDA_US_SOYBEAN_TOTAL_USE_WASDE | t1_direct | - | unknown | No observations have been published.; Backfill window is 0 days; DemandWatch MVP expects at least 1095 days. |
| Grains & Oilseeds | partial | USDA_US_WHEAT_TOTAL_USE_WASDE | t1_direct | Mar 2026 | fresh | Backfill window is 0 days; DemandWatch MVP expects at least 1095 days. |
| Base Metals | live | FRED_US_INDUSTRIAL_PRODUCTION | t6_macro | Feb 2026 | fresh | - |
| Base Metals | live | FRED_US_MANUFACTURING_PRODUCTION | t6_macro | Feb 2026 | fresh | - |
| Base Metals | live | FRED_US_MANUFACTURING_CAPACITY_UTILISATION | t6_macro | Feb 2026 | fresh | - |
| Base Metals | live | FRED_US_HOUSING_STARTS | t4_end_use | Jan 2026 | fresh | - |
| Base Metals | live | FRED_US_TOTAL_VEHICLE_SALES | t4_end_use | Mar 2026 | fresh | - |
| Base Metals | live | FRED_US_BUILDING_PERMITS | t5_leading | Jan 2026 | fresh | - |
| Base Metals | deferred | WORLDSTEEL_GLOBAL_CRUDE_STEEL_PRODUCTION | t2_throughput | - | unknown | Monthly steel-output press releases stay blocked until republication terms are verified. |
| Base Metals | blocked | SPGLOBAL_EUROZONE_MANUFACTURING_PMI_RAW | t6_macro | - | unknown | Raw S&P Global PMI values are off-limits without a licence; do not ingest for MVP. |