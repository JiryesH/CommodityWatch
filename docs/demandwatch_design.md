# DemandWatch — Module Design Document

**Platform:** Contango (CommodityWatch.co)
**Version:** 1.0 Draft
**Date:** March 2026

---

## A. Demand Indicator Taxonomy

Demand indicators are organised into seven tiers, ranked by directness of measurement. Every indicator displayed in DemandWatch carries a **tier badge** so users always know whether they're looking at measured consumption or an inferred proxy.

| Tier | Label | Description | Reliability | Example |
|------|-------|-------------|-------------|---------|
| T1 | **Direct Consumption** | Measured volumes actually consumed or delivered | Highest | EIA product supplied, Ember electricity demand |
| T2 | **Throughput Proxy** | Processing volumes that imply input demand | High | Refinery crude inputs, soybean crush volumes |
| T3 | **Trade Flow** | Import/export volumes measuring cross-border demand | High | China crude imports, US soybean export sales |
| T4 | **End-Use Sector Activity** | Output from consuming industries | Medium-High | Auto production, housing starts, steel output |
| T5 | **Leading Indicator** | Forward-looking signals of future demand | Medium | Building permits, new orders, planting intentions |
| T6 | **Macro Context** | Broad economic backdrop | Medium | GDP, industrial production, housing starts |
| T7 | **Weather-Derived** | Temperature-driven consumption signals | Conditional | HDD, CDD (high reliability for gas/power, limited for others) |

**Display convention:** Each chart and data point in DemandWatch carries a small tier tag (e.g., `T1 · Direct` or `T4 · End-Use`). This makes the data quality spectrum visible without requiring users to read footnotes.

---

## B. Indicator Map by Commodity Vertical

### B1. Crude Oil

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| Total product supplied (proxy for US petroleum demand) | EIA Weekly Petroleum Status Report (WPSR) | Public Domain | Weekly (Wed) | US | API (EIA Open Data) | ~5 days | T1 · Direct | **High** |
| Gasoline product supplied | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T1 · Direct | **High** |
| Distillate product supplied | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T1 · Direct | **High** |
| Jet fuel product supplied | EIA Monthly Energy Review / Petroleum Supply Monthly | Public Domain | Monthly | US | API | ~60 days | T1 · Direct | Medium |
| Refinery crude oil inputs | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T2 · Throughput | **High** |
| Refinery utilisation rate (%) | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T2 · Throughput | **High** |
| US implied demand (4-week avg) | EIA WPSR (derived) | Public Domain | Weekly | US | API (calculated) | ~5 days | T1 · Direct | **High** |
| China crude oil imports | China Customs (via USDA FAS GAIN / state media reporting) | **Needs Verification** (NBS/Customs direct); Public Domain (FAS GAIN commentary) | Monthly | China | PDF / web scrape | ~3–4 weeks | T3 · Trade Flow | **High** |
| China refinery throughput | China NBS | **Needs Verification** | Monthly | China | Web page | ~3–4 weeks | T2 · Throughput | **High** |
| India petroleum consumption | PPAC (Petroleum Planning & Analysis Cell) | **Needs Verification** | Monthly | India | PDF / web page | ~4–6 weeks | T1 · Direct | Medium |
| EU petroleum product inland deliveries | Eurostat (nrg_cb_oil) | CC BY 4.0 | Monthly | EU-27 | API (Eurostat SDMX) | ~8–10 weeks | T1 · Direct | Medium |
| Vehicle miles travelled (VMT) | Federal Highway Administration / BTS | Public Domain | Monthly | US | FRED API (series: TRFVOLUSM227NFWA) | ~6–8 weeks | T4 · End-Use | Medium |
| Airline passengers (TSA throughput) | TSA | Public Domain | Daily | US | Web page (daily checkpoint data) | 1 day | T4 · End-Use | Medium |
| IATA air passenger traffic (RPK) | IATA press releases | Press Release (verify redistribution) | Monthly | Global | PDF press release | ~6–8 weeks | T4 · End-Use | Medium |
| ATA Trucking Tonnage Index | American Trucking Associations | **Needs Verification** (press release summary may be usable; full series likely paid) | Monthly | US | Press release / PDF | ~4 weeks | T4 · End-Use | Medium |
| US GDP growth | BEA (via FRED: GDP, GDPC1) | Public Domain | Quarterly | US | API (FRED) | Advance: ~4 weeks; final: ~12 weeks | T6 · Macro | Medium |
| Global oil demand (IEA commentary) | IEA Oil Market Report — CC-licensed text/analysis only | CC BY 4.0 (text only; data tables stay out of scope) | Monthly | Global | Web page | ~2–3 weeks | T6 · Macro | **High** (qualitative) |

**Key gap — Global oil demand estimates:** DemandWatch will not roadmap restricted third-party global oil demand tables. EIA's STEO public-domain forecasts remain the numeric anchor, with IEA CC-licensed qualitative commentary used only as narrative context.

---

### B2. Natural Gas

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| US natural gas total consumption | EIA Natural Gas Monthly / Weekly | Public Domain | Weekly/Monthly | US | API | Weekly: ~5 days; Monthly: ~8 weeks | T1 · Direct | **High** |
| US gas consumption by sector (residential, commercial, industrial, power) | EIA Natural Gas Monthly | Public Domain | Monthly | US | API | ~8 weeks | T1 · Direct | **High** |
| US power burn (gas for electricity) | EIA Electric Power Monthly / derived from EIA-923 | Public Domain | Monthly | US | API | ~8 weeks | T1 · Direct | **High** |
| US residential/commercial gas deliveries | EIA Natural Gas Monthly | Public Domain | Monthly | US | API | ~8 weeks | T1 · Direct | Medium |
| EU gas consumption | Eurostat (nrg_cb_gas) | CC BY 4.0 | Monthly/Quarterly | EU-27 | API (Eurostat SDMX) | ~8–10 weeks | T1 · Direct | Medium |
| Japan LNG imports | Japan e-Stat / Trade Statistics of Japan | Live-safe candidate; operational verification pending | Monthly | Japan | API (e-Stat DB API) | ~2–3 weeks after month-end | T3 · Trade Flow | Medium |
| South Korea LNG imports | Korea Customs Service via Public Data Portal | Live-safe candidate; operational verification pending | Monthly | South Korea | API (OpenAPI XML) | ~15th of following month | T3 · Trade Flow | Medium |
| China LNG imports | China Customs (via state media / FAS GAIN) | **Needs Verification** (direct); Public Domain (FAS) | Monthly | China | PDF / web | ~3–4 weeks | T3 · Trade Flow | **High** |
| Heating Degree Days (HDD) | NOAA (US), ECMWF/national met services (EU) | Public Domain (NOAA) / **Needs Verification** (ECMWF) | Daily/Weekly | US, EU, Asia | API / CSV | 1 day (US) | T7 · Weather | **High** (winter) |
| Cooling Degree Days (CDD) | NOAA (US) | Public Domain | Daily/Weekly | US | API / CSV | 1 day | T7 · Weather | **High** (summer) |
| US industrial production index | Federal Reserve (G.17 release; FRED: INDPRO) | Public Domain | Monthly | US | API (FRED) | ~2 weeks | T6 · Macro | Medium |

**Weather-derived demand rule:** HDD/CDD data is sourced from and stored in **WeatherWatch**. DemandWatch displays it contextually with a "via WeatherWatch" attribution and link. No data duplication — single source of truth in WeatherWatch, read-only reference in DemandWatch.

---

### B3. Refined Products

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| US gasoline product supplied | EIA WPSR | Public Domain | Weekly (Wed) | US | API | ~5 days | T1 · Direct | **High** |
| US distillate (diesel/heating oil) product supplied | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T1 · Direct | **High** |
| US jet fuel product supplied | EIA Petroleum Supply Monthly | Public Domain | Monthly | US | API | ~60 days | T1 · Direct | Medium |
| US propane/propylene product supplied | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T1 · Direct | Medium |
| US residual fuel oil product supplied | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T1 · Direct | Low |
| US gasoline demand implied (4-week avg) | EIA WPSR (derived) | Public Domain | Weekly | US | API | ~5 days | T1 · Direct | **High** |
| EU petroleum inland deliveries by product | Eurostat (nrg_cb_oil) | CC BY 4.0 | Monthly | EU-27 | API | ~8–10 weeks | T1 · Direct | Medium |
| VMT (gasoline proxy) | FHWA / BTS (FRED: TRFVOLUSM227NFWA) | Public Domain | Monthly | US | API (FRED) | ~6–8 weeks | T4 · End-Use | Medium |
| TSA passenger throughput (jet fuel proxy) | TSA | Public Domain | Daily | US | Web page | 1 day | T4 · End-Use | Medium |
| US vehicle sales (gasoline structural demand) | BEA (FRED: TOTALSA) | Public Domain | Monthly | US | API (FRED) | ~3 days | T5 · Leading | Medium |
| US EV sales share | IEA Global EV Outlook (CC-licensed portions) / automaker press releases | CC BY 4.0 (IEA text) / Press Release | Quarterly / varies | US, Global | Web / PDF | Varies | T5 · Leading | Low (structural) |

---

### B4. Electricity / Power

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| US electricity demand (grid load) | EIA Hourly Electric Grid Monitor | Public Domain | Hourly | US (by region/ISO) | API / web | Near real-time | T1 · Direct | **High** |
| US electricity consumption by sector | EIA Electric Power Monthly (Table 5.1) | Public Domain | Monthly | US | API | ~8 weeks | T1 · Direct | **High** |
| US electricity retail sales | EIA (FRED: ELTOTUSM44MNBES) | Public Domain | Monthly | US | API (FRED) | ~8 weeks | T1 · Direct | Medium |
| EU electricity demand by country | Ember (Global Electricity Review) | CC BY 4.0 | Monthly/Annual | EU + 200 countries | CSV / API | ~4–8 weeks | T1 · Direct | **High** |
| Global electricity demand by country | Ember | CC BY 4.0 | Monthly/Annual | 200+ countries | CSV | Varies | T1 · Direct | **High** |
| ENTSO-E electricity demand (European TSOs) | ENTSO-E Transparency Platform | **Needs Verification** (data is published openly but redistribution terms need checking) | Hourly/Daily | EU/EEA | API (RESTful) | Near real-time | T1 · Direct | Medium |
| India electricity consumption | Central Electricity Authority (CEA) | **Needs Verification** | Daily/Monthly | India | Web page / PDF | ~1–2 days (daily); ~4 weeks (monthly) | T1 · Direct | Medium |
| China electricity consumption | NBS / China Electricity Council | **Needs Verification** | Monthly | China | Web page | ~3–4 weeks | T1 · Direct | **High** |
| CDD/HDD (power demand driver) | NOAA | Public Domain | Daily | US | API | 1 day | T7 · Weather | **High** |
| EV stock / sales (structural electricity demand) | IEA Global EV Outlook (CC portions) | CC BY 4.0 (text/graphs) | Annual / Quarterly | Global | Web / PDF | Varies | T5 · Leading | Low (structural) |
| Data centre power consumption estimates | Ember / IEA (CC portions) | CC BY 4.0 | Annual | Global / US | Web / CSV | ~6–12 months | T5 · Leading | Low (structural) |

**Electricity is the strongest vertical for DemandWatch.** It has near-real-time direct measurement (EIA Grid Monitor, ENTSO-E, Ember), global coverage, and high market relevance. This is a natural MVP candidate.

---

### B5. Coal

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| US coal consumption (electric power sector) | EIA Electric Power Monthly / Short-Term Energy Outlook | Public Domain | Monthly | US | API | ~8 weeks | T1 · Direct | Medium |
| US coal consumption (all sectors) | EIA Quarterly Coal Report | Public Domain | Quarterly | US | API | ~12 weeks | T1 · Direct | Medium |
| Global coal demand by country | Ember | CC BY 4.0 | Monthly/Annual | Global | CSV | ~4–8 weeks | T1 · Direct | **High** |
| China coal consumption | NBS (via state media reporting) | **Needs Verification** | Monthly | China | Web page | ~3–4 weeks | T1 · Direct | **High** |
| India coal consumption (power sector) | CEA / Coal India press releases | **Needs Verification** | Monthly | India | PDF / web | ~4 weeks | T1 · Direct | Medium |
| Coal-to-gas switching price (derived) | Calculated from EIA gas prices + coal prices + heat rates | Public Domain (inputs) | Weekly/Daily | US | Calculated | ~5 days | T4 · End-Use | Medium |
| EU coal consumption | Eurostat (nrg_cb_sff) | CC BY 4.0 | Monthly/Quarterly | EU-27 | API | ~8–10 weeks | T1 · Direct | Medium |

---

### B6. Base Metals (Copper, Aluminium, Zinc, Nickel)

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| US industrial production index (total + manufacturing) | Federal Reserve G.17 (FRED: INDPRO, IPMAN) | Public Domain | Monthly | US | API (FRED) | ~2 weeks | T6 · Macro | **High** |
| US capacity utilisation | Federal Reserve G.17 (FRED: TCU, MCUMFN) | Public Domain | Monthly | US | API (FRED) | ~2 weeks | T6 · Macro | Medium |
| OECD Composite Leading Indicators (Japan, South Korea, India) | OECD SDMX API | Official public terms with attribution | Monthly | Japan, South Korea, India | API (SDMX CSV) | ~1–2 weeks after month-end | T6 · Macro | Medium |
| EU industrial production | Eurostat (sts_inpr_m) | CC BY 4.0 | Monthly | EU-27 | API | ~6–8 weeks | T6 · Macro | **High** |
| China industrial production (YoY %) | NBS | **Needs Verification** | Monthly | China | Web page | ~3–4 weeks | T6 · Macro | **High** |
| US housing starts | Census Bureau (FRED: HOUST) | Public Domain | Monthly | US | API (FRED) | ~2–3 weeks | T4 · End-Use | **High** |
| US building permits | Census Bureau (FRED: PERMIT) | Public Domain | Monthly | US | API (FRED) | ~2–3 weeks | T5 · Leading | **High** |
| US construction spending | Census Bureau (FRED: TTLCONS) | Public Domain | Monthly | US | API (FRED) | ~4 weeks | T4 · End-Use | Medium |
| Global auto sales (by manufacturer) | Automaker press releases (Toyota, VW, GM, Stellantis, BYD, Hyundai) | Press Release (designed for public distribution) | Monthly/Quarterly | Global | Press release / PDF | ~2–4 weeks | T4 · End-Use | **High** |
| Global crude steel production | World Steel Association (worldsteel) press releases | Press Release — **verify if press releases are freely republishable** | Monthly | Global (top 64 producers) | Press release / PDF | ~4 weeks | T2 · Throughput (for iron ore/coking coal) / T4 · End-Use (for metals generally) | **High** |
| China fixed asset investment (YoY %) | NBS | **Needs Verification** | Monthly | China | Web page | ~3–4 weeks | T4 · End-Use | **High** |
| Semiconductor sales | SIA (Semiconductor Industry Association) press releases | Press Release | Monthly | Global | Press release / web | ~6–8 weeks | T4 · End-Use | Medium |
| US durable goods orders | Census Bureau (FRED: DGORDER) | Public Domain | Monthly | US | API (FRED) | ~4 weeks | T5 · Leading | Medium |
| China copper/aluminium imports | China Customs (via state media / FAS) | **Needs Verification** (direct); Public Domain (FAS GAIN for context) | Monthly | China | Web / PDF | ~3–4 weeks | T3 · Trade Flow | **High** |

---

### B7. Precious Metals

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| Gold ETF holdings (SPDR GLD, iShares IAU) | Fund sponsors (State Street, BlackRock) — publicly reported | Press Release / Public Filing | Daily | Global | Web page / CSV | 1 day | T1 · Direct | **High** |
| US Mint gold/silver coin sales | US Mint | Public Domain | Monthly | US | Web page | ~1–2 weeks | T1 · Direct | Low |
| US real interest rates (TIPS yields) | Federal Reserve / Treasury (FRED: DFII10) | Public Domain | Daily | US | API (FRED) | 1 day | T6 · Macro | **High** (gold) |
| Global solar PV installations (silver demand proxy) | IEA / Ember (CC portions); IRENA | CC BY 4.0 (IEA text, Ember data); **Needs Verification** (IRENA) | Annual / Quarterly | Global | CSV / web | ~3–6 months | T4 · End-Use | Medium (silver) |
| Global auto production (PGM demand proxy) | Automaker press releases | Press Release | Monthly/Quarterly | Global | Press release | ~2–4 weeks | T4 · End-Use | **High** (Pt/Pd) |
| Fed funds rate / rate expectations | Federal Reserve (FRED: FEDFUNDS) | Public Domain | Daily (effective rate); 8x/year (decisions) | US | API (FRED) | 1 day | T6 · Macro | **High** (gold) |

---

### B8. Grains & Oilseeds

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| WASDE demand tables (domestic use, feed, food/seed/industrial, ethanol, exports) | USDA WASDE | Public Domain | Monthly (~12th) | US + Global | PDF / API (USDA FAS PSD Online) | Same day | T1 · Direct (estimated) | **High** |
| US ethanol production | EIA WPSR | Public Domain | Weekly | US | API | ~5 days | T2 · Throughput | **High** (corn) |
| US weekly export sales (by commodity) | USDA FAS Export Sales Report | Public Domain | Weekly (Thursday) | US exports by destination | CSV / web | ~1 week | T3 · Trade Flow | **High** |
| US weekly export inspections | USDA AMS / GIPSA | Public Domain | Weekly (Monday) | US | Web / CSV | ~1 week | T3 · Trade Flow | Medium |
| China soybean/corn/wheat imports | USDA FAS GAIN reports; China Customs (via state media) | Public Domain (FAS GAIN); **Needs Verification** (direct Customs) | Monthly | China | PDF (GAIN); web | ~3–4 weeks | T3 · Trade Flow | **High** |
| USDA NASS Cattle on Feed (feed demand context) | USDA NASS | Public Domain | Monthly | US | PDF / API | ~3 weeks | T4 · End-Use | Medium |
| USDA Prospective Plantings (fertiliser demand driver) | USDA NASS | Public Domain | Annual (March 31) | US | PDF / API | Same day | T5 · Leading | **High** |
| USDA Crop Progress (growth stage = harvest timing = demand timing) | USDA NASS | Public Domain | Weekly (Mon, Apr–Nov) | US | PDF / API | Same day | T5 · Leading | Medium |
| Soybean crush margins (derived) | Calculated from CBOT futures (publicly quoted) | Public Domain (inputs) | Daily | US | Calculated | 1 day | T5 · Leading | Medium |
| USDA PSD (Production, Supply & Demand) Online | USDA FAS | Public Domain | Monthly (with WASDE) | Global | API / CSV | Same day | T1 · Direct (estimated) | **High** |
| Global population / income growth (structural food demand) | World Bank WDI | CC BY 3.0 IGO | Annual | Global | API / CSV | ~6 months | T6 · Macro | Low (structural) |

---

### B9. Soft Commodities

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| ICO Coffee Market Report | International Coffee Organization | **Needs Verification** | Monthly | Global | PDF | ~4 weeks | T1 · Direct (estimated) | Medium |
| US coffee imports | Census Bureau / USDA FAS | Public Domain | Monthly | US | API / CSV | ~6–8 weeks | T3 · Trade Flow | Medium |
| EU coffee imports | Eurostat | CC BY 4.0 | Monthly | EU-27 | API | ~8 weeks | T3 · Trade Flow | Medium |
| Cocoa grindings (ICCO / national associations) | ICCO, European Cocoa Association, NCA (US), Cocoa Association of Asia | **Needs Verification** (press release summaries of grindings may be usable) | Quarterly | Global (by region) | Press release / PDF | ~2–4 weeks after quarter-end | T2 · Throughput | **High** (cocoa) |
| USDA FAS sugar PSD | USDA FAS PSD Online | Public Domain | Semi-annual (with WASDE) | Global | API / CSV | Same day | T1 · Direct (estimated) | Medium |
| Brazil sugarcane crush / ethanol mix | UNICA (Brazilian Sugarcane Industry Association) | **Needs Verification** (UNICA publishes bi-weekly reports; some data is public, full datasets may be subscription) | Bi-weekly (harvest season) | Brazil | Web / PDF | ~2 weeks | T2 · Throughput | **High** (sugar) |
| USDA cotton PSD / mill use | USDA FAS PSD Online / WASDE | Public Domain | Monthly | Global | API / CSV | Same day | T1 · Direct (estimated) | Medium |
| ICAC cotton data | International Cotton Advisory Committee | **Needs Verification** (some data published publicly; full dataset likely paid) | Monthly / Quarterly | Global | PDF / web | ~4–6 weeks | T1 · Direct | Medium |

---

### B10. Fertilisers

| Indicator | Source | Republication | Frequency | Geography | Format | Lag | Tier | Market Impact |
|-----------|--------|---------------|-----------|-----------|--------|-----|------|--------------|
| US planted acreage (demand driver) | USDA NASS Prospective Plantings / Acreage | Public Domain | Annual (March + June) | US | PDF / API | Same day | T5 · Leading | **High** |
| US fertiliser imports by type | Census Bureau (trade data) / USDA FAS | Public Domain | Monthly | US | API / CSV | ~6–8 weeks | T3 · Trade Flow | Medium |
| India Department of Fertilizers monthly summary data | Department of Fertilizers (India) | Live-safe source terms appear acceptable; parser and metric contract still pending | Monthly / Quarterly | India | PDF / web | ~4–8 weeks | T3 · Trade Flow | Medium |
| Brazil fertiliser imports | ANDA (Brazilian fertiliser association) / Comex Stat | **Needs Verification** (ANDA); Comex Stat is public | Monthly | Brazil | Web / CSV | ~4 weeks | T3 · Trade Flow | Medium |
| Global fertiliser demand (IFA) | International Fertilizer Association | **Off-Limits** (paid subscription for granular data; press release summaries may be usable — verify) | Annual / Semi-annual | Global | PDF | ~3–6 months | T1 · Direct | Medium |
| USDA crop prices (profitability = planting incentive = fertiliser demand) | USDA NASS | Public Domain | Monthly | US | API | ~4 weeks | T5 · Leading | Medium |

**Acknowledged gap:** Fertilisers have the thinnest demand-side coverage of any vertical. The most practical workaround is to use planted acreage and crop prices as the primary demand proxies (both public domain from USDA) and supplement with trade flow data. Direct fertiliser consumption data at useful frequency is largely behind industry paywalls (IFA, CRU, Fertecon/IHS). For MVP, fertilisers should be a sub-section within Grains & Oilseeds rather than a standalone vertical, with a roadmap note that a commercial data license (IFA or CRU) would close the gap.

---

## C. Data Access Methods

### C1. API-First Sources (Automated Ingestion)

| Source | API | Key Required | Documentation |
|--------|-----|--------------|---------------|
| **EIA Open Data** | Yes (v2 API) | Free key (api.eia.gov) | developer.eia.gov |
| **FRED** | Yes (RESTful) | Free key | fred.stlouisfed.org/docs/api/fred/ |
| **Census Bureau** | Yes (data.census.gov API) | Free key | census.gov/data/developers.html |
| **BLS** | Yes (v2 Public Data API) | Free key (for higher rate limits) | bls.gov/developers/ |
| **BEA** | Yes (RESTful) | Free key | apps.bea.gov/api/ |
| **USDA FAS PSD Online** | Yes (apps.fas.usda.gov/psdonline) | No key needed | Public download / API |
| **Eurostat** | Yes (SDMX / JSON-stat) | No key needed | ec.europa.eu/eurostat/web/main/data/database |
| **World Bank** | Yes (RESTful) | No key needed | data.worldbank.org/developer |
| **Ember** | CSV downloads; some API access | Check | ember-climate.org/data/ |
| **IMF** | Yes (WEO data API) | No key needed | Check terms for commercial use |

**The FRED Advantage:** A single FRED API key unlocks the following DemandWatch-relevant series (partial list):

- `INDPRO` — Industrial Production Index
- `IPMAN` — Industrial Production: Manufacturing
- `TCU` / `MCUMFN` — Capacity Utilisation
- `HOUST` — Housing Starts
- `PERMIT` — Building Permits
- `TTLCONS` — Total Construction Spending
- `RSAFS` — Retail Sales
- `PAYEMS` — Nonfarm Payrolls
- `UNRATE` — Unemployment Rate
- `TOTALSA` — Total Vehicle Sales
- `DGORDER` — Durable Goods Orders
- `TRFVOLUSM227NFWA` — Vehicle Miles Travelled
- `GDPC1` — Real GDP
- `FEDFUNDS` — Fed Funds Rate
- `DFII10` — 10-Year TIPS (real yield)
- `PCEC96` — Real Personal Consumption Expenditure

This collapses at least 15–20 individual agency data pulls into a single pipeline source. **FRED should be the primary pipeline for all US macro indicators.**

### C2. Structured Download Sources

| Source | Format | Access | Notes |
|--------|--------|--------|-------|
| Ember electricity data | CSV / XLSX | ember-climate.org/data-catalogue/ | Updated regularly, global coverage |
| USDA WASDE tables | PDF + PSD Online CSV | usda.gov/oce/commodity/wasde | PSD Online is machine-readable |
| USDA Export Sales | CSV | apps.fas.usda.gov/export-sales/ | Weekly, machine-readable |
| Eurostat bulk download | CSV / TSV | ec.europa.eu/eurostat | Large datasets; API preferred |
| EIA Grid Monitor | CSV export | eia.gov/electricity/gridmonitor/ | Hourly US electricity demand |

### C3. Press Release / PDF Parsing Required

| Source | Format | Parsing Complexity | Consistency |
|--------|--------|--------------------|-------------|
| World Steel Association monthly production | Press release (PDF) | Low — structured table in standard format | High — monthly on schedule |
| Automaker monthly/quarterly sales | Press releases (varied formats) | Medium — each manufacturer has a different format | Medium — some report monthly, some quarterly |
| IATA traffic statistics | Press release (PDF) | Low — standard summary table | High — monthly |
| SIA semiconductor sales | Press release (web page) | Low — headline numbers in text | High — monthly |
| Cocoa grindings (regional associations) | Press release / news articles | Medium — summarised in trade press | Medium — quarterly |
| UNICA sugarcane data | Web report (Portuguese, some English) | Medium — needs translation for some fields | High — bi-weekly in season |
| PPAC India petroleum | PDF reports | Medium — PDF tables need extraction | Medium — monthly but release timing varies |
| China NBS data | Web page (in Chinese; English summaries via Xinhua) | Medium — English-language press coverage is most practical | High — monthly on schedule |

### C4. International Data — Language and Access Notes

| Source | Language | Machine-Readable | Practical Access Strategy |
|--------|----------|-------------------|--------------------------|
| China NBS | Chinese (official); English (Xinhua, state media) | Limited (web page) | Use English-language state media reporting for headline numbers; flag as "Needs Verification" for direct NBS redistribution |
| Japan METI | Japanese (official); English summaries available | Some CSV/Excel downloads | Use English publications where available |
| India PPAC/CEA | English | PDF (mostly) | PDF parsing pipeline |
| South Korea KITA | Korean; some English | Some structured data | Use English-language trade data portals |
| Brazil IBGE/UNICA | Portuguese | CSV/API (IBGE); web (UNICA) | IBGE has an API; UNICA needs parsing |

---

## D. Dashboard Layout and UX

### D1. Top-Level View — "Demand Pulse"

The entry screen answers: **"Across commodity markets right now, is demand running hot or cold?"**

**Layout (top to bottom):**

**1. Macro Context Strip (persistent banner)**
A narrow horizontal strip across the top displaying 4–5 key macro indicators that set the demand backdrop:
- US GDP growth (QoQ annualised, latest)
- US industrial production (MoM change)
- US housing starts (SAAR)
- China industrial production (YoY %)
- Directional arrows (↑ improving / ↓ deteriorating / → stable) and period of last update

This strip persists across all views within DemandWatch and updates whenever any underlying indicator is refreshed.

**2. Demand Scorecard (the main grid)**
A heat-map table:

| Vertical | Direct Consumption | YoY Change | Trend (3m) | Latest Data | Freshness |
|----------|--------------------|------------|------------|-------------|-----------|
| Crude Oil | US product supplied: 20.2 mb/d | +1.8% | ↑ | Week ending Mar 14 | 5d ago |
| Natural Gas | US consumption: 82.4 Bcf/d | -3.2% | ↓ | Week ending Mar 14 | 5d ago |
| Gasoline | US product supplied: 8.9 mb/d | +0.7% | → | Week ending Mar 14 | 5d ago |
| Electricity | US grid load: 428 GW (peak) | +2.1% | ↑ | Mar 19 (hourly) | 1d ago |
| Coal | US power consumption: 38.2M st (Q4) | -8.4% | ↓ | Q4 2025 | 12w ago |
| Copper / Industrial Metals | US IP Index: 103.8 | +1.2% | ↑ | February 2026 | 3w ago |
| Gold | ETF holdings: 878t / Real yield: 1.82% | ETF +12t / Yield -40bp YoY | ↑ | Mar 19 | 1d ago |
| Grains (Corn) | WASDE total use: 14,890 mb | +2.1% | → | March WASDE | 8d ago |

Colour coding:
- **Green** — YoY demand above +1% (or above historical seasonal norm)
- **Amber** — Roughly flat (–1% to +1%)
- **Red** — YoY demand below –1% (or below seasonal norm)
- **Grey** — Data stale (>30 days for weekly series; >90 days for monthly series)

Each row is clickable → drills into the commodity-level view.

**3. Demand Movers (below the scorecard)**
A feed of the 5–10 most recent demand data releases, sorted by recency, showing:
- Indicator name
- Reported value vs. prior period and YoY change
- Commodity vertical tag
- Tier badge (T1–T7)
- "Surprise" flag if the reading is >1 standard deviation from recent trend

This replaces a traditional news feed with a structured data release log.

---

### D2. Commodity-Level View

When a user clicks into a commodity vertical (e.g., Crude Oil), they see a dedicated demand dashboard.

**Layout:**

**Header:** Commodity name + single-sentence demand summary
> *"US petroleum demand is running 1.8% above year-ago levels, led by strong gasoline consumption."*
(Auto-generated from latest data — template-based, not AI-generated commentary.)

**Section 1: Direct Consumption (T1)**
- Primary chart: US product supplied, weekly, with YoY overlay (current year line vs prior year line vs 5-year range band)
- Secondary charts: Sub-product breakdown (gasoline, distillates, jet fuel, other)
- Data table with exact values, WoW change, YoY change

**Section 2: Throughput Proxies (T2)**
- Refinery crude inputs chart (weekly, YoY overlay)
- Refinery utilisation rate

**Section 3: End-Use Activity (T4)**
- VMT, airline passengers, trucking tonnage (where available)
- Each with YoY comparison

**Section 4: Trade Flows (T3)**
- China crude imports (monthly, bar chart)
- India petroleum consumption (monthly)
- EU inland deliveries (monthly)

**Section 5: Macro Context (T6)**
- US GDP growth, industrial production, key rates
- Relevant to this commodity's demand drivers

**Section 6: Structural Trends**
- EV adoption rate (for gasoline/diesel demand erosion)
- Fleet fuel efficiency trends
- Long-run demand forecasts from EIA STEO / IEA CC text

**Right sidebar: Data calendar**
- Next data release dates for all indicators on this page
- Links to CalendarWatch for full schedule

---

### D3. The "Demand Surprise" Feature

For indicators where a prior-period baseline exists:

- Show **actual vs. prior period** and **actual vs. year-ago** prominently
- For US petroleum data: show **actual vs. EIA STEO forecast** (public domain) as a "demand surprise" metric
- For WASDE data: show **actual vs. prior month WASDE estimate** (the monthly revision is itself a major market event)
- For macro data: show **current vs. prior period** and **current vs. year-ago** (avoid consensus estimates, which are proprietary)

Display format: A bold number with colour coding:
> **US Gasoline Demand Surprise: +340 kb/d vs year-ago** 🟢

---

### D4. Structural Trends Section (V2)

A dedicated sub-page tracking slow-moving demand shifts:

| Trend | Key Metric | Source | Direction |
|-------|-----------|--------|-----------|
| EV adoption | Global EV sales share (%) | IEA (CC), automaker PRs | ↑ Reducing gasoline/diesel demand |
| Data centre power | Estimated electricity consumption (TWh) | Ember, IEA (CC) | ↑ Increasing electricity demand |
| Coal-to-gas switching | Gas-coal price ratio ($/MMBtu equivalent) | EIA (derived) | Varies |
| Solar/wind capacity | New installations (GW) | IRENA, Ember (CC) | ↑ Increasing silver demand; changing power demand shape |
| Urbanisation | Urban population share (%) | World Bank (CC) | ↑ Long-term metals/energy demand driver |

---

## E. Coverage Gap Assessment

### E1. Gap Matrix

| Region | Energy (Oil/Gas/Power) | Metals | Grains/Softs | Gap Severity |
|--------|----------------------|--------|-------------|-------------|
| **US** | Excellent (EIA, FRED) | Good (FRED macro + automaker PRs) | Excellent (USDA) | **Low** |
| **EU** | Good (Eurostat CC BY, Ember CC BY) | Good (Eurostat industrial production) | Moderate (Eurostat trade; USDA FAS for grain) | **Low–Medium** |
| **China** | Critical gap — highest impact | Critical gap — highest impact | Moderate gap (USDA FAS GAIN helps) | **High** |
| **India** | Moderate gap (PPAC, CEA exist but need verification) | Moderate gap | Moderate gap (FAS GAIN helps) | **Medium** |
| **Japan/Korea** | Moderate gap (data exists, terms need verification) | Low priority (smaller markets) | Low priority | **Medium** |
| **LatAm** | Low gap for Brazil (IBGE API); thin elsewhere | Thin | Sugar/coffee/soy covered by USDA FAS + UNICA | **Medium** |
| **Africa/Middle East/SE Asia** | Thin (World Bank macro only) | Thin | FAS GAIN provides some coverage | **High** (but low impact for MVP) |

### E2. China — The Critical Gap

China is the single largest demand centre for crude oil imports, iron ore, copper, soybeans, LNG, and coal. Not having reliable, republishable Chinese demand data is the biggest constraint on DemandWatch.

**Best available workarounds:**
1. **USDA FAS GAIN reports** from Beijing — public domain, cover agricultural imports and some energy/industrial context. Qualitative rather than time-series data.
2. **State media reporting** (Xinhua, Global Times) — report NBS headline numbers in English. Citing a widely-reported headline number with attribution to the reporting outlet may be defensible. **Legal review recommended** before systematically displaying numbers sourced from state media summaries.
3. **Trade flow data** — China's import volumes for crude oil, iron ore, copper, soybeans, and LNG are reported by exporting countries' customs agencies (Australia, Brazil, and other exporters) and can be cross-referenced.
4. **Ember** — Covers China electricity demand (CC BY 4.0). This is a confirmed, high-quality source for one key vertical.

**Commercial data option:** A licence from a data provider covering Chinese commodity demand (e.g., CEIC, Wind Information, or a consultancy) would close this gap. Estimated cost: $5,000–$20,000/year depending on scope. **Defer to V2 unless organic revenue supports it.**

**MVP decision:** Launch with China data limited to Ember electricity data (confirmed CC BY), USDA FAS GAIN agricultural context (public domain), and a "China Demand" placeholder section noting that additional coverage is planned. This is an honest gap and users will appreciate transparency over pretending it doesn't exist.

### E3. Survey Indexes Stay Out Of Scope

DemandWatch will not roadmap proprietary survey-index products for the MVP or near-term V2. The base-metals macro view stays anchored to public-domain industrial production, housing, vehicle sales, and trade or throughput proxies that can be published cleanly and stored systematically.

---

## F. Macro Demand Context Module

### F1. Design

The Macro Dashboard is a standalone sub-module within DemandWatch, accessible from the top navigation. It provides the economic backdrop that drives all commodity demand.

**Layout:**

**Global Summary Bar:**
| Indicator | Latest | Change | Trend | Freshness |
|-----------|--------|--------|-------|-----------|
| US GDP (QoQ ann.) | +2.4% | +0.3pp | → | Q4 2025 (3rd est.) |
| US Industrial Production (MoM) | +0.2% | –0.1pp | → | Feb 2026 |
| US Manufacturing (direction) | Expanding | 4th month | ↑ | Mar 2026 |
| EU GDP (QoQ) | +0.3% | +0.1pp | ↑ | Q4 2025 |
| EU Industrial Production (MoM) | –0.1% | –0.3pp | ↓ | Jan 2026 |
| China IP (YoY) | +5.8% | +0.4pp | ↑ | Feb 2026 |

**US Detail Section (all via FRED API — public domain):**

| Category | Indicators | FRED Series |
|----------|-----------|-------------|
| Growth | Real GDP, GDI, PCE | GDPC1, GDI, PCEC96 |
| Industry | Industrial Production, Capacity Utilisation, Durable Goods | INDPRO, TCU, DGORDER |
| Labour | Nonfarm Payrolls, Unemployment Rate, Initial Claims | PAYEMS, UNRATE, ICSA |
| Consumer | Retail Sales, Vehicle Sales, Consumer Confidence (if public) | RSAFS, TOTALSA |
| Housing | Housing Starts, Building Permits, Construction Spending | HOUST, PERMIT, TTLCONS |
| Trade | Goods Trade Balance, Imports, Exports | BOPGSTB |
| Rates | Fed Funds Rate, 10Y Treasury, 10Y TIPS (real yield) | FEDFUNDS, DGS10, DFII10 |

Each indicator row shows: latest value, prior period, YoY change, directional arrow, next release date.

**Europe Section (Eurostat CC BY 4.0):**
- Industrial production, GDP, trade balance, energy consumption
- All accessible via Eurostat SDMX API

**China Section (Needs Verification — display where legally confirmed):**
- Industrial production, retail sales, fixed asset investment
- Ember electricity data (confirmed CC BY)

**Emerging Markets Section (V2):**
- India, Brazil, ASEAN — thin at launch, build as sources are verified

### F2. MVP vs Deferred

**Recommendation:** Include a *minimal* macro strip (5–6 key indicators) in the DemandWatch MVP. Defer the full Macro Dashboard sub-module to V2.

Rationale: The macro context strip is essential for interpreting commodity demand data — a user seeing gasoline demand up 2% YoY needs to know whether the economy is growing 3% (demand is lagging) or contracting (demand is surprisingly resilient). But a fully built-out macro dashboard is a major engineering effort that competes with core demand indicator coverage.

**MVP macro strip:** US GDP, US Industrial Production, US housing starts, US employment (nonfarm payrolls), and a China industrial production figure (if legally cleared). All available from FRED except China.

---

## G. Data Pipeline Architecture

### G1. Ingestion Layer

```
┌─────────────────────────────────────────────────────────┐
│                   INGESTION LAYER                        │
├──────────────────┬──────────────────┬────────────────────┤
│  API Scheduled   │  File Download   │  Press Release     │
│  Pulls           │  (Batch)         │  Monitor           │
├──────────────────┼──────────────────┼────────────────────┤
│ EIA Open Data    │ Ember CSV        │ World Steel Assoc  │
│ FRED             │ USDA WASDE PDF   │ Automaker sales    │
│ Eurostat SDMX    │ USDA PSD CSV     │ IATA traffic       │
│ BLS API          │ USDA Export CSV  │ SIA semiconductors │
│ BEA API          │ IEA CC content   │ Cocoa grindings    │
│ Census API       │ World Bank CSV   │ UNICA (Brazil)     │
│ IMF API          │                  │ PPAC (India)       │
└──────────────────┴──────────────────┴────────────────────┘
```

**Scheduling rules:**
- **Weekly sources** (EIA WPSR, USDA Export Sales): Pull within 1 hour of scheduled release time
- **Monthly sources** (FRED/industrial production, Census housing starts, BLS employment): Pull on release day, aligned to release calendar
- **Quarterly/Annual sources** (GDP, Ember annual data): Pull on release day; backfill check monthly
- **Press releases** (auto sales, steel production): Monitor RSS feeds + source web pages daily; parse within 24 hours of publication

### G2. FRED as the Backbone

```
FRED API (fred.stlouisfed.org/docs/api/)
│
├── Single API key → 120 requests/minute (sufficient for all macro indicators)
├── All US macro indicators in one pipeline
├── Observation history (full time series with revision tracking via vintage dates)
├── FRED release calendar API → feeds CalendarWatch automatically
└── Series metadata (units, seasonal adjustment, frequency) → auto-configure display
```

**Implementation note:** FRED provides "vintage dates" — the date on which a particular data value was first reported or revised. This supports the revision-handling requirement. When industrial production is revised from +0.2% to +0.3%, the pipeline stores both the original and revised values with their vintage dates.

### G3. Storage Schema

```
demand_observations
├── series_id          (e.g., "eia_us_gasoline_product_supplied")
├── commodity          (crude_oil, natural_gas, gasoline, etc.)
├── geography          (us, eu, china, india, japan, global)
├── indicator_type     (T1_direct, T2_throughput, T3_trade, T4_enduse, T5_leading, T6_macro, T7_weather)
├── frequency          (daily, weekly, monthly, quarterly, annual)
├── observation_date   (the period the data refers to)
├── release_date       (when the data was published)
├── vintage_date       (for revision tracking)
├── value              (numeric)
├── unit               (kb/d, Bcf/d, GW, index, %, kt, million_bushels, etc.)
├── yoy_change         (calculated, %)
├── yoy_change_abs     (calculated, in native units)
├── source             (eia, fred, eurostat, usda, ember, etc.)
├── source_series_id   (original series ID from source, e.g., FRED: INDPRO)
└── source_url         (direct link to source publication)
```

**Mixed-frequency handling:** Store all observations in the same table with a `frequency` column. The dashboard layer handles display logic:
- Weekly and daily data: show latest + 4-week/30-day moving average
- Monthly data: show latest + trailing 3-month trend
- Quarterly data: show latest + QoQ and YoY change
- Annual data: show in structural trends section only

**Backfill target:** Minimum 3 years (for YoY comparisons + trend calculation). Target 5–10 years for key indicators (EIA product supplied, FRED macro, USDA WASDE). Most API sources provide full historical series.

### G4. Processing Layer

```
Raw Ingestion → Validation → Transformation → Storage → Display
                    │              │
                    │              ├── YoY change calculation
                    │              ├── Seasonal norm comparison (5-year avg for that week/month)
                    │              ├── Unit normalisation
                    │              ├── Composite "demand pulse" signal (V2)
                    │              └── Cross-module event triggers (HeadlineWatch, CalendarWatch)
                    │
                    ├── Schema validation (expected columns, value ranges)
                    ├── Revision detection (compare new value to previously stored value for same period)
                    └── Staleness check (flag if source hasn't updated on expected schedule)
```

### G5. Reliability Safeguards

| Risk | Mitigation |
|------|------------|
| Source downtime | Cache last-known values; display "as of [date]" with staleness warning |
| Revision to historical data | Store all vintages; display latest vintage; show revision history on demand |
| Missing international data | "Last available" display with date; fallback to most recent available period |
| Format changes in press releases | Alert system on parse failure; manual review queue |
| API rate limits | Implement exponential backoff; batch requests; cache aggressively |
| Unit inconsistency | Enforce unit normalisation at ingestion; validate against expected units per series |

---

## H. MVP Prioritisation

### H1. MVP Commodity Verticals (Launch with 4)

| Priority | Vertical | Rationale |
|----------|----------|-----------|
| **1** | **Crude Oil + Refined Products** (combined) | Highest market interest; EIA provides world-class weekly data (public domain); most indicators are T1 direct measurement; the gasoline/distillate demand chart is the single most-watched demand indicator in commodity markets |
| **2** | **Electricity / Power** | Best global coverage (Ember CC BY + EIA); near-real-time data; growing structural relevance (data centres, EVs, electrification); genuinely differentiated — most commodity platforms don't present electricity demand well |
| **3** | **Grains & Oilseeds** | USDA provides unmatched public-domain demand data (WASDE, Export Sales, PSD Online); weekly export sales report is a major market-moving event; corn ethanol ties to energy complex |
| **4** | **Base Metals** (as macro-demand proxy dashboard) | Use FRED macro indicators (IP, housing starts, auto sales, durable goods) as the primary demand signals; extend the macro context with OECD CLI for Japan, South Korea, and India; keep World Steel Association data placeholder-only until systematic republication is clean |

**Deferred to V2:** Natural Gas (needs better HDD/CDD integration from WeatherWatch first), Coal (thinner data, lower user demand), Precious Metals, Soft Commodities, Fertilisers.

### H2. MVP Indicator Types

| Tier | In MVP? | Notes |
|------|---------|-------|
| T1 · Direct Consumption | **Yes** | Core of MVP — EIA product supplied, Ember electricity, USDA WASDE |
| T2 · Throughput Proxy | **Yes** | Refinery inputs, ethanol production, steel production (if press release terms cleared) |
| T3 · Trade Flow | **Partial** | USDA export sales (public domain, weekly, high-impact); China imports deferred until legal review |
| T4 · End-Use | **Partial** | Housing starts, auto sales via FRED; VMT as gasoline proxy; defer airline data, trucking |
| T5 · Leading | **Minimal** | Building permits (FRED); USDA Prospective Plantings (annual); defer most |
| T6 · Macro | **Strip plus selected OECD context** | Macro strip remains the primary user surface; selected OECD CLI series are live-safe backend context, with a fuller dashboard still deferred to V2 |
| T7 · Weather | **Deferred** | Requires WeatherWatch integration; link to WeatherWatch in V2 |

### H3. Minimum Viable Dashboard

The MVP DemandWatch answers one question: **"Is demand running above or below seasonal norms, and is the trend improving or deteriorating?"**

**MVP screens:**
1. **Demand Scorecard** — The heat-map table (Section D1), covering the 4 MVP verticals
2. **Demand Movers** — Feed of latest data releases with surprise flags
3. **Commodity drill-down** — For each of the 4 verticals: primary T1 demand chart with YoY overlay, supporting T2/T3 data where available, data table
4. **Macro strip** — Persistent banner with US GDP, IP, housing starts, nonfarm payrolls, and one China indicator (Ember electricity as confirmed source)

**What's NOT in MVP:**
- Full macro dashboard sub-module (V2)
- Structural trends section (V2)
- Cross-module balance view (Supply – Demand) (V3)
- Demand-on-price overlay charts (V2)
- Composite demand index / "demand pulse" score (V2)
- Demand forecast-vs-actual (V2)
- International data beyond EU/Eurostat and Ember global (V2)

### H4. Phasing

| Phase | Scope | Timeline Target |
|-------|-------|----------------|
| **MVP** | 4 verticals (Crude/Products, Electricity, Grains, Metals-as-macro); T1 + T2 indicators; US-centric + Ember global; demand scorecard + commodity drill-downs; macro strip | First release |
| **V2** | Add Natural Gas, Coal, Precious Metals; full Macro Dashboard; WeatherWatch integration (HDD/CDD); China data (pending legal review); demand-on-price overlays; structural trends | +3–4 months |
| **V3** | Supply-Demand balance view (SupplyWatch integration); composite demand indices; soft commodities; international expansion; forecast-vs-actual; HeadlineWatch auto-triggers | +6–8 months |

---

## I. Cross-Module Integration

### I1. Architecture Principle

All modules write to a shared time-series store with a common schema. The key fields that enable cross-module queries:

```
shared_observation
├── module           (demandwatch, supplywatch, inventorywatch, weatherwatch)
├── commodity        (standardised commodity codes)
├── geography        (standardised ISO codes)
├── observation_date
├── value
├── unit             (standardised units per commodity)
```

**Unit standardisation is non-negotiable from day one.** If DemandWatch stores crude oil demand in kb/d and SupplyWatch stores production in mb/d, the balance view will produce nonsense. Define a canonical unit per commodity:

| Commodity | Canonical Unit (DemandWatch) | Canonical Unit (SupplyWatch) |
|-----------|-----------------------------|-----------------------------|
| Crude oil | kb/d (thousand barrels/day) | kb/d |
| Natural gas | Bcf/d (billion cubic feet/day) | Bcf/d |
| Gasoline | kb/d | kb/d |
| Electricity | GWh (or TWh for annual) | GWh |
| Corn | million bushels (marketing year) | million bushels |
| Copper | kt (thousand tonnes) | kt |
| Gold | tonnes | tonnes |

### I2. Cross-Module Connections

**DemandWatch ↔ SupplyWatch (V3: Balance View)**
- Combine supply and demand on a common time axis to show implied surplus/deficit
- For crude oil: EIA supply (production + imports + stock draws) vs. EIA product supplied (demand)
- For grains: USDA supply (production + beginning stocks + imports) vs. USDA demand (domestic use + exports)
- The balance residual = ending stocks → cross-check with InventoryWatch
- **Data architecture requirement:** Both modules must store data at the same periodicity and geography to enable subtraction. Weekly US petroleum is the easiest starting point.

**DemandWatch ↔ InventoryWatch**
- When InventoryWatch shows a large weekly crude stock draw, DemandWatch provides the demand side: was product supplied (demand) unusually strong that week?
- Implementation: InventoryWatch "story" feature pulls the corresponding week's demand data from DemandWatch and displays it in context
- **Alignment requirement:** Same week-ending date conventions (EIA reports as "week ending Friday")

**DemandWatch ↔ WeatherWatch**
- WeatherWatch owns HDD/CDD data (single source of truth)
- DemandWatch reads HDD/CDD via internal API and displays in the gas/power demand context
- Visual: HDD/CDD chart overlaid on gas/power consumption chart, with "via WeatherWatch" badge
- **DemandWatch never stores or calculates its own HDD/CDD values**

**DemandWatch ↔ HeadlineWatch**
- Auto-trigger rule: Any T1 or T2 demand indicator that deviates >1.5σ from its trailing 52-week YoY average generates a HeadlineWatch item
- Template: "[Commodity] demand [above/below] seasonal norms: [Indicator] at [value], [YoY change] vs year-ago"
- Also trigger on: WASDE demand revisions > 50 million bushels (corn/soybeans), EIA product supplied > 500 kb/d above/below year-ago
- **Implementation:** DemandWatch processing layer emits events to a shared event bus; HeadlineWatch consumes and renders

**DemandWatch ↔ CalendarWatch**
- All demand data release dates feed into CalendarWatch
- Tags: Each release tagged with commodity vertical(s) affected
- Source: EIA release schedule (published), USDA release calendar (published), FRED release calendar API, BLS/Census/BEA release schedules (published)
- **Volume note:** There are demand-relevant data releases almost every business day. CalendarWatch needs filtering — user should be able to filter by commodity vertical to avoid calendar overload.

**DemandWatch ↔ PriceWatch (V2)**
- Overlay demand indicators on price charts
- Key combinations: US gasoline product supplied vs. RBOB gasoline futures; China copper imports vs. LME copper; WASDE corn demand vs. CBOT corn
- **Architecture requirement:** PriceWatch and DemandWatch must share the same time axis and be able to render dual-axis charts (demand indicator on left axis, price on right axis)

---

## Appendix: Key Remaining Legal / Operational Questions

Before launch, the following need formal verification or legal counsel review:

| # | Question | Impact | Priority |
|---|----------|--------|----------|
| 1 | Can NBS (China) headline data points, as reported in English-language state media, be displayed with attribution? | Unlocks China demand data | **High** |
| 2 | Are World Steel Association press release data points (monthly crude steel production) freely republishable? | Important T2 indicator for metals | **Medium** |
| 3 | Can IATA press release headline numbers (RPK, FTK) be displayed with attribution? | Jet fuel demand proxy | **Medium** |
| 4 | What are JODI's redistribution terms? | Could fill demand data gaps for non-US countries | **Medium** |
| 5 | Is ENTSO-E Transparency Platform data freely republishable? | EU real-time electricity demand | **Medium** |
| 6 | Can Japan e-Stat LNG imports be verified end to end with a stable query contract and app-ID-backed automation? | Unlocks Japan LNG trade-flow coverage | **Medium** |
| 7 | Can Korea Customs item-trade LNG imports be verified end to end against the live XML payload contract? | Unlocks South Korea LNG trade-flow coverage | **Medium** |
| 8 | Can Department of Fertilizers monthly summary PDFs be parsed reliably enough for automated ingestion without manual QA? | India fertiliser demand coverage | **Low** (V2) |

---

## Appendix: FRED Series Reference (Complete DemandWatch Set)

For pipeline convenience, a consolidated list of all FRED series IDs relevant to DemandWatch:

**Output & Industry:**
`INDPRO`, `IPMAN`, `IPMANSICS`, `TCU`, `MCUMFN`, `DGORDER`, `NEWORDER`

**Housing & Construction:**
`HOUST`, `HOUST1F`, `PERMIT`, `TTLCONS`, `TLRESCONS`, `TLNRESCONS`

**Consumer & Retail:**
`RSAFS`, `TOTALSA`, `ALTSALES`, `UMCSENT` (Michigan sentiment — verify terms)

**Labour:**
`PAYEMS`, `UNRATE`, `ICSA`, `CCSA`, `CES0500000003` (avg hourly earnings)

**GDP & Growth:**
`GDPC1`, `A191RL1Q225SBEA` (real GDP % change), `GDI`, `PCEC96`

**Transport & Demand Proxies:**
`TRFVOLUSM227NFWA` (VMT)

**Rates & Financial:**
`FEDFUNDS`, `DGS10`, `DGS2`, `DFII10` (TIPS/real yield), `T10YFF` (term spread)

**Trade:**
`BOPGSTB` (goods trade balance)

**Prices (contextual):**
`DCOILWTICO` (WTI), `DCOILBRENTEU` (Brent), `GASREGW` (retail gasoline)
