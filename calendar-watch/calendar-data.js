import { sortEvents } from "./calendar-utils.js";

const RECURRING_SERIES = [
  {
    idPrefix: "usda-export-sales",
    name: "USDA Export Sales Report",
    organiser: "USDA Foreign Agricultural Service",
    cadence: "weekly",
    commodity_sectors: ["agriculture"],
    eventTimeUtc: "12:30:00Z",
    dates: ["2026-03-12", "2026-03-19", "2026-03-26", "2026-04-02", "2026-04-09", "2026-04-16", "2026-04-23"],
    calendar_url: "https://apps.fas.usda.gov/export-sales/esrd1.html",
    source_label: "USDA FAS",
    notes:
      "Weekly export sales release covering U.S. grains, oilseeds, cotton, livestock and related shipment pace.",
    is_confirmed: true,
  },
  {
    idPrefix: "eia-natural-gas-storage",
    name: "EIA Weekly Natural Gas Storage Report",
    organiser: "US Energy Information Administration",
    cadence: "weekly",
    commodity_sectors: ["energy"],
    eventTimeUtc: "14:30:00Z",
    dates: ["2026-03-12", "2026-03-19", "2026-03-26", "2026-04-02", "2026-04-09", "2026-04-16", "2026-04-23"],
    calendar_url: "https://ir.eia.gov/ngs/schedule.html",
    source_label: "EIA",
    notes:
      "Weekly working gas in underground storage release. Published on Thursdays at 10:30 a.m. Eastern in the official EIA schedule.",
    is_confirmed: true,
  },
  {
    idPrefix: "eia-petroleum-status",
    name: "EIA Weekly Petroleum Status Report",
    organiser: "US Energy Information Administration",
    cadence: "weekly",
    commodity_sectors: ["energy"],
    eventTimeUtc: "14:30:00Z",
    dates: ["2026-03-18", "2026-03-25", "2026-04-01", "2026-04-08", "2026-04-15", "2026-04-22"],
    calendar_url: "https://www.eia.gov/petroleum/supply/weekly/schedule.php",
    source_label: "EIA",
    notes:
      "Weekly U.S. crude, gasoline, distillate and refinery update. EIA publishes the report at 10:30 a.m. Eastern on Wednesdays.",
    is_confirmed: true,
  },
  {
    idPrefix: "baker-hughes-rig-count",
    name: "Baker Hughes North America Rig Count",
    organiser: "Baker Hughes",
    cadence: "weekly",
    commodity_sectors: ["energy"],
    eventTimeUtc: "17:00:00Z",
    dates: ["2026-03-13", "2026-03-20", "2026-03-27", "2026-04-03", "2026-04-10", "2026-04-17"],
    calendar_url: "https://rigcount.bakerhughes.com/",
    source_label: "Baker Hughes",
    notes:
      "North America rotary rig count release. Baker Hughes publishes the series each Friday at noon U.S. Central time.",
    is_confirmed: true,
  },
  {
    idPrefix: "cftc-cot",
    name: "CFTC Commitments of Traders Report",
    organiser: "US Commodity Futures Trading Commission",
    cadence: "weekly",
    commodity_sectors: ["cross-commodity"],
    eventTimeUtc: "19:30:00Z",
    dates: ["2026-03-13", "2026-03-20", "2026-03-27", "2026-04-03", "2026-04-10", "2026-04-17"],
    calendar_url: "https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm",
    source_label: "CFTC",
    notes:
      "Friday positioning update across energy, metals, grains, softs, rates and currencies. Official release schedule posts 3:30 p.m. Eastern.",
    is_confirmed: true,
  },
  {
    idPrefix: "usda-crop-progress",
    name: "USDA Crop Progress Report",
    organiser: "USDA National Agricultural Statistics Service",
    cadence: "weekly",
    commodity_sectors: ["agriculture"],
    eventTimeUtc: "20:00:00Z",
    dates: ["2026-04-06", "2026-04-13", "2026-04-20"],
    calendar_url: "https://www.nass.usda.gov/Publications/Calendar/reports_by_date.php?month=04&year=2026",
    source_label: "USDA NASS",
    notes:
      "Weekly planting, condition and development survey for major U.S. crops. The April release calendar places Crop Progress at 4:00 p.m. Eastern.",
    is_confirmed: true,
  },
];

const ONE_OFF_EVENTS = [
  {
    id: "iea-oil-market-report-2026-03-12",
    name: "IEA Oil Market Report",
    organiser: "International Energy Agency",
    cadence: "monthly",
    commodity_sectors: ["energy"],
    event_date: "2026-03-12T09:00:00Z",
    calendar_url: "https://www.iea.org/events/oil-market-report-march-2026",
    source_label: "IEA",
    notes:
      "The March 2026 Oil Market Report is scheduled for 10:00 a.m. Paris time, which is 09:00 UTC before the European summer time switch.",
    is_confirmed: true,
  },
  {
    id: "alcoa-jp-morgan-industrials-conference-2026-03-17",
    name: "Alcoa at J.P. Morgan Industrials Conference",
    organiser: "Alcoa",
    cadence: "ad_hoc",
    commodity_sectors: ["metals"],
    event_date: "2026-03-17T13:30:00Z",
    calendar_url: "https://investors.alcoa.com/events-and-presentations/events-calendar/default.aspx",
    source_label: "Alcoa IR",
    notes:
      "Investor presentation scheduled for 9:30 a.m. Eastern. Useful as a mid-quarter read-through for alumina and aluminum commentary.",
    is_confirmed: true,
  },
  {
    id: "fed-fomc-decision-2026-03-18",
    name: "Federal Reserve FOMC Decision",
    organiser: "Board of Governors of the Federal Reserve System",
    cadence: "ad_hoc",
    commodity_sectors: ["macro"],
    event_date: "2026-03-18T18:00:00Z",
    calendar_url: "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
    source_label: "Federal Reserve",
    notes:
      "The 2026 FOMC calendar confirms the March 17-18 meeting. Time is set to the standard 2:00 p.m. Eastern statement release for planning.",
    is_confirmed: true,
  },
  {
    id: "boe-mpc-decision-2026-03-19",
    name: "Bank of England MPC Decision",
    organiser: "Bank of England Monetary Policy Committee",
    cadence: "ad_hoc",
    commodity_sectors: ["macro"],
    event_date: "2026-03-19T12:00:00Z",
    calendar_url: "https://www.bankofengland.co.uk/monetary-policy-summary-and-minutes/2026/march-2026",
    source_label: "Bank of England",
    notes: "Published at 12:00 p.m. UK time. March falls before the UK daylight-saving switch, so the release lands at 12:00 UTC.",
    is_confirmed: true,
  },
  {
    id: "ecb-monetary-policy-decision-2026-03-19",
    name: "ECB Monetary Policy Decision",
    organiser: "European Central Bank Governing Council",
    cadence: "ad_hoc",
    commodity_sectors: ["macro"],
    event_date: "2026-03-19T13:15:00Z",
    calendar_url: "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html",
    source_label: "ECB",
    notes:
      "The Governing Council calendar confirms the 18-19 March meeting. Time is set to the standard 14:15 CET decision release, equivalent to 13:15 UTC.",
    is_confirmed: true,
  },
  {
    id: "ons-uk-cpi-february-2026",
    name: "ONS UK CPI Release",
    organiser: "Office for National Statistics",
    cadence: "monthly",
    commodity_sectors: ["macro"],
    event_date: "2026-03-25T07:00:00Z",
    calendar_url: "https://www.ons.gov.uk/releases/consumerpriceinflationukfebruary2026",
    source_label: "ONS",
    notes: "Consumer price inflation release for February 2026. ONS lists the publication at 7:00 a.m. UK time, which is 07:00 UTC in March.",
    is_confirmed: true,
  },
  {
    id: "usda-grain-stocks-2026-03-31",
    name: "USDA Grain Stocks",
    organiser: "USDA National Agricultural Statistics Service",
    cadence: "quarterly",
    commodity_sectors: ["agriculture"],
    event_date: "2026-03-31T16:00:00Z",
    calendar_url: "https://www.nass.usda.gov/Publications/Calendar/reports_by_date.php?month=03&year=2026",
    source_label: "USDA NASS",
    notes:
      "The March 2026 NASS release calendar schedules Grain Stocks for 12:00 p.m. Eastern, which converts to 16:00 UTC after the U.S. daylight-saving shift.",
    is_confirmed: true,
  },
  {
    id: "usda-prospective-plantings-2026-03-31",
    name: "USDA Prospective Plantings",
    organiser: "USDA National Agricultural Statistics Service",
    cadence: "annual",
    commodity_sectors: ["agriculture"],
    event_date: "2026-03-31T16:00:00Z",
    calendar_url: "https://www.nass.usda.gov/Publications/Calendar/reports_by_date.php?month=03&year=2026",
    source_label: "USDA NASS",
    notes: "Annual U.S. acreage intentions report. NASS schedules the report for 12:00 p.m. Eastern on 31 March 2026.",
    is_confirmed: true,
  },
  {
    id: "fao-food-price-index-2026-04-03",
    name: "FAO Food Price Index Release",
    organiser: "Food and Agriculture Organization of the United Nations",
    cadence: "monthly",
    commodity_sectors: ["agriculture"],
    event_date: "2026-04-03T08:00:00Z",
    calendar_url: "https://www.fao.org/worldfoodsituation/foodpricesindex/en/",
    source_label: "FAO",
    notes:
      "FAO confirms the 3 April 2026 release date. Time is set as an 08:00 UTC planning placeholder because the site publishes the date but not a fixed publication hour.",
    is_confirmed: false,
  },
  {
    id: "eia-steo-2026-04-07",
    name: "EIA Short-Term Energy Outlook",
    organiser: "US Energy Information Administration",
    cadence: "monthly",
    commodity_sectors: ["energy"],
    event_date: "2026-04-07T16:00:00Z",
    calendar_url: "https://www.eia.gov/outlooks/steo/",
    source_label: "EIA",
    notes:
      "EIA lists 7 April 2026 as the next STEO release date. The 16:00 UTC timestamp is a planning placeholder based on typical midday U.S. publication cadence.",
    is_confirmed: false,
  },
  {
    id: "usda-wasde-2026-04-09",
    name: "USDA WASDE Monthly Report",
    organiser: "USDA Office of the Chief Economist",
    cadence: "monthly",
    commodity_sectors: ["agriculture"],
    event_date: "2026-04-09T16:00:00Z",
    calendar_url:
      "https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report",
    source_label: "USDA OCE",
    notes: "USDA posts the April 2026 WASDE on 9 April at 12:00 p.m. Eastern, or 16:00 UTC.",
    is_confirmed: true,
  },
  {
    id: "bls-us-cpi-2026-04-10",
    name: "BLS US CPI Release",
    organiser: "US Bureau of Labor Statistics",
    cadence: "monthly",
    commodity_sectors: ["macro"],
    event_date: "2026-04-10T12:30:00Z",
    calendar_url: "https://www.bls.gov/schedule/news_release/cpi.htm",
    source_label: "BLS",
    notes: "The CPI release calendar shows 10 April 2026. BLS releases CPI at 8:30 a.m. Eastern, equivalent to 12:30 UTC in April.",
    is_confirmed: true,
  },
  {
    id: "iea-oil-market-report-2026-04-14",
    name: "IEA Oil Market Report",
    organiser: "International Energy Agency",
    cadence: "monthly",
    commodity_sectors: ["energy"],
    event_date: "2026-04-14T08:00:00Z",
    calendar_url: "https://www.iea.org/data-and-statistics/data-product/oil-market-report-omr",
    source_label: "IEA",
    notes:
      "The IEA schedule lists the April 2026 Oil Market Report for 14 April. The timestamp reflects the standard 10:00 a.m. Paris publication window, or 08:00 UTC in April.",
    is_confirmed: true,
  },
  {
    id: "bhp-operational-review-2026-04-21",
    name: "BHP Operational Review",
    organiser: "BHP",
    cadence: "quarterly",
    commodity_sectors: ["metals"],
    event_date: "2026-04-21T22:30:00Z",
    calendar_url: "https://www.bhp.com/investors/financial-calendar",
    source_label: "BHP",
    notes:
      "BHP's financial calendar points to the 22 April 2026 Operational Review. The UTC timestamp reflects an 08:30 Melbourne placeholder and falls late on 21 April in UTC.",
    is_confirmed: false,
  },
  {
    id: "ons-uk-cpi-march-2026",
    name: "ONS UK CPI Release",
    organiser: "Office for National Statistics",
    cadence: "monthly",
    commodity_sectors: ["macro"],
    event_date: "2026-04-22T06:00:00Z",
    calendar_url: "https://www.ons.gov.uk/releases/consumerpriceinflationukmarch2026",
    source_label: "ONS",
    notes:
      "Consumer price inflation release for March 2026. ONS lists the publication at 7:00 a.m. UK time, which is 06:00 UTC after the UK moves to British Summer Time.",
    is_confirmed: true,
  },
  {
    id: "teck-q1-conference-call-2026-04-23",
    name: "Teck Q1 2026 Conference Call",
    organiser: "Teck Resources",
    cadence: "quarterly",
    commodity_sectors: ["metals"],
    event_date: "2026-04-23T15:00:00Z",
    calendar_url: "https://www.teck.com/news/events/q1-2026-conference-call",
    source_label: "Teck",
    notes: "Teck schedules the Q1 2026 conference call for 11:00 a.m. Eastern on 23 April 2026.",
    is_confirmed: true,
  },
];

function expandRecurringSeries(series) {
  return series.dates.map((date) => ({
    id: `${series.idPrefix}-${date.replaceAll("-", "")}`,
    name: series.name,
    organiser: series.organiser,
    cadence: series.cadence,
    commodity_sectors: [...series.commodity_sectors],
    event_date: `${date}T${series.eventTimeUtc}`,
    calendar_url: series.calendar_url,
    source_label: series.source_label,
    notes: series.notes,
    is_confirmed: series.is_confirmed,
  }));
}

export const MOCK_EVENTS = sortEvents([...RECURRING_SERIES.flatMap(expandRecurringSeries), ...ONE_OFF_EVENTS]);

function normalizeEventTimestamp(value) {
  const timestamp = Date.parse(String(value || ""));
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null;
}

function toApiDate(value) {
  if (!value) {
    return null;
  }

  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return null;
  }

  return date.toISOString().slice(0, 10);
}

export async function loadCalendarEvents({ from, to, sectors } = {}) {
  const params = new URLSearchParams();

  const fromDate = toApiDate(from);
  const toDate = toApiDate(to);

  if (fromDate) {
    params.set("from", fromDate);
  }
  if (toDate) {
    params.set("to", toDate);
  }
  if (Array.isArray(sectors) && sectors.length) {
    params.set("sectors", sectors.join(","));
  }

  const query = params.toString();
  const response = await fetch(query ? `/api/calendar?${query}` : "/api/calendar");
  if (!response.ok) {
    throw new Error(`Calendar API request failed with ${response.status}`);
  }

  const payload = await response.json();
  if (!Array.isArray(payload?.data)) {
    throw new Error("Calendar API returned an unexpected payload");
  }

  return payload.data;
}

export function findCalendarEventForDemandRelease(release, events = MOCK_EVENTS) {
  const releaseName = String(release?.releaseName || release?.release_name || release?.label || "").trim();
  const scheduledFor = normalizeEventTimestamp(release?.scheduledFor || release?.scheduled_for || release?.event_date);
  if (!releaseName || !scheduledFor) {
    return null;
  }

  return (
    events.find((event) => {
      return event?.name === releaseName && normalizeEventTimestamp(event?.event_date) === scheduledFor;
    }) || null
  );
}
