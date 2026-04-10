import test from "node:test";
import assert from "node:assert/strict";

import { buildDemandWatchPageModel, mapDemandScorecardItems, mapDemandVerticalDetail } from "../data.js";

test("mapDemandVerticalDetail appends deferred indicators as placeholders", () => {
  const detail = {
    id: "base-metals",
    code: "base_metals",
    label: "Base Metals",
    nav_label: "Metals",
    short_label: "Base Metals",
    sector: "metals",
    summary: "Macro releases remain the safe demand anchor.",
    scorecard: {
      id: "base-metals",
      code: "base_metals",
      label: "Base Metals",
      nav_label: "Metals",
      short_label: "Base Metals",
      sector: "metals",
      scorecard_label: "Industrial production",
      display_value: "103.8",
      yoy_label: "+1.2% YoY",
      trend: "up",
      latest_period_label: "Mar 2026",
      freshness: "3w ago",
      freshness_state: "fresh",
      stale: false,
      source_url: "https://example.com/fred",
      primary_series_code: "FRED_US_INDUSTRIAL_PRODUCTION",
    },
    facts: [
      { label: "Primary cadence", value: "Monthly", note: "Federal Reserve G.17" },
    ],
    sections: [
      {
        id: "macro",
        title: "Macro Backbone",
        description: "Public-domain industrial releases carry the page.",
        indicators: [
          {
            series_id: "live-series",
            code: "FRED_US_INDUSTRIAL_PRODUCTION",
            title: "Industrial production",
            tier: "t6_macro",
            tier_label: "T6 · Macro",
            source_label: "Federal Reserve",
            display_value: "103.8",
            change_label: "+1.2% YoY",
            detail: "Latest revision preserved from FRED vintages.",
            trend: "up",
            sparkline: [101.1, 102.2, 103.8],
            freshness: "3w ago",
            freshness_state: "fresh",
            latest_period_label: "Mar 2026",
            source_url: "https://example.com/fred",
            coverage_status: "live",
          },
        ],
        table_rows: [
          {
            series_id: "live-series",
            code: "FRED_US_INDUSTRIAL_PRODUCTION",
            label: "Industrial production",
            source_label: "Federal Reserve",
            latest_display: "103.8",
            change_display: "+0.2",
            yoy_display: "+1.2% YoY",
            freshness: "3w ago",
            freshness_state: "fresh",
            trend: "up",
            source_url: "https://example.com/fred",
          },
        ],
      },
    ],
    calendar: [],
    notes: ["China metals demand remains deferred pending republication review."],
  };

  const coverageNotes = {
    verticals: [
      {
        id: "base-metals",
        code: "base_metals",
        name: "Base Metals",
        commodity_code: "base_metals",
        sector: "metals",
        counts: {
          live: 1,
          partial: 0,
          deferred: 1,
          blocked: 0,
        },
        live: [],
        partial: [],
        deferred: [
          {
            series_id: "china-demand",
            code: "CHINA_METALS_DEMAND",
            name: "China metals demand",
            tier: "t3_trade",
            coverage_status: "needs_verification",
            source_name: "China Customs",
            source_slug: "china_customs",
            freshness_state: "stale",
            reasons: ["Direct republication terms remain unresolved."],
          },
        ],
        blocked: [],
      },
    ],
  };

  const mapped = mapDemandVerticalDetail(detail, coverageNotes);
  const gapSection = mapped.sections.at(-1);

  assert.equal(gapSection.id, "coverage-gaps");
  assert.equal(gapSection.isCoverageGap, true);
  assert.deepEqual(
    gapSection.indicators.map((indicator) => ({
      title: indicator.title,
      value: indicator.value,
      placeholder: indicator.placeholder,
    })),
    [
      {
        title: "China metals demand",
        value: "Needs verification",
        placeholder: true,
      },
    ]
  );
  assert.equal(gapSection.tableRows[0].latest, "Needs verification");
  assert.equal(mapped.coverageLabel, "1 live / 1 deferred");
});

test("buildDemandWatchPageModel orders scorecard items by launch vertical metadata", () => {
  const pageModel = buildDemandWatchPageModel({
    macroStrip: {
      items: [],
    },
    scorecard: {
      items: [
        {
          id: "grains",
          code: "grains_oilseeds",
          label: "Grains & Oilseeds",
          nav_label: "Grains",
          short_label: "Grains",
          sector: "agriculture",
          scorecard_label: "USDA total use",
          display_value: "14,890 mbu",
          yoy_label: "+2.1% YoY",
          trend: "flat",
          latest_period_label: "Mar 2026",
          freshness: "8d ago",
          freshness_state: "fresh",
          stale: false,
          primary_series_code: "USDA_US_CORN_TOTAL_USE_WASDE",
        },
        {
          id: "crude-products",
          code: "crude_products",
          label: "Crude Oil + Refined Products",
          nav_label: "Crude",
          short_label: "Crude + Products",
          sector: "energy",
          scorecard_label: "US product supplied",
          display_value: "9.6 mb/d",
          yoy_label: "+6.7% YoY",
          trend: "up",
          latest_period_label: "Week ending 27 Mar 2026",
          freshness: "5d ago",
          freshness_state: "fresh",
          stale: false,
          primary_series_code: "EIA_US_TOTAL_PRODUCT_SUPPLIED",
        },
      ],
    },
    movers: {
      items: [],
    },
    coverageNotes: {
      summary: {
        vertical_count: 2,
        series_count: 6,
        status_counts: {
          live: 5,
          partial: 0,
          deferred: 1,
          blocked: 0,
        },
      },
      verticals: [],
    },
    verticalDetails: [],
  });

  assert.deepEqual(pageModel.scorecard.map((item) => item.id), ["crude-products", "grains"]);
  assert.equal(pageModel.scorecard[0].accent, "var(--color-energy)");
  assert.equal(pageModel.hero, undefined);
});

test("mapDemandScorecardItems supplies fallback YoY labels when the backend omits them", () => {
  const items = mapDemandScorecardItems([
    {
      id: "electricity",
      code: "electricity",
      label: "Electricity / Power",
      nav_label: "Power",
      short_label: "Electricity",
      sector: "energy",
      scorecard_label: "US grid load",
      display_value: "428 GW",
      yoy_label: null,
      trend: "up",
      latest_period_label: "04 Apr 2026",
      freshness: "1d ago",
      freshness_state: "fresh",
      stale: false,
      primary_series_code: "EIA_US_ELECTRICITY_GRID_LOAD",
    },
  ]);

  assert.equal(items[0].scorecard.yoyLabel, "YoY unavailable");
  assert.equal(items[0].accent, "var(--color-natural-gas)");
});

test("mapDemandVerticalDetail keeps release source labels and strips estimated timing copy", () => {
  const mapped = mapDemandVerticalDetail(
    {
      id: "grains",
      code: "grains_oilseeds",
      label: "Grains & Oilseeds",
      nav_label: "Grains",
      short_label: "Grains",
      sector: "agriculture",
      summary: "USDA demand signals remain current.",
      scorecard: {
        id: "grains",
        code: "grains_oilseeds",
        label: "Grains & Oilseeds",
        nav_label: "Grains",
        short_label: "Grains",
        sector: "agriculture",
        scorecard_label: "Corn total use",
        source_label: "USDA PSD",
        display_value: "14,890 mbu",
        yoy_label: "+2.1% YoY",
        trend: "flat",
        latest_period_label: "Mar 2026",
        freshness: "8d ago",
        freshness_state: "fresh",
        stale: false,
        primary_series_code: "USDA_US_CORN_TOTAL_USE_WASDE",
      },
      facts: [],
      sections: [],
      calendar: [
        {
          release_name: "USDA WASDE Monthly Report",
          source_name: "USDA OCE",
          cadence: "monthly",
          scheduled_for: "2026-04-09T16:00:00Z",
          is_estimated: true,
          source_url: "https://example.com/wasde",
          notes: [
            "Next release time is estimated from cadence or latest observed release.",
            "Calendar-driven release; confirm against CalendarWatch before publication.",
          ],
        },
      ],
      notes: [],
    },
    { verticals: [] }
  );

  assert.equal(mapped.calendar[0].value, "Thu 09 Apr 16:00 UTC");
  assert.equal(mapped.calendar[0].sourceLabel, "USDA OCE");
  assert.equal(mapped.calendar[0].note, "Monthly");
});
