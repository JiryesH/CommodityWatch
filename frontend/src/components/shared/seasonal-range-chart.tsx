"use client";

import type { Data, Layout, Shape } from "plotly.js";

import { PlotlyChart } from "@/lib/charts/plotly-chart";
import { useChartHeight } from "@/lib/charts/use-chart-height";
import { useChartTheme } from "@/lib/charts/use-chart-theme";
import { formatUtcTimestamp } from "@/lib/format/dates";
import { formatValue } from "@/lib/format/numbers";
import type { SeasonalRangePoint } from "@/types/api";
import type { SeasonalSeriesPoint } from "@/features/inventory/selectors";

interface ChartAnnotation {
  periodIndex: number;
  label: string;
}

interface SeasonalRangeChartProps {
  seasonalRange: SeasonalRangePoint[];
  currentYear: SeasonalSeriesPoint[];
  priorYear?: SeasonalSeriesPoint[];
  periodMode: "week" | "month" | "day";
  unit: string;
  annotations?: ChartAnnotation[];
}

function tickConfig(periodMode: SeasonalRangeChartProps["periodMode"]) {
  if (periodMode === "month") {
    return {
      tickvals: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
      ticktext: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
      title: "",
    };
  }

  if (periodMode === "day") {
    return {
      tickvals: [15, 46, 74, 105, 135, 166, 196, 227, 258, 288, 319, 349],
      ticktext: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
      title: "",
    };
  }

  return { title: "", tickmode: "linear" as const, dtick: 4 };
}

export function SeasonalRangeChart({
  seasonalRange,
  currentYear,
  priorYear = [],
  periodMode,
  unit,
  annotations = [],
}: SeasonalRangeChartProps) {
  const height = useChartHeight();
  const chartTheme = useChartTheme();

  const priorYearByIndex = new Map(priorYear.map((point) => [point.periodIndex, point.value]));
  const seasonalByIndex = new Map(seasonalRange.map((point) => [point.periodIndex, point]));

  const currentYearCustomData = currentYear.map((point) => {
    const seasonal = seasonalByIndex.get(point.periodIndex) ?? null;
    const priorValue = priorYearByIndex.get(point.periodIndex) ?? null;
    const median = seasonal?.p50 ?? null;
    const vsMedian = median != null ? point.value - median : null;
    const bracket =
      seasonal == null
        ? "Unavailable"
        : seasonal.p10 != null && point.value < seasonal.p10
          ? "Below 10th"
          : seasonal.p25 != null && point.value < seasonal.p25
            ? "10th-25th"
            : seasonal.p75 != null && point.value <= seasonal.p75
              ? "25th-75th"
              : seasonal.p90 != null && point.value <= seasonal.p90
                ? "75th-90th"
                : "Above 90th";

    return [
      point.label,
      formatValue(point.value, unit),
      priorValue == null ? "N/A" : formatValue(priorValue, unit),
      median == null ? "N/A" : formatValue(median, unit),
      vsMedian == null ? "N/A" : formatValue(vsMedian, unit),
      bracket,
      formatUtcTimestamp(point.releaseDate),
    ];
  });

  const traces: Data[] = [
    {
      x: seasonalRange.map((point) => point.periodIndex),
      y: seasonalRange.map((point) => point.p90 ?? point.p75 ?? point.p50 ?? null),
      type: "scatter",
      mode: "lines",
      line: { color: "rgba(0,0,0,0)", width: 0 },
      hoverinfo: "skip",
      showlegend: false,
    },
    {
      x: seasonalRange.map((point) => point.periodIndex),
      y: seasonalRange.map((point) => point.p10 ?? point.p25 ?? point.p50 ?? null),
      type: "scatter",
      mode: "lines",
      fill: "tonexty",
      fillcolor: chartTheme.band1090,
      line: { color: "rgba(0,0,0,0)", width: 0 },
      hoverinfo: "skip",
      showlegend: false,
    },
    {
      x: seasonalRange.map((point) => point.periodIndex),
      y: seasonalRange.map((point) => point.p75 ?? point.p50 ?? null),
      type: "scatter",
      mode: "lines",
      line: { color: "rgba(0,0,0,0)", width: 0 },
      hoverinfo: "skip",
      showlegend: false,
    },
    {
      x: seasonalRange.map((point) => point.periodIndex),
      y: seasonalRange.map((point) => point.p25 ?? point.p50 ?? null),
      type: "scatter",
      mode: "lines",
      fill: "tonexty",
      fillcolor: chartTheme.band2575,
      line: { color: "rgba(0,0,0,0)", width: 0 },
      hoverinfo: "skip",
      showlegend: false,
    },
    {
      x: seasonalRange.map((point) => point.periodIndex),
      y: seasonalRange.map((point) => point.p50),
      type: "scatter",
      mode: "lines",
      line: { color: chartTheme.median, width: 1.25, dash: "dash" },
      name: "Median",
      showlegend: false,
      hoverinfo: "skip",
    },
    {
      x: priorYear.map((point) => point.periodIndex),
      y: priorYear.map((point) => point.value),
      type: "scatter",
      mode: "lines",
      line: { color: chartTheme.priorYear, width: 1.5, dash: "dash" },
      name: "Prior year",
      showlegend: false,
      hoverinfo: "skip",
    },
    {
      x: currentYear.map((point) => point.periodIndex),
      y: currentYear.map((point) => point.value),
      customdata: currentYearCustomData,
      type: "scatter",
      mode: "lines",
      line: { color: chartTheme.currentYear, width: 3 },
      name: "Current year",
      showlegend: false,
      hovertemplate:
        "<b>%{customdata[0]}</b><br>" +
        "Current: %{customdata[1]}<br>" +
        "Prior year: %{customdata[2]}<br>" +
        "Median: %{customdata[3]}<br>" +
        "vs median: %{customdata[4]}<br>" +
        "Bracket: %{customdata[5]}<br>" +
        "Release: %{customdata[6]}<extra></extra>",
    },
  ];

  const shapes = annotations.map((annotation) => ({
    type: "line",
    x0: annotation.periodIndex,
    x1: annotation.periodIndex,
    yref: "paper",
    y0: 0,
    y1: 1,
    line: {
      color: chartTheme.annotation,
      width: 1,
      dash: "dot",
    },
  })) as Shape[];

  const layout: Partial<Layout> = {
    height,
    margin: { l: 56, r: 18, t: 16, b: 42 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    xaxis: {
      ...tickConfig(periodMode),
      color: chartTheme.axis,
      showgrid: false,
      zeroline: false,
      fixedrange: true,
    },
    yaxis: {
      title: { text: unit.toUpperCase() },
      color: chartTheme.axis,
      gridcolor: chartTheme.grid,
      zeroline: false,
      fixedrange: true,
    },
    hoverlabel: {
      bgcolor: chartTheme.tooltipBg,
      bordercolor: chartTheme.tooltipBorder,
      font: {
        color: chartTheme.tooltipText,
        family: "var(--font-mono)",
        size: 12,
      },
    },
    shapes,
  };

  return (
    <PlotlyChart
      config={{ displayModeBar: false, responsive: true, scrollZoom: false, staticPlot: false }}
      data={traces}
      layout={layout}
      style={{ width: "100%", height }}
      useResizeHandler
    />
  );
}
