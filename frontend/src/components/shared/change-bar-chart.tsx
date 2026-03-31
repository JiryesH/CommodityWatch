"use client";

import type { Data, Layout } from "plotly.js";

import { PlotlyChart } from "@/lib/charts/plotly-chart";
import { useChartTheme } from "@/lib/charts/use-chart-theme";
import type { ChangeBarPoint } from "@/features/inventory/selectors";

interface ChangeBarChartProps {
  series: ChangeBarPoint[];
  semanticMode: "inventory" | "generic";
  seasonalAverage?: { label: string; value: number }[];
}

export function ChangeBarChart({ series, semanticMode, seasonalAverage }: ChangeBarChartProps) {
  const chartTheme = useChartTheme();

  const markers = series.map((point) => {
    if (point.value === 0) {
      return chartTheme.axis;
    }

    const positiveSignal = semanticMode === "inventory" ? point.value < 0 : point.value > 0;
    return positiveSignal ? chartTheme.positive : chartTheme.negative;
  });

  const traces: Data[] = [
    {
      x: series.map((point) => point.label),
      y: series.map((point) => point.value),
      type: "bar",
      marker: { color: markers },
      hovertemplate: "%{x}<br>%{y:.2f}<extra></extra>",
      showlegend: false,
    },
  ];

  if (seasonalAverage?.length) {
    traces.push({
      x: seasonalAverage.map((point) => point.label),
      y: seasonalAverage.map((point) => point.value),
      type: "scatter",
      mode: "lines",
      line: { color: chartTheme.axis, width: 1.25, dash: "dash" },
      hoverinfo: "skip",
      showlegend: false,
    });
  }

  const layout: Partial<Layout> = {
    height: 220,
    margin: { l: 56, r: 18, t: 18, b: 38 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    bargap: 0.18,
    xaxis: {
      color: chartTheme.axis,
      showgrid: false,
      tickangle: -35,
      fixedrange: true,
    },
    yaxis: {
      color: chartTheme.axis,
      gridcolor: chartTheme.grid,
      zerolinecolor: chartTheme.axis,
      zerolinewidth: 1,
      fixedrange: true,
    },
  };

  return (
    <PlotlyChart
      config={{ displayModeBar: false, responsive: true, scrollZoom: false }}
      data={traces}
      layout={layout}
      style={{ width: "100%", height: 220 }}
      useResizeHandler
    />
  );
}
