"use client";

import dynamic from "next/dynamic";
import type { ComponentType } from "react";
import type { Config, Data, Layout } from "plotly.js";

interface PlotlyChartProps {
  data: Data[];
  layout: Partial<Layout>;
  config?: Partial<Config>;
  className?: string;
  useResizeHandler?: boolean;
  style?: React.CSSProperties;
}

const Plot = dynamic(
  async () => (await import("react-plotly.js")).default as ComponentType<PlotlyChartProps>,
  { ssr: false },
) as ComponentType<PlotlyChartProps>;

export function PlotlyChart(props: PlotlyChartProps) {
  return <Plot {...props} />;
}
