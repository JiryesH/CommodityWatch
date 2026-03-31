"use client";

import { useMemo } from "react";

import { useUIStore } from "@/stores/ui-store";

function readCssVariable(name: string) {
  if (typeof window === "undefined") {
    return "";
  }

  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function useChartTheme() {
  const theme = useUIStore((state) => state.theme);

  return useMemo(
    () => ({
      theme,
      grid: readCssVariable("--chart-grid"),
      axis: readCssVariable("--chart-axis"),
      band1090: readCssVariable("--chart-band-10-90"),
      band2575: readCssVariable("--chart-band-25-75"),
      median: readCssVariable("--chart-median"),
      currentYear: readCssVariable("--chart-current-year"),
      priorYear: readCssVariable("--chart-prior-year"),
      positive: readCssVariable("--chart-positive"),
      negative: readCssVariable("--chart-negative"),
      info: readCssVariable("--chart-info"),
      annotation: readCssVariable("--chart-annotation"),
      tooltipBg: readCssVariable("--chart-tooltip-bg"),
      tooltipBorder: readCssVariable("--chart-tooltip-border"),
      tooltipText: readCssVariable("--chart-tooltip-text"),
      foreground: readCssVariable("--color-text-primary"),
      foregroundMuted: readCssVariable("--color-text-muted"),
      background: readCssVariable("--color-bg-surface"),
    }),
    [theme],
  );
}
