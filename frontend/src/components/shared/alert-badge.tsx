import { cn } from "@/lib/utils/cn";
import type { AlertKind } from "@/types/api";

const badgeStyles: Record<AlertKind, string> = {
  "extreme-low": "border-[color:var(--color-positive-soft)] bg-[color:var(--color-positive-soft)] text-positive",
  "extreme-high": "border-[color:var(--color-negative-soft)] bg-[color:var(--color-negative-soft)] text-negative",
  fresh: "border-[color:var(--color-positive-soft)] bg-[color:var(--color-positive-soft)] text-positive",
  aged: "border-[color:var(--color-caution-soft)] bg-[color:var(--color-caution-soft)] text-caution",
  "awaiting-release": "border-[color:var(--color-neutral-soft)] bg-[color:var(--color-neutral-soft)] text-foreground-muted",
  estimate: "border-[color:var(--color-info-soft)] bg-[color:var(--color-info-soft)] text-info",
  proxy: "border-[color:var(--color-info-soft)] bg-[color:var(--color-info-soft)] text-info",
  observed: "border-[color:var(--color-neutral-soft)] bg-[color:var(--color-neutral-soft)] text-foreground-secondary",
  disruption: "border-[color:var(--color-negative-soft)] bg-[color:var(--color-negative-soft)] text-negative",
};

const defaultLabels: Record<AlertKind, string> = {
  "extreme-low": "Below 10th",
  "extreme-high": "Above 90th",
  fresh: "Fresh",
  aged: "Aged",
  "awaiting-release": "Awaiting",
  estimate: "Estimate",
  proxy: "Proxy",
  observed: "Observed",
  disruption: "Disruption",
};

export function AlertBadge({ kind, label }: { kind: AlertKind; label?: string }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2 py-0.5 font-mono text-[11px] font-medium uppercase tracking-[0.06em]",
        badgeStyles[kind],
      )}
    >
      {label ?? defaultLabels[kind]}
    </span>
  );
}
