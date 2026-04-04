import { cn } from "@/lib/utils/cn";
import { formatSignedValue } from "@/lib/format/numbers";

interface DirectionalIndicatorProps {
  value: number | null;
  unit: string | null | undefined;
  semanticMode?: "inventory" | "generic";
  label?: string;
  className?: string;
}

export function DirectionalIndicator({
  value,
  unit,
  semanticMode = "generic",
  label = "vs prior period",
  className,
}: DirectionalIndicatorProps) {
  if (value == null) {
    return (
      <div className={cn("space-y-1", className)}>
        <div className="text-caption text-foreground-muted">{label}</div>
        <div className="font-mono text-[12px] text-foreground-soft">No change</div>
      </div>
    );
  }

  const direction = value > 0 ? "up" : value < 0 ? "down" : "flat";
  const isPositiveSignal =
    semanticMode === "inventory"
      ? value < 0
      : value > 0;

  const arrow = direction === "up" ? "↑" : direction === "down" ? "↓" : "→";
  const colorClass = direction === "flat" ? "text-foreground-muted" : isPositiveSignal ? "text-positive" : "text-negative";

  return (
    <div className={cn("space-y-1", className)}>
      <div className="text-caption text-foreground-muted">{label}</div>
      <div className={cn("font-mono text-[12px] uppercase tracking-[0.04em]", colorClass)}>
        {arrow} {formatSignedValue(value, unit)}
      </div>
    </div>
  );
}
