import Link from "next/link";

import { AlertBadge } from "@/components/shared/alert-badge";
import { DirectionalIndicator } from "@/components/shared/directional-indicator";
import { FreshnessBadge } from "@/components/shared/freshness-badge";
import { SourceAttribution } from "@/components/shared/source-attribution";
import { Sparkline } from "@/components/shared/sparkline";
import { formatSignedValue, formatValue } from "@/lib/format/numbers";
import { cn } from "@/lib/utils/cn";
import type { AlertKind, SnapshotCardData } from "@/types/api";

function signalRuleClass(signal: SnapshotCardData["signal"], freshness: SnapshotCardData["freshness"]) {
  if (freshness === "aged") {
    return "before:bg-caution";
  }

  if (signal === "tightening") {
    return "before:bg-positive";
  }

  if (signal === "loosening") {
    return "before:bg-negative";
  }

  return "before:bg-border";
}

function sparklineTrend(values: number[]) {
  if (values.length < 2) return "flat" as const;
  const delta = values[values.length - 1] - values[0];
  if (delta > 0) return "up" as const;
  if (delta < 0) return "down" as const;
  return "flat" as const;
}

function DeviationBar({ value }: { value: number | null }) {
  if (value == null) {
    return <div className="h-2 rounded-full bg-[color:var(--color-neutral-soft)]" />;
  }

  const ratio = Math.max(-1, Math.min(1, value / 25));
  const width = `${Math.abs(ratio) * 50}%`;
  const alignClass = ratio >= 0 ? "left-1/2" : "right-1/2";
  const colorClass = ratio >= 0 ? "bg-negative" : "bg-positive";

  return (
    <div className="relative h-2 rounded-full bg-[color:var(--color-neutral-soft)]">
      <div className="absolute bottom-0 left-1/2 top-0 w-px -translate-x-1/2 bg-border-strong" />
      <div className={cn("absolute bottom-0 top-0 rounded-full", alignClass, colorClass)} style={{ width }} />
    </div>
  );
}

interface IndicatorCardProps {
  card: SnapshotCardData;
  href: string;
  alertKind?: AlertKind | null;
}

export function IndicatorCard({ card, href, alertKind }: IndicatorCardProps) {
  const deviationLabel =
    card.deviationAbs == null
      ? "Deviation unavailable"
      : `${formatSignedValue(card.deviationAbs, card.unit)} ${card.deviationAbs < 0 ? "below" : "above"} avg`;

  return (
    <Link
      className={cn(
        "card-surface group relative flex min-h-[214px] flex-col overflow-hidden p-3 transition-transform hover:-translate-y-0.5",
        "before:absolute before:bottom-0 before:left-0 before:top-0 before:w-1",
        signalRuleClass(card.signal, card.freshness),
      )}
      href={href}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="font-mono text-[11px] uppercase tracking-[0.1em] text-foreground-soft">{card.snapshotGroup}</div>
          <h2 className="mt-2 text-h3 text-foreground">{card.name}</h2>
        </div>
        <div className="flex flex-col items-end gap-2">
          <FreshnessBadge state={card.freshness} />
          {alertKind ? <AlertBadge kind={alertKind} /> : null}
        </div>
      </div>
      <div className="mt-4 flex items-end justify-between gap-4">
        <div className="min-w-0">
          <div className="text-data-xl text-foreground">{formatValue(card.latestValue, card.unit)}</div>
          <DirectionalIndicator className="mt-2" semanticMode={card.semanticMode} unit={card.unit} value={card.changeAbs} />
        </div>
      </div>
      <div className="mt-4 space-y-2">
        <div className="flex items-center justify-between gap-3">
          <div className="text-caption text-foreground-muted">vs 5Y median</div>
          <div className="font-mono text-[12px] text-foreground-secondary">{deviationLabel}</div>
        </div>
        <DeviationBar value={card.deviationAbs} />
      </div>
      <div className="mt-auto flex items-end justify-between gap-3 pt-5">
        <SourceAttribution sourceLabel={card.sourceLabel} timestamp={card.lastUpdatedAt} />
        <Sparkline trend={sparklineTrend(card.sparkline)} values={card.sparkline} />
      </div>
    </Link>
  );
}
