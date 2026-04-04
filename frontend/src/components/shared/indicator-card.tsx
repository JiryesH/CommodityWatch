import Link from "next/link";

import { DirectionalIndicator } from "@/components/shared/directional-indicator";
import { FreshnessBadge } from "@/components/shared/freshness-badge";
import { SourceAttribution } from "@/components/shared/source-attribution";
import { Sparkline } from "@/components/shared/sparkline";
import { AlertBadge } from "@/components/shared/alert-badge";
import { alertKindFromSnapshotCard, changeReferenceLabel, snapshotSeasonalComparisonText, trendWindowLabel } from "@/features/inventory/selectors";
import { formatValue } from "@/lib/format/numbers";
import { cn } from "@/lib/utils/cn";
import type { SnapshotCardData } from "@/types/api";

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

interface IndicatorCardProps {
  card: SnapshotCardData;
  href: string;
}

export function IndicatorCard({ card, href }: IndicatorCardProps) {
  const alertKind = alertKindFromSnapshotCard(card);
  const seasonalComparison = snapshotSeasonalComparisonText(card);
  const changeLabel = changeReferenceLabel(card);
  const trendLabel = trendWindowLabel(card.sparkline.length);

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
          <div className="text-caption text-foreground-muted">Latest value</div>
          <div className="mt-1 text-data-xl text-foreground">{formatValue(card.latestValue, card.unit)}</div>
          <DirectionalIndicator
            className="mt-3"
            label={changeLabel}
            semanticMode={card.semanticMode}
            unit={card.unit}
            value={card.changeAbs}
          />
        </div>
      </div>
      <div className="mt-4">
        <div className="flex items-center justify-between gap-3">
          <div className="text-caption text-foreground-muted">{seasonalComparison.title}</div>
          <div className="font-mono text-[12px] text-foreground-secondary">{seasonalComparison.value}</div>
        </div>
      </div>
      <div className="mt-auto flex items-end justify-between gap-3 pt-5">
        <SourceAttribution
          sourceLabel={card.sourceLabel}
          timestamp={card.releaseDate ?? card.commodityWatchUpdatedAt ?? card.periodEndAt}
        />
        <div className="flex flex-col items-end gap-1">
          <div className="text-caption text-right text-foreground-muted">{trendLabel}</div>
          <Sparkline trend={sparklineTrend(card.sparkline)} values={card.sparkline} />
        </div>
      </div>
    </Link>
  );
}
