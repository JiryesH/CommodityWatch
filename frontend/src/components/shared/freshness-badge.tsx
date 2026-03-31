import { AlertBadge } from "@/components/shared/alert-badge";
import type { FreshnessState } from "@/types/api";

const freshnessBadgeMap: Record<FreshnessState, { kind: "fresh" | "aged" | "awaiting-release" | "observed"; label: string }> =
  {
    live: { kind: "fresh", label: "Live" },
    current: { kind: "fresh", label: "Current" },
    lagged: { kind: "awaiting-release", label: "Lagged" },
    structural: { kind: "observed", label: "Structural" },
    aged: { kind: "aged", label: "Aged" },
  };

export function FreshnessBadge({ state }: { state: FreshnessState }) {
  const badge = freshnessBadgeMap[state];
  return <AlertBadge kind={badge.kind} label={badge.label} />;
}
