"use client";

import { useMemo } from "react";

import { CommoditySubNav } from "@/components/layout/commodity-sub-nav";
import { EmptyState } from "@/components/shared/empty-state";
import { ErrorState } from "@/components/shared/error-state";
import { IndicatorCard } from "@/components/shared/indicator-card";
import { LoadingState } from "@/components/shared/loading-state";
import { PageHeader } from "@/components/shared/page-header";
import { COMMODITY_GROUPS, getCommodityGroup, type CommodityGroupSlug } from "@/config/commodities";
import { groupDescriptionFor } from "@/features/inventory/indicator-registry";
import { useInventorySnapshot } from "@/features/inventory/queries";
import { buildVisibleSnapshotSections, filterCardsByCommodityGroup, groupCardsForSnapshot } from "@/features/inventory/selectors";

const SECTION_ORDER = [
  "Crude Oil",
  "Refined Products",
  "Natural Gas",
  "Base Metals",
  "Grains",
  "Softs",
  "Precious Metals",
  "Inventory",
];

function SnapshotGridSkeleton() {
  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
      {Array.from({ length: 12 }).map((_, index) => (
        <LoadingState key={index} variant="card" />
      ))}
    </div>
  );
}

export function InventorySnapshotView({ groupSlug = "all" }: { groupSlug?: CommodityGroupSlug }) {
  const snapshotQuery = useInventorySnapshot({ includeSparklines: true, limit: 100 });
  const cards = useMemo(
    () => filterCardsByCommodityGroup(snapshotQuery.data?.cards ?? [], groupSlug),
    [groupSlug, snapshotQuery.data?.cards],
  );

  const groupedCards = useMemo(() => groupCardsForSnapshot(cards), [cards]);
  const sectionEntries = useMemo(() => buildVisibleSnapshotSections(groupedCards, SECTION_ORDER), [groupedCards]);

  const activeGroup = getCommodityGroup(groupSlug);
  const trackedCount = groupSlug === "all" ? snapshotQuery.data?.cards.length ?? 0 : cards.length;

  if (snapshotQuery.isLoading && !snapshotQuery.data) {
    return (
      <div className="space-y-6">
        <PageHeader
          description={groupDescriptionFor(groupSlug)}
          eyebrow="InventoryWatch"
          title={groupSlug === "all" ? "Market snapshot" : activeGroup.label}
        >
          <CommoditySubNav active={groupSlug} />
        </PageHeader>
        <SnapshotGridSkeleton />
      </div>
    );
  }

  if (snapshotQuery.isError && !snapshotQuery.data) {
    return (
      <div className="space-y-6">
        <PageHeader
          description={groupDescriptionFor(groupSlug)}
          eyebrow="InventoryWatch"
          title={groupSlug === "all" ? "Market snapshot" : activeGroup.label}
        >
          <CommoditySubNav active={groupSlug} />
        </PageHeader>
        <ErrorState
          message="Unable to load data. Tap to retry."
          onRetry={() => {
            void snapshotQuery.refetch();
          }}
        />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {snapshotQuery.isError && snapshotQuery.data ? (
        <div className="card-surface border border-[color:var(--color-caution-soft)] bg-[color:var(--color-caution-soft)]/60 p-3 text-body text-caution">
          Latest cached data shown. Refresh failed.
        </div>
      ) : null}
      <PageHeader
        description={
          groupSlug === "all"
            ? `Tracked inventory series across InventoryWatch. ${trackedCount} indicators in the current snapshot.`
            : `${groupDescriptionFor(groupSlug)}${trackedCount ? ` · ${trackedCount} indicators in view.` : ""}`
        }
        eyebrow="InventoryWatch"
        title={groupSlug === "all" ? "Market snapshot" : activeGroup.label}
      >
        <CommoditySubNav active={groupSlug} />
      </PageHeader>

      {!cards.length ? (
        <EmptyState
          message={`No published indicators are available for ${activeGroup.label} right now.`}
          title="No data available"
        />
      ) : (
        <div className="space-y-8">
          {sectionEntries.map(([sectionName, sectionCards]) => (
            <section key={sectionName}>
              <div className="mb-3 flex items-end justify-between gap-3">
                <div>
                  <h2 className="text-h2 text-foreground">{sectionName}</h2>
                  <p className="mt-1 text-caption text-foreground-muted">
                    {sectionCards.length} indicator{sectionCards.length === 1 ? "" : "s"}
                  </p>
                </div>
              </div>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                {sectionCards.map((card) => (
                  <IndicatorCard
                    card={card}
                    href={`/inventory/${COMMODITY_GROUPS.find((group) => group.commodityCodes.includes(card.commodityCode ?? ""))?.slug ?? "all"}/${card.indicatorId}`}
                    key={card.indicatorId}
                  />
                ))}
              </div>
            </section>
          ))}
        </div>
      )}
    </div>
  );
}
