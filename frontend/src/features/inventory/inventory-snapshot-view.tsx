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
import { useIndicatorLatestMap, useIndicators, useInventorySnapshot } from "@/features/inventory/queries";
import { alertKindFromLatest, filterCardsByCommodityGroup, groupCardsForSnapshot } from "@/features/inventory/selectors";

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
  const indicatorsQuery = useIndicators({ module: "inventorywatch", limit: 200 });
  const cards = useMemo(
    () => filterCardsByCommodityGroup(snapshotQuery.data?.cards ?? [], groupSlug),
    [groupSlug, snapshotQuery.data?.cards],
  );
  const latestMap = useIndicatorLatestMap(cards.map((card) => card.indicatorId));

  const groupedCards = useMemo(() => groupCardsForSnapshot(cards), [cards]);
  const sectionEntries = useMemo(
    () =>
      SECTION_ORDER.map((name) => [name, groupedCards[name] ?? []] as const).filter(([, groupCards]) =>
        groupSlug === "all" ? groupCards.length > 0 : true,
      ),
    [groupSlug, groupedCards],
  );

  const activeGroup = getCommodityGroup(groupSlug);
  const trackedCount = cards.length || indicatorsQuery.data?.items.length || snapshotQuery.data?.cards.length || 0;

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
            ? `Dense snapshot of tracked inventory indicators across InventoryWatch. ${trackedCount} indicators currently exposed by the backend.`
            : `${groupDescriptionFor(groupSlug)} ${trackedCount ? `· ${trackedCount} tracked indicators currently indexed.` : ""}`
        }
        eyebrow="InventoryWatch"
        title={groupSlug === "all" ? "Market snapshot" : activeGroup.label}
      >
        <CommoditySubNav active={groupSlug} />
      </PageHeader>

      {!cards.length ? (
        <EmptyState
          message={`No snapshot indicators are currently available for ${activeGroup.label}. The navigation is in place and will populate as backend coverage expands.`}
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
              {sectionCards.length ? (
                <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                  {sectionCards.map((card) => (
                    <IndicatorCard
                      alertKind={alertKindFromLatest(latestMap[card.indicatorId])}
                      card={card}
                      href={`/inventory/${COMMODITY_GROUPS.find((group) => group.commodityCodes.includes(card.commodityCode ?? ""))?.slug ?? "all"}/${card.indicatorId}`}
                      key={card.indicatorId}
                    />
                  ))}
                </div>
              ) : (
                <EmptyState
                  message={`No indicators are currently available in ${sectionName}.`}
                  title="No data available"
                />
              )}
            </section>
          ))}
        </div>
      )}
    </div>
  );
}
