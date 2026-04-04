"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

import { AlertBadge } from "@/components/shared/alert-badge";
import { ChangeBarChart } from "@/components/shared/change-bar-chart";
import { DataTable } from "@/components/shared/data-table";
import { DirectionalIndicator } from "@/components/shared/directional-indicator";
import { EmptyState } from "@/components/shared/empty-state";
import { ErrorState } from "@/components/shared/error-state";
import { LoadingState } from "@/components/shared/loading-state";
import { PageHeader } from "@/components/shared/page-header";
import { SeasonalRangeChart } from "@/components/shared/seasonal-range-chart";
import { SourceAttribution } from "@/components/shared/source-attribution";
import { COMMODITY_GROUPS, getCommodityGroup, type CommodityGroupSlug } from "@/config/commodities";
import { useIndicatorData, useIndicatorLatest } from "@/features/inventory/queries";
import { SeasonalToggle } from "@/features/inventory/seasonal-toggle";
import {
  alertKindFromSeasonalPosition,
  buildChangeBarSeries,
  buildRecentReleaseRows,
  buildSeasonalSeries,
  changeReferenceLabel,
  describeMedianDeviation,
  hasSeasonalCoverage,
  percentileBracketLabel,
  seasonalPointForLatest,
  type RecentReleaseRow,
} from "@/features/inventory/selectors";
import { formatUtcTimestamp, isoDate } from "@/lib/format/dates";
import { formatPercent, formatSignedValue, formatValue } from "@/lib/format/numbers";

function alternateSeasonalProfile(groupSlug: CommodityGroupSlug) {
  return groupSlug === "natural-gas" ? "inventorywatch_5y_daily_ex_2020" : "inventorywatch_5y_ex_2020";
}

export function InventoryDetailView({
  commoditySlug,
  indicatorId,
}: {
  commoditySlug: CommodityGroupSlug;
  indicatorId: string;
}) {
  const [excludeYear2020, setExcludeYear2020] = useState(false);
  const baseDataQuery = useIndicatorData(indicatorId, { includeSeasonal: true, limitPoints: 2500 });
  const alternateDataQuery = useIndicatorData(indicatorId, {
    includeSeasonal: true,
    seasonalProfile: alternateSeasonalProfile(commoditySlug),
    limitPoints: 2500,
  });
  const latestQuery = useIndicatorLatest(indicatorId);

  const data = excludeYear2020 && alternateDataQuery.data?.seasonalRange.length ? alternateDataQuery.data : baseDataQuery.data;
  const showSeasonalCoverage = Boolean(data && hasSeasonalCoverage(data));
  const chartSeries = useMemo(() => (data && showSeasonalCoverage ? buildSeasonalSeries(data) : null), [data, showSeasonalCoverage]);
  const changeSeries = useMemo(() => (data ? buildChangeBarSeries(data.series) : []), [data]);
  const recentRows = useMemo(() => (data ? buildRecentReleaseRows(data) : []), [data]);
  const tableColumns = useMemo(() => {
    const columns = [
      {
        accessorKey: "periodEndAt",
        header: "Period end",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => isoDate(row.original.periodEndAt),
      },
      {
        accessorKey: "releaseDate",
        header: "Released",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => isoDate(row.original.releaseDate),
      },
      {
        accessorKey: "value",
        header: "Value",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => formatValue(row.original.value, data?.indicator.unit),
      },
      {
        accessorKey: "change",
        header: "Change",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => formatSignedValue(row.original.change, data?.indicator.unit),
      },
      {
        accessorKey: "percentChange",
        header: "% Change",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => formatPercent(row.original.percentChange),
      },
    ];

    if (showSeasonalCoverage) {
      columns.push(
        {
          accessorKey: "vsMedian",
          header: "vs 5Y Median",
          cell: ({ row }: { row: { original: RecentReleaseRow } }) => formatSignedValue(row.original.vsMedian, data?.indicator.unit),
        },
        {
          accessorKey: "percentileRankLabel",
          header: "Percentile Rank",
          cell: ({ row }: { row: { original: RecentReleaseRow } }) => row.original.percentileRankLabel,
        },
      );
    }

    return columns;
  }, [data?.indicator.unit, showSeasonalCoverage]);
  const latestSeasonalPoint =
    data && latestQuery.data && showSeasonalCoverage
      ? seasonalPointForLatest(latestQuery.data.latest.periodEndAt, data.seasonalRange, data.indicator)
      : null;
  const activeGroup = getCommodityGroup(commoditySlug);

  if (baseDataQuery.isLoading && !data) {
    return (
      <div className="space-y-6">
        <LoadingState variant="chart" />
        <LoadingState variant="chart" />
        <LoadingState rows={8} variant="table" />
      </div>
    );
  }

  if (baseDataQuery.isError && !data) {
    return (
      <ErrorState
        message="Unable to load data. Tap to retry."
        onRetry={() => {
          void baseDataQuery.refetch();
          void latestQuery.refetch();
        }}
      />
    );
  }

  if (!data || !data.series.length) {
    return (
      <EmptyState
        message="No published observations are available for this indicator yet."
        title="No data available"
      />
    );
  }

  const semanticMode = activeGroup.slug === "energy" || activeGroup.slug === "natural-gas" ? "inventory" : "generic";
  const changeLabel = changeReferenceLabel(data.indicator);
  const latestValue = latestQuery.data?.latest.value ?? data.series[data.series.length - 1]?.value ?? null;
  const latestPeriodEndAt =
    latestQuery.data?.latest.periodEndAt ?? data.metadata.latestPeriodEndAt ?? data.series[data.series.length - 1]?.periodEndAt ?? null;
  const latestReleaseDate =
    latestQuery.data?.latest.releaseDate ?? data.metadata.latestReleaseAt ?? data.series[data.series.length - 1]?.releaseDate ?? null;
  const commodityWatchUpdatedAt =
    latestQuery.data?.latest.commodityWatchUpdatedAt ?? data.metadata.latestVintageAt ?? data.series[data.series.length - 1]?.vintageAt ?? null;
  const latestDeviationFromMedian =
    latestValue != null && latestSeasonalPoint?.p50 != null ? latestValue - latestSeasonalPoint.p50 : null;
  const alertKind = latestValue != null && showSeasonalCoverage ? alertKindFromSeasonalPosition(latestValue, latestSeasonalPoint) : null;
  const percentileLabel = latestValue != null && showSeasonalCoverage ? percentileBracketLabel(latestValue, latestSeasonalPoint) : "Current only";

  return (
    <div className="space-y-6">
      {baseDataQuery.isError && baseDataQuery.data ? (
        <div className="card-surface border border-[color:var(--color-caution-soft)] bg-[color:var(--color-caution-soft)]/60 p-3 text-body text-caution">
          Latest cached data shown. Refresh failed.
        </div>
      ) : null}
      <PageHeader
        description={
          data.indicator.description ??
          (showSeasonalCoverage
            ? "Seasonal inventory range, period changes, and recent releases."
            : "Latest value, period changes, and recent releases.")
        }
        eyebrow="InventoryWatch"
        title={data.indicator.name}
      >
        <div className="flex flex-wrap items-center gap-2 text-caption text-foreground-muted">
          <Link href="/inventory">Inventory</Link>
          <span>→</span>
          <Link href={`/inventory/${commoditySlug}`}>{activeGroup.label}</Link>
          <span>→</span>
          <span>{data.indicator.code}</span>
        </div>
      </PageHeader>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px]">
        <div className="space-y-6">
          <section className="card-surface p-4">
            <div className="mb-4 flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
              <div>
                <h2 className="text-h2 text-foreground">{showSeasonalCoverage ? "Seasonal range" : "History"}</h2>
                <p className="mt-1 text-body text-foreground-secondary">
                  {showSeasonalCoverage
                    ? "Current year versus the 5-year seasonal percentile bands."
                    : "Current releases and period changes. Seasonal comparison is not shown for this series."}
                </p>
              </div>
              {showSeasonalCoverage ? <SeasonalToggle excludeYear2020={excludeYear2020} onChange={setExcludeYear2020} /> : null}
            </div>
            {chartSeries ? (
              <SeasonalRangeChart
                currentYear={chartSeries.currentYear}
                periodMode={chartSeries.mode}
                priorYear={chartSeries.priorYear}
                seasonalRange={data.seasonalRange}
                unit={data.indicator.unit ?? ""}
              />
            ) : (
              <EmptyState
                message="Seasonal comparison is not shown for this series."
                title="Current only"
              />
            )}
          </section>

          <section className="card-surface p-4">
            <div className="mb-4">
              <h2 className="text-h2 text-foreground">Period change</h2>
              <p className="mt-1 text-body text-foreground-secondary">
                {semanticMode === "inventory"
                  ? "Each release versus the prior period, with inventory-style color cues."
                  : "Each release versus the prior period."}
              </p>
            </div>
            {changeSeries.length ? (
              <ChangeBarChart semanticMode={semanticMode} series={changeSeries} />
            ) : (
              <EmptyState message="Not enough history is available to compute period changes." title="No change history" />
            )}
          </section>

          <DataTable
            columns={tableColumns}
            data={recentRows}
            description="Latest 20 published observations with period end and release date."
            exportName={`${data.indicator.code.toLowerCase()}-recent-releases`}
            title="Recent releases"
          />
        </div>

        <aside className="space-y-4">
          <div className="card-surface p-4">
            <h2 className="text-h3 text-foreground">Indicator summary</h2>
            <div className="mt-4 text-caption text-foreground-muted">Latest value</div>
            <div className="mt-1 text-data-xl text-foreground">{formatValue(latestValue, data.indicator.unit)}</div>
            <DirectionalIndicator
              className="mt-3"
              label={changeLabel}
              semanticMode={semanticMode}
              unit={data.indicator.unit}
              value={latestQuery.data?.latest.changeFromPriorAbs ?? null}
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {alertKind ? <AlertBadge kind={alertKind} /> : null}
              {showSeasonalCoverage ? <AlertBadge kind="observed" label={percentileLabel} /> : null}
            </div>
            <dl className="mt-4 space-y-3">
              <div>
                <dt className="text-caption text-foreground-muted">Period end</dt>
                <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">{isoDate(latestPeriodEndAt)}</dd>
              </div>
              <div>
                <dt className="text-caption text-foreground-muted">Release date</dt>
                <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">
                  {latestReleaseDate ? formatUtcTimestamp(latestReleaseDate) : "N/A"}
                </dd>
              </div>
              <div>
                <dt className="text-caption text-foreground-muted">CommodityWatch updated</dt>
                <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">
                  {commodityWatchUpdatedAt ? formatUtcTimestamp(commodityWatchUpdatedAt) : "Awaiting update"}
                </dd>
              </div>
              {showSeasonalCoverage ? (
                <div>
                  <dt className="text-caption text-foreground-muted">vs 5Y median</dt>
                  <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">
                    {describeMedianDeviation(latestDeviationFromMedian, data.indicator.unit)}
                  </dd>
                </div>
              ) : (
                <div>
                  <dt className="text-caption text-foreground-muted">Seasonal baseline</dt>
                  <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">Current only</dd>
                </div>
              )}
            </dl>
          </div>

          <div className="card-surface p-4">
            <h2 className="text-h3 text-foreground">Source</h2>
            <SourceAttribution
              href={data.metadata.sourceUrl}
              sourceLabel={data.metadata.sourceLabel ?? "CommodityWatch API"}
              timestamp={latestReleaseDate ?? latestPeriodEndAt ?? commodityWatchUpdatedAt ?? data.series[data.series.length - 1]?.periodEndAt}
            />
            <p className="mt-3 text-body text-foreground-secondary">
              Native frequency: <span className="font-mono uppercase">{data.indicator.frequency}</span>
            </p>
          </div>
        </aside>
      </div>
    </div>
  );
}
