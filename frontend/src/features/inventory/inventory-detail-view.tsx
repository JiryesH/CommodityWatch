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
  alertKindFromSeasonal,
  buildChangeBarSeries,
  buildRecentReleaseRows,
  buildSeasonalSeries,
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
  const chartSeries = useMemo(() => (data ? buildSeasonalSeries(data) : null), [data]);
  const changeSeries = useMemo(() => (data ? buildChangeBarSeries(data.series) : []), [data]);
  const recentRows = useMemo(() => (data ? buildRecentReleaseRows(data) : []), [data]);
  const tableColumns = useMemo(
    () => [
      {
        accessorKey: "date",
        header: "Date",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => isoDate(row.original.date),
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
      {
        accessorKey: "vsMedian",
        header: "vs 5Y Median",
        cell: ({ row }: { row: { original: RecentReleaseRow } }) => formatSignedValue(row.original.vsMedian, data?.indicator.unit),
      },
      {
        accessorKey: "percentileRankLabel",
        header: "Percentile Rank",
      },
    ],
    [data?.indicator.unit],
  );
  const latestSeasonalPoint = data && latestQuery.data
    ? seasonalPointForLatest(latestQuery.data.latest.periodEndAt, data.seasonalRange, data.indicator.frequency)
    : null;
  const alertKind =
    data && latestQuery.data ? alertKindFromSeasonal(latestQuery.data.latest.value, latestSeasonalPoint) : null;
  const percentileLabel =
    data && latestQuery.data ? percentileBracketLabel(latestQuery.data.latest.value, latestSeasonalPoint) : "Unavailable";
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
        message="No data available for this indicator yet. The detail route is live and will populate as soon as the backend publishes the series."
        title="No data available"
      />
    );
  }

  const latestValue = latestQuery.data?.latest.value ?? data.series[data.series.length - 1]?.value ?? null;
  const latestTimestamp = latestQuery.data?.latest.releaseDate ?? data.metadata.latestReleaseAt ?? data.series[data.series.length - 1]?.releaseDate;

  return (
    <div className="space-y-6">
      {baseDataQuery.isError && baseDataQuery.data ? (
        <div className="card-surface border border-[color:var(--color-caution-soft)] bg-[color:var(--color-caution-soft)]/60 p-3 text-body text-caution">
          Latest cached data shown. Refresh failed.
        </div>
      ) : null}
      <PageHeader
        description={data.indicator.description ?? "Seasonal inventory range, period changes, and recent releases."}
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
                <h2 className="text-h2 text-foreground">Seasonal range</h2>
                <p className="mt-1 text-body text-foreground-secondary">
                  Current year versus the 5-year seasonal percentile bands.
                </p>
              </div>
              <SeasonalToggle excludeYear2020={excludeYear2020} onChange={setExcludeYear2020} />
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
              <EmptyState message="Seasonal data is not available for this indicator." title="No seasonal range" />
            )}
          </section>

          <section className="card-surface p-4">
            <div className="mb-4">
              <h2 className="text-h2 text-foreground">Period change</h2>
              <p className="mt-1 text-body text-foreground-secondary">
                Weekly or monthly builds and draws with inventory-aware color conventions.
              </p>
            </div>
            {changeSeries.length ? (
              <ChangeBarChart semanticMode={activeGroup.slug === "energy" || activeGroup.slug === "natural-gas" ? "inventory" : "generic"} series={changeSeries} />
            ) : (
              <EmptyState message="Not enough history is available to compute period changes." title="No change history" />
            )}
          </section>

          <DataTable columns={tableColumns} data={recentRows} exportName={`${data.indicator.code.toLowerCase()}-recent-releases`} />
        </div>

        <aside className="space-y-4">
          <div className="card-surface p-4">
            <h2 className="text-h3 text-foreground">Indicator summary</h2>
            <div className="mt-4 text-data-xl text-foreground">{formatValue(latestValue, data.indicator.unit)}</div>
            <DirectionalIndicator
              className="mt-2"
              semanticMode={activeGroup.slug === "energy" || activeGroup.slug === "natural-gas" ? "inventory" : "generic"}
              unit={data.indicator.unit}
              value={latestQuery.data?.latest.changeFromPriorAbs ?? null}
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {alertKind ? <AlertBadge kind={alertKind} /> : null}
              <AlertBadge kind="observed" label={percentileLabel} />
            </div>
            <dl className="mt-4 space-y-3">
              <div>
                <dt className="text-caption text-foreground-muted">Last updated</dt>
                <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">{formatUtcTimestamp(latestTimestamp)}</dd>
              </div>
              <div>
                <dt className="text-caption text-foreground-muted">Release date</dt>
                <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">
                  {latestQuery.data?.latest.releaseDate ? formatUtcTimestamp(latestQuery.data.latest.releaseDate) : "N/A"}
                </dd>
              </div>
              <div>
                <dt className="text-caption text-foreground-muted">vs 5Y median</dt>
                <dd className="mt-1 font-mono text-[13px] text-foreground-secondary">
                  {formatSignedValue(latestQuery.data?.latest.deviationFromSeasonalAbs ?? null, data.indicator.unit)}
                </dd>
              </div>
            </dl>
          </div>

          <div className="card-surface p-4">
            <h2 className="text-h3 text-foreground">Source</h2>
            <SourceAttribution
              href={data.metadata.sourceUrl}
              sourceLabel={data.metadata.sourceLabel ?? "CommodityWatch API"}
              timestamp={latestTimestamp ?? data.series[data.series.length - 1]?.periodEndAt}
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
