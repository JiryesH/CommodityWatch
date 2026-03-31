import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getCommodityGroup, isCommodityGroupSlug } from "@/config/commodities";
import { InventoryDetailView } from "@/features/inventory/inventory-detail-view";

function humanizeIndicatorId(indicatorId: string) {
  return indicatorId.replaceAll("_", " ");
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ commodity: string; indicatorId: string }>;
}): Promise<Metadata> {
  const { commodity, indicatorId } = await params;
  if (!isCommodityGroupSlug(commodity)) {
    return {};
  }

  return {
    title: `InventoryWatch | ${humanizeIndicatorId(indicatorId)}`,
    description: `Seasonal inventory chart, recent changes, and release history for ${humanizeIndicatorId(indicatorId)} in ${getCommodityGroup(commodity).label}.`,
  };
}

export default async function IndicatorDetailPage({
  params,
}: {
  params: Promise<{ commodity: string; indicatorId: string }>;
}) {
  const { commodity, indicatorId } = await params;
  if (!isCommodityGroupSlug(commodity)) {
    notFound();
  }

  return <InventoryDetailView commoditySlug={commodity} indicatorId={indicatorId} />;
}
