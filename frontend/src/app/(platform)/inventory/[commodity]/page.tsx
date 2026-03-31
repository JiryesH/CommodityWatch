import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getCommodityGroup, isCommodityGroupSlug } from "@/config/commodities";
import { InventorySnapshotView } from "@/features/inventory/inventory-snapshot-view";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ commodity: string }>;
}): Promise<Metadata> {
  const { commodity } = await params;
  if (!isCommodityGroupSlug(commodity)) {
    return {};
  }

  const group = getCommodityGroup(commodity);
  return {
    title: `InventoryWatch | ${group.label}`,
    description: `Inventory snapshot and tracked indicators for ${group.label}.`,
  };
}

export default async function CommodityInventoryPage({
  params,
}: {
  params: Promise<{ commodity: string }>;
}) {
  const { commodity } = await params;
  if (!isCommodityGroupSlug(commodity)) {
    notFound();
  }

  return <InventorySnapshotView groupSlug={commodity} />;
}
