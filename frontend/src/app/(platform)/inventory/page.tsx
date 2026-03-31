import type { Metadata } from "next";

import { InventorySnapshotView } from "@/features/inventory/inventory-snapshot-view";

export const metadata: Metadata = {
  title: "InventoryWatch | Market snapshot",
  description: "Track commodity inventories, builds, draws, and deviations from seasonal norms.",
};

export default function InventoryPage() {
  return <InventorySnapshotView groupSlug="all" />;
}
