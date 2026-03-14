import { CalendarModule, HeadlineModule, PriceModule } from "./modules.js";

export const MODULE_REGISTRY = [
  {
    id: "prices",
    label: "Benchmark Prices",
    component: PriceModule,
    slot: "main-top",
    sectors: ["energy", "metals", "agriculture"],
    status: "live",
  },
  {
    id: "headlines",
    label: "Latest Headlines",
    component: HeadlineModule,
    slot: "main-left",
    sectors: ["energy", "metals", "agriculture", "macro", "cross-commodity"],
    status: "live",
  },
  {
    id: "calendar",
    label: "Upcoming Releases",
    component: CalendarModule,
    slot: "sidebar",
    sectors: ["energy", "metals", "agriculture", "macro", "cross-commodity"],
    status: "live",
  },
];
