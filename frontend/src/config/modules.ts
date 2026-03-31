import type { ModuleNavItem } from "@/types/api";

export const MODULE_NAV_ITEMS: ModuleNavItem[] = [
  {
    code: "inventory",
    label: "Inventory",
    href: "/inventory",
    enabled: true,
    description: "Market snapshot and seasonal inventory ranges.",
  },
  {
    code: "supply",
    label: "Supply",
    href: "/supply",
    enabled: false,
    description: "Observed output, disruptions, and capacity.",
  },
  {
    code: "demand",
    label: "Demand",
    href: "/demand",
    enabled: false,
    description: "Demand pulse and proxy indicators.",
  },
  {
    code: "weather",
    label: "Weather",
    href: "/weather",
    enabled: false,
    description: "Regional weather context for commodity markets.",
  },
  {
    code: "headlines",
    label: "Headlines",
    href: "/headlines",
    enabled: false,
    description: "Signal-focused commodity headlines.",
  },
  {
    code: "calendar",
    label: "Calendar",
    href: "/calendar",
    enabled: false,
    description: "Upcoming data releases and market events.",
  },
  {
    code: "prices",
    label: "Prices",
    href: "/prices",
    enabled: false,
    description: "Benchmark commodity prices and overlays.",
  },
];
