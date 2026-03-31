import Link from "next/link";

import { COMMODITY_GROUPS, type CommodityGroupSlug } from "@/config/commodities";
import { cn } from "@/lib/utils/cn";

export function CommoditySubNav({ active = "all" }: { active?: CommodityGroupSlug }) {
  return (
    <nav aria-label="Inventory commodity groups" className="overflow-x-auto">
      <div className="flex min-w-max items-center gap-2">
        {COMMODITY_GROUPS.map((group) => {
          const href = group.slug === "all" ? "/inventory" : `/inventory/${group.slug}`;

          return (
            <Link
              className={cn(
                "inline-flex h-9 items-center rounded-full border px-3 text-body transition-colors",
                active === group.slug
                  ? "border-accent bg-[color:var(--color-accent-soft)] text-foreground"
                  : "border-border-subtle bg-surface text-foreground-secondary hover:text-foreground",
              )}
              href={href}
              key={group.slug}
            >
              {group.label}
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
