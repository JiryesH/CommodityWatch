import Link from "next/link";

import { MODULE_NAV_ITEMS } from "@/config/modules";
import { cn } from "@/lib/utils/cn";
import type { ModuleCode } from "@/types/api";

export function ModuleNav({ current }: { current: ModuleCode }) {
  return (
    <nav
      aria-label="Primary module navigation"
      className="sticky top-[var(--size-top-app-bar-height)] z-30 border-b border-border-subtle bg-[color:color-mix(in_srgb,var(--color-bg-surface)_94%,transparent)] backdrop-blur-md"
    >
      <div className="content-shell overflow-x-auto">
        <div className="flex min-w-max items-stretch gap-2 py-2">
          {MODULE_NAV_ITEMS.map((item) =>
            item.enabled ? (
              <Link
                className={cn(
                  "inline-flex items-center border-b-2 px-2 py-2 text-body-strong transition-colors",
                  item.code === current
                    ? "border-accent text-foreground"
                    : "border-transparent text-foreground-muted hover:text-foreground",
                )}
                href={item.href}
                key={item.code}
              >
                {item.label}
              </Link>
            ) : (
              <span
                aria-disabled="true"
                className="inline-flex cursor-not-allowed items-center border-b-2 border-transparent px-2 py-2 text-body text-foreground-soft"
                key={item.code}
              >
                {item.label}
                <span className="ml-1 text-caption uppercase tracking-[0.08em]">Soon</span>
              </span>
            ),
          )}
        </div>
      </div>
    </nav>
  );
}
