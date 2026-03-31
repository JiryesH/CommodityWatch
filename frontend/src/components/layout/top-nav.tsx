"use client";

import { Menu } from "lucide-react";
import Link from "next/link";
import { useEffect } from "react";

import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import { MODULE_NAV_ITEMS } from "@/config/modules";
import { cn } from "@/lib/utils/cn";
import { useUIStore } from "@/stores/ui-store";
import type { ModuleCode } from "@/types/api";

export function TopNav({ current }: { current: ModuleCode }) {
  const mobileNavOpen = useUIStore((state) => state.mobileNavOpen);
  const setMobileNavOpen = useUIStore((state) => state.setMobileNavOpen);
  const theme = useUIStore((state) => state.theme);
  const toggleTheme = useUIStore((state) => state.toggleTheme);
  const syncThemeFromDom = useUIStore((state) => state.syncThemeFromDom);

  useEffect(() => {
    syncThemeFromDom();
  }, [syncThemeFromDom]);

  return (
    <header className="sticky top-0 z-40 border-b border-border-subtle bg-[color:color-mix(in_srgb,var(--color-bg-surface)_90%,transparent)] backdrop-blur-md">
      <div className="content-shell flex h-[var(--size-top-app-bar-height)] items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-3">
          <Link className="flex min-w-0 items-center gap-3" href="/inventory">
            <div className="flex h-9 w-9 items-center justify-center rounded-md border border-[color:var(--color-accent-soft)] bg-[color:var(--color-accent-soft)] text-data-sm text-accent">
              CT
            </div>
            <div className="min-w-0">
              <div className="truncate text-body-strong text-foreground">Contango</div>
              <div className="truncate text-data-sm uppercase tracking-[0.12em] text-foreground-soft">
                CommodityWatch.co
              </div>
            </div>
          </Link>
        </div>
        <div className="hidden items-center gap-4 lg:flex">
          <div className="rounded-md border border-border-subtle bg-surface-alt px-3 py-1.5 text-caption text-foreground-muted">
            InventoryWatch MVP
          </div>
          <label className="flex items-center gap-2 text-caption text-foreground-muted">
            <span>Dark</span>
            <Switch
              aria-label="Toggle dark mode"
              checked={theme === "dark"}
              onCheckedChange={() => toggleTheme()}
            />
          </label>
        </div>
        <div className="flex items-center gap-2 lg:hidden">
          <label className="flex items-center gap-2 text-caption text-foreground-muted">
            <span className="sr-only">Dark mode</span>
            <Switch
              aria-label="Toggle dark mode"
              checked={theme === "dark"}
              onCheckedChange={() => toggleTheme()}
            />
          </label>
          <Sheet onOpenChange={setMobileNavOpen} open={mobileNavOpen}>
            <SheetTrigger asChild>
              <Button aria-label="Open navigation" size="icon" variant="ghost">
                <Menu className="h-4 w-4" />
              </Button>
            </SheetTrigger>
            <SheetContent>
              <div className="mt-8 flex flex-col gap-5">
                <div>
                  <div className="text-h3 text-foreground">Modules</div>
                  <p className="mt-1 text-caption text-foreground-muted">
                    Inventory is live in Phase 1. Other tabs stay visible for platform context.
                  </p>
                </div>
                <nav className="flex flex-col gap-2" aria-label="Mobile module navigation">
                  {MODULE_NAV_ITEMS.map((item) =>
                    item.enabled ? (
                      <Link
                        className={cn(
                          "rounded-md border px-3 py-2 text-body transition-colors",
                          item.code === current
                            ? "border-accent bg-[color:var(--color-accent-soft)] text-foreground"
                            : "border-border-subtle bg-surface-alt text-foreground-secondary",
                        )}
                        href={item.href}
                        key={item.code}
                        onClick={() => setMobileNavOpen(false)}
                      >
                        {item.label}
                      </Link>
                    ) : (
                      <div
                        className="rounded-md border border-border-subtle bg-surface-alt px-3 py-2 text-body text-foreground-soft"
                        key={item.code}
                      >
                        {item.label}
                        <span className="ml-2 text-caption uppercase tracking-[0.08em]">Soon</span>
                      </div>
                    ),
                  )}
                </nav>
              </div>
            </SheetContent>
          </Sheet>
        </div>
      </div>
    </header>
  );
}
