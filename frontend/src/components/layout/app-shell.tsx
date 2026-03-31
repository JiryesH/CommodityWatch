"use client";

import { usePathname } from "next/navigation";
import { ReactNode } from "react";

import { ModuleNav } from "@/components/layout/module-nav";
import { TopNav } from "@/components/layout/top-nav";
import type { ModuleCode } from "@/types/api";

function resolveCurrentModule(pathname: string): ModuleCode {
  if (pathname.startsWith("/headlines")) return "headlines";
  if (pathname.startsWith("/prices")) return "prices";
  if (pathname.startsWith("/calendar")) return "calendar";
  if (pathname.startsWith("/supply")) return "supply";
  if (pathname.startsWith("/demand")) return "demand";
  if (pathname.startsWith("/weather")) return "weather";
  return "inventory";
}

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const currentModule = resolveCurrentModule(pathname);

  return (
    <div className="min-h-screen">
      <TopNav current={currentModule} />
      <ModuleNav current={currentModule} />
      <main className="content-shell pb-10 pt-6">{children}</main>
    </div>
  );
}
