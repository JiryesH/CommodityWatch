import { ReactNode } from "react";

import { cn } from "@/lib/utils/cn";

interface PageHeaderProps {
  title: string;
  description: string;
  actions?: ReactNode;
  eyebrow?: string;
  children?: ReactNode;
  className?: string;
}

export function PageHeader({ title, description, actions, eyebrow, children, className }: PageHeaderProps) {
  return (
    <section className={cn("mb-6 flex flex-col gap-4", className)}>
      <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div className="max-w-3xl">
          {eyebrow ? (
            <div className="mb-2 text-data-sm uppercase tracking-[0.12em] text-foreground-soft">{eyebrow}</div>
          ) : null}
          <h1 className="text-h1 text-foreground">{title}</h1>
          <p className="mt-2 max-w-2xl text-body text-foreground-secondary">{description}</p>
        </div>
        {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
      </div>
      {children}
    </section>
  );
}
