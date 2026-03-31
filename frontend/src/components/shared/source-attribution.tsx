import Link from "next/link";

import { formatUtcTimestamp } from "@/lib/format/dates";
import { cn } from "@/lib/utils/cn";

interface SourceAttributionProps {
  sourceLabel: string;
  timestamp: string;
  href?: string | null;
  className?: string;
}

export function SourceAttribution({ sourceLabel, timestamp, href, className }: SourceAttributionProps) {
  const content = `${sourceLabel.toUpperCase()} · ${formatUtcTimestamp(timestamp)}`;

  if (href) {
    return (
      <Link
        className={cn("font-mono text-[12px] uppercase tracking-[0.08em] text-foreground-soft hover:text-foreground", className)}
        href={href}
        rel="noreferrer"
        target="_blank"
      >
        {content}
      </Link>
    );
  }

  return <div className={cn("font-mono text-[12px] uppercase tracking-[0.08em] text-foreground-soft", className)}>{content}</div>;
}
