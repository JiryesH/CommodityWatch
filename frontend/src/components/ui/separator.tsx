import { cn } from "@/lib/utils/cn";

export function Separator({ className }: { className?: string }) {
  return <div aria-hidden className={cn("h-px w-full bg-border-subtle", className)} />;
}
