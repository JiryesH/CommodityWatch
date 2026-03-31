import { Skeleton } from "@/components/ui/skeleton";

export function LoadingState({
  variant,
  rows = 3,
}: {
  variant: "card" | "chart" | "table" | "feed";
  rows?: number;
}) {
  if (variant === "chart") {
    return (
      <div className="card-surface p-4">
        <Skeleton className="mb-4 h-5 w-40" />
        <Skeleton className="h-[280px] w-full md:h-[340px] xl:h-[420px]" />
      </div>
    );
  }

  if (variant === "table") {
    return (
      <div className="card-surface p-4">
        <Skeleton className="mb-3 h-5 w-32" />
        <div className="space-y-2">
          {Array.from({ length: rows }).map((_, index) => (
            <Skeleton className="h-10 w-full" key={index} />
          ))}
        </div>
      </div>
    );
  }

  if (variant === "feed") {
    return (
      <div className="space-y-3">
        {Array.from({ length: rows }).map((_, index) => (
          <Skeleton className="h-16 w-full rounded-xl" key={index} />
        ))}
      </div>
    );
  }

  return (
    <div className="card-surface p-3">
      <Skeleton className="mb-3 h-4 w-24" />
      <Skeleton className="mb-4 h-8 w-32" />
      <Skeleton className="mb-3 h-4 w-40" />
      <Skeleton className="h-6 w-full" />
    </div>
  );
}
