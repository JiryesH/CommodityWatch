import { cn } from "@/lib/utils/cn";

interface SparklineProps {
  values: number[];
  trend: "up" | "down" | "flat";
  width?: number;
  height?: number;
  className?: string;
}

export function Sparkline({
  values,
  trend,
  width = 120,
  height = 32,
  className,
}: SparklineProps) {
  if (values.length < 2) {
    return <div className={cn("h-8 w-[120px] rounded-sm bg-[color:var(--color-neutral-soft)]", className)} />;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const stroke =
    trend === "up"
      ? "var(--color-positive)"
      : trend === "down"
        ? "var(--color-negative)"
        : "var(--color-neutral)";
  const points = values
    .map((value, index) => {
      const x = (index / (values.length - 1)) * width;
      const y = height - (((value - min) / range) * (height - 4) + 2);
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <svg
      aria-hidden
      className={className}
      fill="none"
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      width={width}
    >
      <polyline points={points} stroke={stroke} strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} />
    </svg>
  );
}
