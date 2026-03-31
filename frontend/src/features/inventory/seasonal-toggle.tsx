"use client";

import { Switch } from "@/components/ui/switch";

export function SeasonalToggle({
  excludeYear2020,
  onChange,
}: {
  excludeYear2020: boolean;
  onChange: (value: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 text-caption text-foreground-secondary">
      <Switch checked={excludeYear2020} onCheckedChange={onChange} />
      Exclude 2020 from range
    </label>
  );
}
