function precisionForUnit(unit: string) {
  if (unit === "mb") return 1;
  if (unit === "bcf") return 0;
  if (unit === "twh") return 1;
  if (unit === "%") return 1;
  return 1;
}

export function displayUnitFor(unit: string | null | undefined) {
  if (unit === "kb") {
    return { unit: "mb", factor: 0.001 };
  }

  return { unit: unit ?? "", factor: 1 };
}

export function convertUnitValue(value: number | null | undefined, unit: string | null | undefined) {
  if (value == null) {
    return null;
  }

  return value * displayUnitFor(unit).factor;
}

export function formatValue(value: number | null | undefined, unit: string | null | undefined) {
  if (value == null) {
    return "No data";
  }

  const { unit: displayUnit } = displayUnitFor(unit);
  const precision = precisionForUnit(displayUnit);
  return `${value.toLocaleString("en-US", {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  })}${displayUnit ? ` ${displayUnit}` : ""}`;
}

export function formatSignedValue(value: number | null | undefined, unit: string | null | undefined) {
  if (value == null) {
    return "No change";
  }

  const { unit: displayUnit } = displayUnitFor(unit);
  const precision = precisionForUnit(displayUnit);
  const formatted = Math.abs(value).toLocaleString("en-US", {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  });
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${formatted}${displayUnit ? ` ${displayUnit}` : ""}`;
}

export function formatPercent(value: number | null | undefined) {
  if (value == null) {
    return "N/A";
  }

  return `${value.toLocaleString("en-US", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })}%`;
}
